"""Own an SMTP receiver independently of application producer threads."""

from __future__ import annotations

import multiprocessing
import os
from collections import Counter


def _receive(channel, port):
    from aiosmtpd.controller import Controller

    class Sink:
        def __init__(self):
            self.messages = []
            self.recipients = Counter()

        async def handle_DATA(self, server, session, envelope):
            self.messages.append({"mail_from": envelope.mail_from,
                                  "rcpt_tos": list(envelope.rcpt_tos),
                                  "content": bytes(envelope.original_content)})
            if envelope.rcpt_tos:
                self.recipients[envelope.rcpt_tos[0]] += 1
            return "250 Message accepted"

    sink = Sink()
    controller = Controller(sink, hostname="127.0.0.1", port=port)
    try:
        controller.start()
        channel.send({"process_id": os.getpid(), "port": port})
        while True:
            command, argument = channel.recv()
            if command == "stop":
                break
            if command == "count":
                channel.send(sum(sink.recipients[recipient] for recipient in argument))
            elif command == "messages":
                # Full payload transfer happens after the measured window.
                channel.send(sink.messages)
            else:
                raise ValueError("Unknown SMTP receiver command")
    finally:
        controller.stop()
        channel.close()


class SMTPReceiver:
    """Retain exact envelopes in the receiver; request only counts while timed."""

    def __init__(self, port, timeout=30):
        context = multiprocessing.get_context("spawn")
        self.channel, child = context.Pipe()
        self.process = context.Process(target=_receive, args=(child, port))
        self.child_channel = child
        self.timeout = timeout

    def start(self):
        self.process.start()
        self.child_channel.close()
        try:
            receipt = self._response()
            if receipt.get("process_id") != self.process.pid:
                raise RuntimeError("Unexpected SMTP receiver readiness identity")
        except BaseException:
            self.stop()
            raise

    def _response(self):
        if not self.channel.poll(self.timeout):
            raise TimeoutError("Owned SMTP receiver did not respond")
        try:
            return self.channel.recv()
        except EOFError as error:
            raise RuntimeError("Owned SMTP receiver exited") from error

    def _request(self, command, argument=None):
        if not self.process.is_alive():
            raise RuntimeError("Owned SMTP receiver exited")
        self.channel.send((command, argument))
        return self._response()

    def accepted_count(self, recipients):
        return self._request("count", tuple(recipients))

    @property
    def messages(self):
        return self._request("messages")

    def stop(self):
        try:
            if self.process.is_alive():
                try:
                    self.channel.send(("stop", None))
                except (BrokenPipeError, EOFError, OSError):
                    pass
                self.process.join(min(self.timeout, 10))
                if self.process.is_alive():
                    self.process.terminate()
                    self.process.join(10)
                    if self.process.is_alive():
                        self.process.kill()
                        self.process.join()
        finally:
            self.channel.close()
            self.child_channel.close()
