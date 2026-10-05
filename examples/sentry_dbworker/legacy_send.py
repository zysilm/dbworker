"""Synchronous bridge into the unmodified Sentry 24.1.0 email utility."""

import json
import sys


def main():
    from sentry.runner import configure
    configure(skip_service_validation=True)
    from django.core.mail import EmailMultiAlternatives
    from celery.app.task import Task
    from kombu import Producer
    from sentry.utils.email import send_messages

    def reject(*args, **kwargs):
        raise RuntimeError("Unexpected asynchronous dispatch in the legacy bridge")

    Producer.publish = reject
    Task.apply = reject
    payload = json.load(sys.stdin)
    messages = []
    for recipient in payload["to"]:
        message = EmailMultiAlternatives(subject=payload["subject"], body=payload["text"],
                                         from_email=payload["from"], to=[recipient],
                                         headers=payload.get("headers", {}))
        message.attach_alternative(payload["html"], "text/html")
        messages.append(message)
    accepted = send_messages(messages)
    print(json.dumps({"accepted": accepted}))


if __name__ == "__main__":
    main()
