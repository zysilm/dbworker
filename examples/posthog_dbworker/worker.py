"""Run the existing PostHog coordinator separately from API producers."""

from __future__ import annotations

import argparse
import json
import os
import signal
import threading
import time
from pathlib import Path

from examples.posthog_dbworker.runtime import coordinator


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DBWORKER_DATABASE_URL"))
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--ready-file", type=Path, required=True)
    args = parser.parse_args()
    if not args.database_url:
        parser.error("--database-url or DBWORKER_DATABASE_URL is required")
    if args.concurrency < 1:
        parser.error("--concurrency must be positive")

    stopping = threading.Event()
    failed = threading.Event()
    original_hook = threading.excepthook

    def thread_failed(error):
        original_hook(error)
        failed.set()
        stopping.set()

    threading.excepthook = thread_failed
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda number, frame: stopping.set())
    runtime = coordinator(args.database_url, concurrency=args.concurrency)
    try:
        runtime.start()
        receipt = {"pid": os.getpid(), "concurrency": args.concurrency,
                   "timestamp_ns": time.time_ns(),
                   "readiness_scope": "coordinator started; application warmup is separate"}
        temporary = args.ready_file.with_name(f"{args.ready_file.name}.{os.getpid()}.tmp")
        temporary.write_text(json.dumps(receipt) + "\n")
        temporary.replace(args.ready_file)
        stopping.wait()
    finally:
        runtime.stop()
        runtime.session_factory.kw["bind"].dispose()
        threading.excepthook = original_hook
    if failed.is_set():
        raise RuntimeError("The PostHog coordinator scheduling thread failed")


if __name__ == "__main__":
    main()
