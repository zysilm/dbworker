"""Sample owned Paperless processes, including replacement workers and OCR children."""
import threading
import time


class StackMetrics:
    def __init__(self, root_pid):
        import psutil
        self.psutil = psutil
        self.root = psutil.Process(root_pid)
        self.samples = {}
        self.peak = 0
        self.peak_processes = 0
        self.error = None
        self.stop_event = threading.Event()

    def sample(self):
        try:
            processes = [self.root, *self.root.children(recursive=True)]
            rss = 0
            for process in processes:
                try:
                    key = (process.pid, process.create_time())
                    cpu = process.cpu_times()
                    total = cpu.user + cpu.system
                    memory = process.memory_info().rss
                    if key not in self.samples:
                        # Existing process CPU is untimed. A newly born child
                        # starts at zero, preserving its startup CPU cost.
                        baseline = total if process.create_time() < self.started_at else 0
                        self.samples[key] = [baseline, total]
                    else:
                        self.samples[key][1] = max(self.samples[key][1], total)
                    rss += memory
                except (self.psutil.NoSuchProcess, self.psutil.ZombieProcess):
                    pass
            self.peak = max(self.peak, rss)
            self.peak_processes = max(self.peak_processes, len(processes))
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"

    def __enter__(self):
        self.started_at = time.time()
        self.sample()
        if self.error:
            raise RuntimeError(f"Owned-stack resource observation unavailable: {self.error}")
        def loop():
            while not self.stop_event.wait(.05):
                self.sample()
        self.thread = threading.Thread(target=loop, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop_event.set()
        self.thread.join()
        self.sample()

    def values(self):
        if self.error:
            raise RuntimeError(f"Owned-stack resource observation failed: {self.error}")
        return {"cpu_seconds": sum(max(0, last - first) for first, last in self.samples.values()),
                "peak_rss_bytes": self.peak, "observed_processes": len(self.samples),
                "peak_processes": self.peak_processes, "resource_sample_interval_seconds": .05,
                "resource_scope": "harness, Redis, worker replacements and OCR descendants; SQLite is in-process",
                "short_lived_process_sampling_limit": "Processes born and exited between 50ms samples may be missed"}
