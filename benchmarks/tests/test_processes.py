import os
import sys
import tempfile
import unittest
from pathlib import Path

import psutil

from benchmarks.common.processes import run_command


class ProcessTests(unittest.TestCase):
    def test_timeout_cleans_a_child_with_an_independent_session(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pid_file = root / "child.pid"
            script = root / "launch.py"
            script.write_text("import subprocess, sys, time\n"
                              "from pathlib import Path\n"
                              "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], start_new_session=True)\n"
                              f"Path({str(pid_file)!r}).write_text(str(child.pid))\n"
                              "time.sleep(60)\n")
            with self.assertRaises(TimeoutError):
                run_command([sys.executable, str(script)], cwd=root, env=os.environ.copy(),
                            log=root / "process.log", timeout=2)
            self.assertTrue(pid_file.exists())
            pid = int(pid_file.read_text())
            self.assertTrue(not psutil.pid_exists(pid) or psutil.Process(pid).status() == psutil.STATUS_ZOMBIE)


if __name__ == "__main__":
    unittest.main()
