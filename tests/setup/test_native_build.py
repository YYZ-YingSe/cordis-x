"""The shared CMake directory must have one writer across test processes."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class NativeBuildLockTests(unittest.TestCase):
    def test_lock_serializes_processes_and_releases_after_exit(self):
        script = Path(__file__).resolve().parents[1] / "build.py"
        worker = """
import runpy, sys
from pathlib import Path
build = runpy.run_path(sys.argv[1])
with build['native_lock'](Path(sys.argv[2])):
    print('locked', flush=True)
    sys.stdin.readline()
"""
        with tempfile.TemporaryDirectory() as directory:
            command = [sys.executable, "-c", worker, str(script), directory]
            with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, text=True) as first:
                try:
                    self.assertEqual(first.stdout.readline(), "locked\n")
                    with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE, text=True) as second:
                        try:
                            with self.assertRaises(subprocess.TimeoutExpired):
                                second.communicate(input="\n", timeout=0.2)
                            # An exiting test process releases the OS lock too.
                            first.kill()
                            first.communicate(timeout=10)
                            output, error = second.communicate(timeout=10)
                            self.assertEqual(second.returncode, 0, error)
                            self.assertEqual(output, "locked\n")
                        finally:
                            if second.poll() is None:
                                second.kill()
                                second.communicate()
                finally:
                    if first.poll() is None:
                        first.kill()
                        first.communicate()


if __name__ == "__main__":
    unittest.main()
