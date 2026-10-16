"""Environment setup failure handling; no downloads or system installation."""
import hashlib
import io
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools/setup"))
from common import Setup, unpack


class SetupTests(unittest.TestCase):
    def test_zip_traversal_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "bad.zip"
            with zipfile.ZipFile(archive, "w") as package:
                package.writestr("../outside", "unexpected")
            destination = root / "out"
            destination.mkdir()
            with self.assertRaises(ValueError):
                unpack(archive, destination)
            self.assertFalse((root / "outside").exists())

    def test_tar_link_cannot_escape_destination(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "bad.tar.gz"
            with tarfile.open(archive, "w:gz") as package:
                link = tarfile.TarInfo("link")
                link.type = tarfile.SYMTYPE
                link.linkname = "../outside"
                package.addfile(link)
            destination = root / "out"
            destination.mkdir()
            with self.assertRaises(tarfile.FilterError):
                unpack(archive, destination)

    def test_bad_checksum_is_not_installed_or_cached(self):
        with tempfile.TemporaryDirectory() as directory:
            setup = Setup(Path(directory), 1)
            setup.versions["assets"]["bad"] = {"url": "https://example.invalid/tool", "sha256": "0" * 64}
            with patch("urllib.request.urlopen", return_value=io.BytesIO(b"corrupt")):
                with self.assertRaisesRegex(RuntimeError, "SHA-256 mismatch"):
                    setup.download("bad")
            self.assertEqual(list((setup.prefix / "downloads").iterdir()), [])

    def test_verified_download_is_reused_offline(self):
        with tempfile.TemporaryDirectory() as directory:
            setup = Setup(Path(directory), 1)
            setup.versions["assets"]["tool"] = {"url": "https://example.invalid/tool", "sha256": hashlib.sha256(b"ok").hexdigest()}
            with patch("urllib.request.urlopen", return_value=io.BytesIO(b"ok")) as request:
                first = setup.download("tool")
                self.assertEqual(first, setup.download("tool"))
                self.assertEqual(request.call_count, 1)

    def test_environment_export_and_component_runs_preserve_values(self):
        with tempfile.TemporaryDirectory(prefix="cordis setup '") as directory:
            setup = Setup(Path(directory), 1)
            value = "space ' quote $HOME `do-not-run`"
            setup.set(CORDIS_SETUP_TEST=value)
            setup.path(setup.prefix / "bin with spaces")
            env_file = setup.prefix / "github-env"
            path_file = setup.prefix / "github-path"
            with patch.dict(os.environ, {"GITHUB_ENV": str(env_file), "GITHUB_PATH": str(path_file)}):
                setup.save(github=True)
            self.assertIn("CORDIS_SETUP_TEST=" + value, env_file.read_text())
            self.assertEqual(path_file.read_text().strip(), setup.paths[0])
            restored = Setup(setup.prefix, 1)
            self.assertEqual(restored.variables, setup.variables)
            if os.name != "nt":
                result = subprocess.check_output(["sh", "-c", '. "$1"; printf "%s" "$CORDIS_SETUP_TEST"',
                                                  "sh", str(setup.prefix / "env.sh")], text=True)
                self.assertEqual(result, value)
            self.assertIn("'' quote", (setup.prefix / "env.ps1").read_text())

    def test_subprocess_resolves_prepared_path_and_propagates_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            setup = Setup(Path(directory), 1)
            with patch("shutil.which", return_value="/prepared/bin/tool") as resolve:
                with patch("subprocess.run", side_effect=subprocess.CalledProcessError(7, "tool")) as run:
                    with self.assertRaises(subprocess.CalledProcessError):
                        setup.run("tool", "argument")
                    self.assertEqual(run.call_args.args[0][0], "/prepared/bin/tool")
                    self.assertEqual(resolve.call_args.kwargs["path"], setup.environment()["PATH"])


if __name__ == "__main__":
    unittest.main()
