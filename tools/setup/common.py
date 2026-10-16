"""Shared I/O for explicitly requested environment setup. Never imported by builds."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
import zipfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent


def host():
    system = {"Darwin": "macos", "Linux": "linux", "Windows": "windows"}.get(platform.system())
    arch = {"arm64": "arm64", "aarch64": "arm64", "AMD64": "x64", "x86_64": "x64"}.get(platform.machine())
    key = f"{system}-{arch}"
    if key not in {"linux-x64", "linux-arm64", "macos-x64", "macos-arm64", "windows-x64"}:
        raise RuntimeError(f"No setup recipe for {platform.system()} / {platform.machine()}")
    return key


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def unpack(archive, destination):
    """Extract only inside destination; preserve executable bits and safe symlinks."""
    if zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive) as package:
            for item in package.infolist():
                path = (destination / item.filename).resolve()
                if not path.is_relative_to(destination.resolve()):
                    raise ValueError(f"Archive path escapes destination: {item.filename}")
            package.extractall(destination)
            for item in package.infolist():
                mode = item.external_attr >> 16
                if mode & 0o111:
                    (destination / item.filename).chmod(0o755)
    else:
        with tarfile.open(archive) as package:
            package.extractall(destination, filter="data")


class Setup:
    def __init__(self, prefix, jobs):
        self.prefix = prefix.expanduser().resolve()
        if self.prefix == ROOT or self.prefix == Path(self.prefix.anchor):
            raise ValueError("Choose a dedicated tool directory, not the repository or filesystem root")
        self.jobs = jobs
        self.host = host()
        self.windows = self.host.startswith("windows")
        self.exe = ".exe" if self.windows else ""
        self.versions = json.loads((HERE / "versions.json").read_text())
        self.prefix.mkdir(parents=True, exist_ok=True)
        state = self.prefix / "env.json"
        previous = json.loads(state.read_text()) if state.exists() else {}
        self.variables = previous.get("variables", {})
        self.paths = previous.get("paths", [])
        self.done = set()

    def environment(self):
        return {**os.environ, **self.variables, "PATH": os.pathsep.join([*self.paths, os.environ.get("PATH", "")])}

    def set(self, **values):
        self.variables.update({key: str(value) for key, value in values.items()})

    def path(self, path):
        value = str(path)
        self.paths = [value, *(entry for entry in self.paths if entry != value)]

    def run(self, *args, cwd=ROOT, capture=False):
        args = [str(arg) for arg in args]
        environment = self.environment()
        # Windows CreateProcess does not use env['PATH'] to resolve argv[0].
        if not Path(args[0]).is_absolute():
            executable = shutil.which(args[0], path=environment["PATH"])
            if executable is None:
                raise RuntimeError(f"Required command not found: {args[0]}. Prepare system prerequisites first.")
            args[0] = executable
        print("+ " + shlex.join(args), flush=True)
        return subprocess.run(args, cwd=cwd, env=environment, check=True,
                              text=True, stdout=subprocess.PIPE if capture else None).stdout

    def download(self, key):
        asset = self.versions["assets"][key]
        cache = self.prefix / "downloads"
        cache.mkdir(exist_ok=True)
        path = cache / (key + "-" + asset["sha256"][:12])
        if path.exists() and digest(path) == asset["sha256"]:
            return path
        partial = path.with_suffix(".partial")
        print(f"Downloading {asset['url']}", flush=True)
        try:
            with urllib.request.urlopen(asset["url"], timeout=120) as source, partial.open("wb") as output:
                shutil.copyfileobj(source, output)
            if digest(partial) != asset["sha256"]:
                raise RuntimeError(f"SHA-256 mismatch: {asset['url']}")
            partial.replace(path)
        finally:
            partial.unlink(missing_ok=True)
        return path

    def archive(self, key, name, *, strip_root=True):
        destination = self.prefix / name
        checksum = self.versions["assets"][key]["sha256"]
        marker = destination / ".archive-sha256"
        if marker.exists() and marker.read_text() == checksum:
            return destination
        if destination.exists():
            raise RuntimeError(f"Unrecognized or changed tools at {destination}; choose a fresh --prefix")
        archive = self.download(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=destination.parent) as temporary:
            stage = Path(temporary) / "unpacked"
            stage.mkdir()
            unpack(archive, stage)
            entries = list(stage.iterdir())
            if strip_root:
                if len(entries) != 1 or not entries[0].is_dir():
                    raise RuntimeError(f"Expected one archive root in {key}")
                stage = entries[0]
            (stage / ".archive-sha256").write_text(checksum)
            stage.rename(destination)
        return destination

    def require(self, name):
        if name not in self.done:
            # Modules have tool-specific entry points, but share one dependency order.
            import importlib
            importlib.import_module(name).install(self)
            self.done.add(name)

    def save(self, github=False):
        data = {"variables": self.variables, "paths": self.paths}
        (self.prefix / "env.json").write_text(json.dumps(data, indent=2) + "\n")
        shell = [f"export {key}={shlex.quote(value)}" for key, value in self.variables.items()]
        shell.append("export PATH=" + shlex.quote(os.pathsep.join(self.paths)) + ':"$PATH"')
        (self.prefix / "env.sh").write_text("\n".join(shell) + "\n")
        quote = lambda value: "'" + value.replace("'", "''") + "'"
        powershell = [f"$env:{key} = {quote(value)}" for key, value in self.variables.items()]
        powershell.append("$env:PATH = " + quote(os.pathsep.join(self.paths) + os.pathsep) + " + $env:PATH")
        (self.prefix / "env.ps1").write_text("\n".join(powershell) + "\n", encoding="utf-8-sig")
        if github:
            with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as output:
                for key, value in self.variables.items():
                    if "\n" in value or "\r" in value:
                        raise ValueError("GitHub environment values must be single-line")
                    output.write(f"{key}={value}\n")
            with open(os.environ["GITHUB_PATH"], "a", encoding="utf-8") as output:
                output.write("\n".join(reversed(self.paths)) + "\n")
        print(f"Environment ready. Activate with:\n  " +
              (f". {quote(str(self.prefix / 'env.ps1'))}" if self.windows else f"source {shlex.quote(str(self.prefix / 'env.sh'))}"))


def main(component=None):
    if sys.version_info < (3, 12):
        raise SystemExit("Setup requires Python 3.12+. Run tools/setup/system.sh or system.ps1 first.")
    parser = argparse.ArgumentParser(description="Explicitly install Cordis-X development tools. Builds never invoke this script.")
    parser.add_argument("--prefix", required=True, type=Path, help="dedicated installation directory chosen by you")
    parser.add_argument("--jobs", type=int, default=min(os.cpu_count() or 2, 4))
    parser.add_argument("--github-env", action="store_true", help="export tools to subsequent GitHub Actions steps")
    if component is None:
        parser.add_argument("--only", nargs="+", choices=["rust", "python", "typescript", "protoc", "cpp", "wasm"],
                            default=["rust", "python", "typescript", "protoc", "cpp", "wasm"])
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    if args.github_env and not all(os.environ.get(key) for key in ("GITHUB_ENV", "GITHUB_PATH")):
        parser.error("--github-env requires a GitHub Actions step")
    try:
        setup = Setup(args.prefix, args.jobs)
        for name in ([component] if component else args.only):
            setup.require(name)
        setup.save(args.github_env)
    except (OSError, ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"Setup failed: {error}") from error
