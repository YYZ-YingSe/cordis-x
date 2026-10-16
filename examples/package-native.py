#!/usr/bin/env python3
"""Install and describe the repository's two native packages using CMake/Goblin."""
import json
import os
import argparse
import pathlib
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=pathlib.Path, required=True)
    parser.add_argument("--backend", choices=["abi", "process"], required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--host", action="append", default=[], help="explicit host import=library mapping")
    args = parser.parse_args()
    libraries = json.loads(os.environ.get("CORDIS_EXAMPLE_HOST_LIBRARIES", "{}"))
    for value in args.host:
        name, path = value.split("=", 1)
        libraries[name] = path
    repository = pathlib.Path(__file__).resolve().parent.parent
    build = args.build.resolve()
    output = args.output.resolve()
    # A new output root prevents old installed generations from entering a package.
    output.mkdir(parents=True, exist_ok=False)
    for generation in ("v1", "v2"):
        target = f"cordis_package_{args.backend}_{generation}"
        destination = output / generation
        subprocess.run(["cmake", "--install", str(build), "--config", "Debug",
                        "--component", f"cordis-{target}", "--prefix", str(destination)], check=True)
        entry = target
        if args.backend == "abi":
            entry = target + ".dll" if sys.platform == "win32" else "lib" + target + (".dylib" if sys.platform == "darwin" else ".so")
        elif sys.platform == "win32":
            entry += ".exe"
        command = ["cargo", "run", "-p", "cordis-loader", "--bin", "cordis-package", "--locked", "--", str(destination), entry]
        for name, path in libraries.items():
            command += ["--host", f"{name}={path}"]
        subprocess.run(command, cwd=repository, check=True)


if __name__ == "__main__":
    main()
