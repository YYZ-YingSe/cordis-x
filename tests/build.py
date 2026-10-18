#!/usr/bin/env python3
"""Build test contracts or the shared native integration fixtures."""
import argparse
from contextlib import contextmanager
import errno
import os
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from pathlib import Path

HERE = Path(__file__).resolve().parent


@contextmanager
def native_lock(build):
    """Serialize CMake mutations across independently launched test processes."""
    build.mkdir(parents=True, exist_ok=True)
    with (build / ".build.lock").open("a+b") as lock:
        if os.name == "nt":
            import msvcrt
            if lock.seek(0, os.SEEK_END) == 0:
                lock.write(b"\0")
                lock.flush()
            lock.seek(0)
            while True:
                try:
                    msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError as error:
                    if error.errno not in (errno.EACCES, errno.EAGAIN, errno.EDEADLK):
                        raise
                    time.sleep(0.05)
        else:
            import fcntl
            fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            if os.name == "nt":
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(lock, fcntl.LOCK_UN)


def native(args):
    build = HERE.parent / "build/native"
    with native_lock(build):
        command = ["cmake", "-S", str(HERE.parent / "sdk/cpp"), "-B", str(build),
                   "-DCMAKE_BUILD_TYPE=Debug", "-DCORDIS_BUILD_EXAMPLES=ON", "-DBUILD_TESTING=ON",
                   "-DCORDIS_BUILD_ABI=ON", f"-DCORDIS_BUILD_PROCESS={'OFF' if os.name == 'nt' else 'ON'}"]
        for kind in ("RUNTIME", "LIBRARY"):
            for suffix in ("", "_DEBUG"):
                command.append(f"-DCMAKE_{kind}_OUTPUT_DIRECTORY{suffix}={build / 'bin'}")
        subprocess.run(command, check=True)
        if not args.configure_only:
            command = ["cmake", "--build", str(build), "--config", "Debug", "--parallel", str(args.jobs)]
            if args.target:
                command += ["--target", *args.target]
            subprocess.run(command, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--codegen", type=Path)
    mode.add_argument("--native", action="store_true", help="prepare shared C++ fixtures; tools must already be installed")
    parser.add_argument("--target", nargs="+", help="native CMake targets; omit to prepare the full integration suite")
    parser.add_argument("--configure-only", action="store_true")
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    if args.native:
        native(args)
        return
    if args.target or args.configure_only:
        parser.error("--target and --configure-only require --native")
    sys.path.insert(0, str(HERE.parent / "tools/codegen"))
    from contracts import generate
    from runtime import messages, protoc, write
    generate(args.codegen.resolve(), HERE / "resources/contracts", HERE / "generated", HERE / "contracts.json")
    for module in sorted((HERE / "generated").glob("*/cpp/module.hpp")):
        name = module.parents[1].name
        header = HERE / "generated/cpp/cordis/tests" / (name + ".hpp")
        header.parent.mkdir(parents=True, exist_ok=True)
        header.write_text('#pragma once\n#include "../../../' + name + '/cpp/module.hpp"\n')

    sources = HERE.parent / "protocols/protobuf"
    messages(sources, ["transport.proto"], "python", HERE / "generated/python")
    config = protoc(["-I", sources, "--encode=cordis.transport.Payload", "transport.proto"],
                    input=(HERE / "resources/empty-config.textproto").read_bytes(), capture_output=True).stdout
    write(HERE / "generated/cpp/cordis/tests/empty_config.hpp",
          "// @generated\n#pragma once\n#include <cstdint>\ninline constexpr uint8_t empty_config[] = {"
          + ",".join(str(b) for b in config) + "};\n")
    composer = HERE.parent / "examples/generated/lifecycle/resources/interfaces/Composer.json"
    write(HERE / "generated/python/contracts.py", "# @generated\nCOMPOSER = " + repr(composer.read_text()) + "\n")


if __name__ == "__main__":
    main()
