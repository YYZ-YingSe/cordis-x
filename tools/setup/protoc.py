#!/usr/bin/env python3
"""Install the host Protobuf compiler; C++ libraries are prepared by cpp.py."""
from common import main


def install(setup):
    directory = setup.archive(f"protoc-{setup.host}", "protoc-" + setup.versions["protobuf"], strip_root=False)
    executable = directory / "bin" / ("protoc" + setup.exe)
    setup.run(executable, "--version")
    setup.path(executable.parent)
    setup.set(PROTOC=executable)


if __name__ == "__main__":
    main("protoc")
