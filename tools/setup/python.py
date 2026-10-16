#!/usr/bin/env python3
"""Prepare an isolated Python environment and portable build tools."""
import sys
from common import HERE, ROOT, main


def install(setup):
    directory = setup.prefix / "python"
    interpreter = directory / ("Scripts/python.exe" if setup.windows else "bin/python")
    if not interpreter.exists():
        setup.run(sys.executable, "-m", "venv", directory)
    setup.run(interpreter, "-m", "pip", "install", "--disable-pip-version-check",
              "-r", HERE / "requirements.txt", "-r", ROOT / "tools/codegen/requirements.txt",
              "-r", ROOT / "tests/requirements.txt")
    clang = setup.run(interpreter, "-c", "import clang, pathlib; print(pathlib.Path(clang.__file__).parent / 'native')", capture=True).strip()
    setup.path(interpreter.parent)
    setup.set(PYTHON=interpreter, CORDIS_TEST_PYTHON=interpreter, LIBCLANG_PATH=clang)


if __name__ == "__main__":
    main("python")
