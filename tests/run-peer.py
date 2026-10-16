#!/usr/bin/env python3
"""Test-only Python module wiring and fault-peer launcher."""
from pathlib import Path
import runpy
import sys

sys.dont_write_bytecode = True

generated = Path(__file__).resolve().parent / "generated/python"
sys.path.insert(0, str(generated))
script = Path(sys.argv.pop(1)).resolve()
sys.path.insert(0, str(script.parent))
sys.argv[0] = str(script)
runpy.run_path(str(script), run_name="__main__")
