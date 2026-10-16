#!/usr/bin/env python3
"""Build the shared example contracts; repository paths belong here."""
import argparse
import sys

sys.dont_write_bytecode = True
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "tools/codegen"))
from contracts import generate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codegen", required=True, type=Path)
    args = parser.parse_args()
    generate(args.codegen.resolve(), HERE / "resources/contracts", HERE / "generated", HERE / "contracts.json")
    for module in sorted((HERE / "generated").glob("*/cpp/module.hpp")):
        name = module.parents[1].name
        header = HERE / "generated/cpp/cordis/examples" / (name + ".hpp")
        header.parent.mkdir(parents=True, exist_ok=True)
        header.write_text('#pragma once\n#include "../../../' + name + '/cpp/module.hpp"\n')



if __name__ == "__main__":
    main()
