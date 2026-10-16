#!/usr/bin/env python3
"""Build test contracts, Python peer bindings and the ABI unload input."""
import argparse
import sys

sys.dont_write_bytecode = True
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "tools/codegen"))
from contracts import generate
from runtime import messages, protoc, write


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codegen", required=True, type=Path)
    args = parser.parse_args()
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
