#!/usr/bin/env python3
"""Build framework protocol artifacts for all repository language bindings."""
import json
import sys

sys.dont_write_bytecode = True
from pathlib import Path

from runtime import messages, schema, write

ROOT = Path(__file__).resolve().parents[2]


def main():
    sources = ROOT / "protocols/protobuf"
    output = ROOT / "protocols/generated/runtime"
    messages(sources, ["cordis/options.proto", "cordis/types.proto", "transport.proto"], "cpp", output / "cpp")
    schema(sources, output / "resources/schema.base64", output / "cpp/cordis/runtime_schema.hpp")
    typescript = ROOT / "sdk/typescript/generated"
    messages(sources, ["transport.proto"], "es", typescript)
    encoded = (output / "resources/schema.base64").read_text().strip()
    write(typescript / "message-schema.ts", "// @generated\nexport const runtimeSchema = " + json.dumps(encoded) + ";\n")


if __name__ == "__main__":
    main()
