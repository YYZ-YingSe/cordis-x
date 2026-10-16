"""Protobuf generation primitives. All source and destination paths are supplied."""
import base64
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path


def protoc(arguments, **kwargs):
    return subprocess.run([os.environ.get("PROTOC", "protoc"), *map(str, arguments)],
                          check=True, **kwargs)


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def messages(sources, inputs, language, output):
    output.mkdir(parents=True, exist_ok=True)
    options = []
    if language == "es":
        plugin = os.environ.get("CORDIS_PROTOC_GEN_ES") or shutil.which("protoc-gen-es")
        if not plugin:
            raise RuntimeError("Provide protoc-gen-es on PATH or CORDIS_PROTOC_GEN_ES")
        options = [f"--plugin=protoc-gen-es={plugin}", "--es_opt=target=ts,import_extension=ts"]
    protoc(["-I", sources, f"--{language}_out={output}", *options, *inputs])


def schema(sources, output, cpp_header=None):
    # The official descriptor implementation retains custom option fields.
    from google.protobuf import descriptor_pb2
    with tempfile.TemporaryDirectory() as directory:
        descriptor = Path(directory) / "runtime.pb"
        protoc(["-I", sources, "--include_imports", f"--descriptor_set_out={descriptor}", "runtime.proto"])
        files = descriptor_pb2.FileDescriptorSet.FromString(descriptor.read_bytes())
        compact = descriptor_pb2.FileDescriptorSet()
        for file in files.file:
            if not file.name.startswith("google/protobuf/"):
                compact.file.add().CopyFrom(file)
        encoded = base64.b64encode(compact.SerializeToString()).decode()
    write(output, encoded + "\n")
    if cpp_header:
        write(cpp_header, "// @generated\n#pragma once\nnamespace cordis::contract {\n"
              "inline constexpr const char* runtime_schema = " + json.dumps(encoded) + ";\n}\n")
