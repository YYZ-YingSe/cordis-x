"""Invoke the contract compiler; callers own every input and output path."""
import json
import subprocess


def generate(compiler, sources, output, manifest):
    for name, item in json.loads(manifest.read_text()).items():
        command = [str(compiler), "generate", "--input", str(sources / item["source"]),
                   "--proto-path", str(sources), "--out", str(output / name),
                   "--languages", item["languages"]]
        for package in item.get("rust_packages", []):
            command += ["--rust-package", package]
        subprocess.run(command, check=True)
