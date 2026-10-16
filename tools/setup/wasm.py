#!/usr/bin/env python3
"""Prepare Rust guests, WASI C++, target libraries, and the component packer."""
from common import ROOT, main
from cpp import dependencies


def install(setup):
    setup.require("rust")
    setup.require("python")
    setup.require("protoc")
    wasi = setup.archive(f"wasi_sdk-{setup.host}", "wasi-sdk-" + setup.versions["wasi_sdk"])
    bindings = setup.archive(f"wit_bindgen-{setup.host}", "wit-bindgen-" + setup.versions["wit_bindgen"])
    executable = bindings / ("wit-bindgen" + setup.exe)
    setup.run(executable, "--version")
    prefix = dependencies(setup, wasm=True, toolchain=wasi / "share/cmake/wasi-sdk-p1.cmake")
    setup.run("cargo", "install", "--locked", "--path", ROOT / "crates/cordis-component-pack",
              "--root", setup.prefix / "component-pack", "--target-dir", setup.prefix / "build/component-pack",
              "--jobs", setup.jobs)
    setup.path(bindings)
    setup.path(setup.prefix / "component-pack/bin")
    setup.set(WASI_SDK_PATH=wasi, WIT_BINDGEN=executable, CORDIS_WASM_PREFIX=prefix,
              CORDIS_COMPONENT_PACK=setup.prefix / "component-pack/bin" / ("cordis-component-pack" + setup.exe))


if __name__ == "__main__":
    main("wasm")
