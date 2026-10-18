#!/usr/bin/env python3
"""Build the native C++ dependencies, including macOS unload support."""
import hashlib
import json
import shutil
from common import HERE, ROOT, main


def build(setup, source, name, prefix, options, *, wasm=False):
    directory = setup.prefix / "build" / name
    # CMake selects the installed Visual Studio instance on Windows.
    generator = ["-A", "x64"] if setup.windows and not wasm else ["-G", "Ninja"]
    compiler = []
    if not wasm:
        for variable in ("CC", "CXX"):
            if value := setup.environment().get(variable):
                compiler.append(f"-DCMAKE_{'C' if variable == 'CC' else 'CXX'}_COMPILER={value}")
    setup.run("cmake", "-S", source, "-B", directory, *generator,
              f"-DCMAKE_INSTALL_PREFIX={prefix}", "-DCMAKE_INSTALL_LIBDIR=lib", "-DCMAKE_CXX_STANDARD=20",
              "-DCMAKE_POSITION_INDEPENDENT_CODE=ON", "-DCMAKE_BUILD_TYPE=Release",
              "-DCMAKE_DEBUG_POSTFIX=d", *compiler, *options)
    # MSVC's Debug CRT/STL must match Debug SDK consumers. Install both configs.
    for config in (["Release", "Debug"] if setup.windows and not wasm else ["Release"]):
        setup.run("cmake", "--build", directory, "--config", config, "--parallel", setup.jobs)
        setup.run("cmake", "--install", directory, "--config", config)


def dependencies(setup, *, wasm=False, toolchain=None):
    kind = "wasm" if wasm else "native"
    prefix = setup.prefix / kind
    abseil = setup.archive("abseil-source", "sources/abseil-" + setup.versions["abseil"])
    protobuf = setup.archive("protobuf-source", f"sources/protobuf-{setup.versions['protobuf']}-{kind}")
    options = []
    if wasm:
        # Compile-only probes falsely accept nonexistent libraries such as -lpthreads.
        # WASI executables can be linked here; configure never needs to run them.
        options += [f"-DCMAKE_TOOLCHAIN_FILE={toolchain}", f"-DCMAKE_FIND_ROOT_PATH={prefix}",
                    "-DCMAKE_TRY_COMPILE_TARGET_TYPE=EXECUTABLE",
                    "-DCMAKE_C_FLAGS=-D_WASI_EMULATED_SIGNAL -D_WASI_EMULATED_MMAN",
                    "-DCMAKE_CXX_FLAGS=-fwasm-exceptions -mllvm -wasm-use-legacy-eh=false -D_WASI_EMULATED_SIGNAL -D_WASI_EMULATED_MMAN"]
    macos = setup.host.startswith("macos") and not wasm
    linux = setup.host.startswith("linux") and not wasm
    shared_abseil = macos or linux
    # Abseil registers pthread exit callbacks on foreign host threads. Keep
    # that shared runtime resident while individual plugin generations unload.
    abseil_options = ["-DCMAKE_SHARED_LINKER_FLAGS=-Wl,-z,nodelete"] if linux else []
    build(setup, abseil, f"abseil-{kind}", prefix, options + [
        "-DABSL_ENABLE_INSTALL=ON", "-DABSL_BUILD_TESTING=OFF", "-DABSL_PROPAGATE_CXX_STD=ON",
        "-DABSL_MSVC_STATIC_RUNTIME=OFF",
        "-DBUILD_SHARED_LIBS=" + ("ON" if shared_abseil else "OFF"),
        "-DABSL_BUILD_MONOLITHIC_SHARED_LIBS=" + ("ON" if shared_abseil else "OFF"),
        *abseil_options], wasm=wasm)
    source = protobuf
    if macos:
        patch = ROOT / "sdk/cpp/tools/protobuf/macos-thread-storage.patch"
        stamp = protobuf / ".cordis-patch"
        expected = hashlib.sha256(patch.read_bytes()).hexdigest()
        if stamp.exists() and stamp.read_text() != expected:
            raise RuntimeError("macOS adaptation changed; use a fresh --prefix for the new dependency build")
        if not stamp.exists():
            setup.run("patch", "--batch", "-p1", "-i", patch, cwd=protobuf)
            stamp.write_text(expected)
        header = patch.parent / "cordis_thread_storage.h"
        installed = protobuf / "src/google/protobuf/cordis_thread_storage.h"
        if not installed.exists() or installed.read_bytes() != header.read_bytes():
            shutil.copyfile(header, installed)
        source = HERE / "protobuf"
    build(setup, source, f"protobuf-{kind}", prefix, options + [
        f"-DCMAKE_PREFIX_PATH={prefix}", "-Dprotobuf_LOCAL_DEPENDENCIES_ONLY=ON",
        "-Dprotobuf_BUILD_TESTS=OFF", "-Dprotobuf_BUILD_SHARED_LIBS=OFF",
        "-Dprotobuf_BUILD_PROTOC_BINARIES=OFF", "-Dprotobuf_BUILD_LIBPROTOC=OFF",
        "-Dprotobuf_BUILD_LIBUPB=OFF", "-Dprotobuf_WITH_ZLIB=OFF", "-Dprotobuf_MSVC_STATIC_RUNTIME=OFF",
        *([f"-DCORDIS_PROTOBUF_SOURCE={protobuf}"] if macos else [])], wasm=wasm)
    return prefix


def install(setup):
    setup.require("python")
    setup.require("protoc")
    prefix = dependencies(setup)
    setup.set(CMAKE_PREFIX_PATH=prefix)
    if setup.host.startswith("macos"):
        libraries = list((prefix / "lib").glob("libabseil_dll.*.*.*.dylib"))
        if len(libraries) != 1:
            raise RuntimeError(f"Expected one versioned Abseil library in {prefix / 'lib'}")
        library = libraries[0]
        setup.set(CORDIS_EXAMPLE_HOST_LIBRARIES=json.dumps({"@rpath/" + library.name: str(library)}))
    elif setup.host.startswith("linux"):
        libraries = list((prefix / "lib").glob("libabseil_dll.so.*.*.*"))
        if len(libraries) != 1:
            raise RuntimeError(f"Expected one versioned Abseil library in {prefix / 'lib'}")
        library = libraries[0]
        setup.set(CORDIS_EXAMPLE_HOST_LIBRARIES=json.dumps({library.name: str(library)}))


if __name__ == "__main__":
    main("cpp")
