# Contributing to Cordis-X

English · [简体中文](CONTRIBUTING.zh-CN.md)

Start with a small change that can be explained and reviewed on its own. For a new API or a change to lifecycle behavior, open an issue describing the use case and the expected behavior before implementing a large change. Fixes and documentation corrections can go directly to a pull request.

Issues and pull requests may be written in English or Chinese. Discuss the code and the tradeoffs, avoid personal attacks, and do not post someone else's private information. Give other contributors enough context to reproduce a result or assess a design.

## Development setup

Run the commands below from the repository root. The shell examples use POSIX syntax; set the equivalent environment variables in your shell on Windows.

Install only the tools needed for the area you are changing:

| Area | Requirements |
| --- | --- |
| Native Rust runtime | Rust 1.96+, Cargo |
| Protobuf generation and Rust wire bindings | `protoc`; the checked-in C++ output uses Protobuf 34.1 |
| TypeScript SDK and generation | Node.js 24+, npm |
| Framework resource generation and Python test peers | Python 3.10+ and the declared Protobuf Python dependency |
| Native C++ | C++20 compiler, CMake 3.20+, Protobuf 34.1.0 and Abseil 20260817 CMake packages |
| Rust ABI bindings | libclang |
| Rust Wasm | Rust's `wasm32-unknown-unknown` target |
| C++ Wasm | WASI SDK, `wit-bindgen` 0.61.1, host `protoc`, and WASI-built Protobuf/Abseil libraries |

Builds invoke tools already present in the environment. You can supply them yourself or explicitly run the installers in `tools/setup/`. `PROTOC` and `CORDIS_PROTOC_GEN_ES` select generator executables; otherwise they are found on `PATH`. CMake dependencies use `CMAKE_PREFIX_PATH` or the corresponding package variables.

### Optional setup scripts

The setup scripts require Python 3.12+. The system prerequisite scripts cover macOS with Homebrew, Ubuntu 24.04+/Debian 13+ with apt, and Windows x64 with winget. They install the system compiler, Python, and Git through the platform's tools, which may request administrator access. Apple command line tools finish through Apple's installer; reopen the terminal after installing prerequisites on Windows. Other Linux distributions can provide the same prerequisites with their own package manager.

```sh
bash tools/setup/system.sh
python3 tools/setup/setup.py --prefix "$HOME/.local/share/cordis-tools" --jobs 4
source "$HOME/.local/share/cordis-tools/env.sh"
```

```powershell
./tools/setup/system.ps1
# In a new terminal:
py -3.12 tools/setup/setup.py --prefix "$env:LOCALAPPDATA/CordisTools" --jobs 4
. "$env:LOCALAPPDATA/CordisTools/env.ps1"
```

`--prefix` is required. The scripts keep tools, downloads, dependency sources, and build caches there; they do not edit shell profiles or replace a system Rust/Node installation. Rust's Cargo/rustup homes and the Python virtual environment are isolated in that directory. Node dependencies are installed in this checkout with `npm ci --ignore-scripts`. Repeated runs reuse downloads and CMake build outputs. Source the generated environment file in each terminal that needs these tools; installation does not activate the parent shell. Keep the prefix in place after installation because it contains absolute paths.

Every toolchain has its own entry point, accepting the same `--prefix` and `--jobs` arguments. Required dependencies are included automatically:

| Script in `tools/setup/` | Installs |
| --- | --- |
| `rust.py` | Rust/Cargo 1.98.1, rustfmt, Clippy, `wasm32-unknown-unknown` |
| `python.py` | Virtual environment, repository Python dependencies, CMake 4.4.4, Ninja 1.13.0, libclang 18.1.1 |
| `typescript.py` | Node.js 24.14.1 with npm; TypeScript, `protoc-gen-es`, and esbuild from the root npm lockfile |
| `protoc.py` | Host `protoc` 34.1 and standard `.proto` includes |
| `cpp.py` | Python build tools, `protoc`, native static Protobuf 34.1.0 and Abseil 20260817.0; macOS uses shared Abseil and the thread-storage adaptation |
| `wasm.py` | Rust and Python tools, host `protoc`, WASI SDK 34.0, `wit-bindgen` 0.61.1, WASI-built static Protobuf/Abseil, and `cordis-component-pack` |
| `setup.py` | All of the above, or the subset selected with `--only` |

For example, `python3 tools/setup/setup.py --prefix /path/to/tools --only rust protoc` prepares the Rust/protoc path. Add `python typescript` for generation and TypeScript work; add `cpp wasm` for the full integration suite. Native C++ needs the system compiler from the prerequisite step. Windows builds install both Debug and Release dependencies to match the MSVC runtime used by consumers.

On Linux, setup selects Clang for native C++ and exports `CC`/`CXX` for subsequent builds; GCC 13 crashes on coroutine expressions used by the SDK. Explicit `CC`/`CXX` settings select your own compiler. On Windows, CMake selects the installed Visual Studio instance, including VS 2026, instead of assuming a specific IDE release.

[versions.json](tools/setup/versions.json) pins official binary/source archives and their SHA-256 checksums. Python tools are pinned in [requirements.txt](tools/setup/requirements.txt); npm and Cargo dependencies use their lockfiles. OS compiler and Python patch versions follow the selected system package source. The binary recipes cover Linux/macOS x64 and ARM64, and Windows x64. Cross-platform validation is still performed by CI; having an installer recipe does not establish backend support on its own.

CI invokes these same installers with `--github-env`, which exports the prepared environment to subsequent steps. Installation failures fail the job before tests start. Normal `cargo`, CMake, npm generation, and SDK commands never call the installers.

### Using an existing environment

For TypeScript, code generation, and process tests:

```sh
npm ci --ignore-scripts
python3 -m pip install -r tools/codegen/requirements.txt -r tests/requirements.txt
export PATH="$PWD/node_modules/.bin:$PATH"
export CORDIS_TEST_PYTHON="$(python3 -c 'import sys; print(sys.executable)')"
```

Use a Python virtual environment if your Python installation requires one. The npm workspaces share the root lockfile. Keep tool versions in [the generator workspace](tools/codegen/typescript/package.json) and runtime dependencies in their owning packages.

On macOS, C++ ABI unloading requires a private static Protobuf runtime and host-shared Abseil. A manually supplied Protobuf build must incorporate the [thread-storage adaptation](sdk/cpp/tools/protobuf/macos-thread-storage.patch) and its [header](sdk/cpp/tools/protobuf/cordis_thread_storage.h) and [implementation](sdk/cpp/tools/protobuf/cordis_thread_storage.cc). The optional `cpp.py` installer performs this step; SDK CMake only checks for that support. WASI needs its own target libraries; a native dependency prefix cannot be used for a Wasm build.

## Source and generated files

Edit `.proto` sources, not generated clients or message types. Repository-wide generation is:

```sh
npm run generate
```

This generates framework resources, builds `cordis-codegen`, and then generates the shared example and test contracts. It uses the installed npm tool versions and does not download a toolchain.

Generation paths are owned by three build entry points:

- [Framework](tools/codegen/build.py): shared protocol resources and SDK protocol modules.
- [Examples](examples/build.py): the inputs in [examples/contracts.json](examples/contracts.json).
- [Tests](tests/build.py): the inputs in [tests/contracts.json](tests/contracts.json), plus test-only protocol bindings and resources.

Keep handwritten resources, handwritten code, and generated output separate. Business code imports named crates, npm packages, or C++ headers. Concrete paths belong in build scripts, Cargo build scripts, package exports, and CMake configuration. New languages should extend the compiler backends rather than introduce a second contract system.

## Design expectations

Cordis-X is still in development. When an unpublished interface changes, update its implementation, callers, examples, and tests together, and remove the replaced path. Do not add old-name aliases, protocol probing, or migration layers without an actual released consumer that needs them.

Lifecycle behavior is part of the API. Changes must preserve ownership, authorization, admitted-work draining, cancellation semantics, and cleanup responsibility. In particular, a cancelled waiter is not evidence that a remote operation did not execute.

Validation is selected by the host binding. `checked` is the default, `contract` is an explicit choice for trusted bindings, and `audit` adds diagnostic checks. Avoid repeated decoding or full validation at every backend boundary. Memory representation, hard limits, authority, and resource ownership still have to be enforced.

## Running tests

Use the existing Cargo, Node, and CTest entry points. Run the tests relevant to the change and report what ran; do not report an unavailable platform as passing.

Native runtime changes:

```sh
cargo test --locked -p cordis-kernel -p cordis-runtime
```

Contract compiler changes, with `protoc`, `protoc-gen-es`, Node, and native C++ dependencies available:

```sh
cargo test --locked -p cordis-codegen
```

TypeScript changes:

```sh
npm run test:typescript
npx --no-install tsc -p sdk/typescript/tsconfig.json
```

Native C++ changes:

```sh
cmake -S sdk/cpp -B target/cpp \
  -DCMAKE_PREFIX_PATH=/path/to/native/dependencies \
  -DCORDIS_BUILD_EXAMPLES=ON -DBUILD_TESTING=ON
cmake --build target/cpp --config Debug --parallel 2
ctest --test-dir target/cpp -C Debug --output-on-failure
```

The C++ process adapter is disabled by default on Windows. For Wasm tests, prepare the target compiler and libraries first. `CORDIS_WASM_RUSTC` can select a separate Rust guest compiler. The [C++ component builder](sdk/cpp/tools/build-component.py) accepts explicit toolchain, dependency, source, and output paths; use `--help` for its arguments.

With the required backend environments available, the workspace checks are:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets \
  --features cordis-wasm/loader,cordis-abi/loader,cordis-remote/loader \
  --locked -- -D warnings
cargo test --workspace \
  --features cordis-wasm/loader,cordis-abi/loader,cordis-remote/loader --locked
```

Some integration tests build and launch real plugins. Do not run another build against the same output directory concurrently. Reuse build outputs; there is no need to clean the cache before every test run.

## Submitting a change

Explain the problem, the resulting behavior, and how you tested it. For lifecycle changes, describe what happens to work and resources already in flight. Add a regression test when it demonstrates behavior that the existing tests do not cover; documentation-only changes do not need unrelated test runs.

Include regenerated output when a contract changes. Keep the English and Chinese README and contribution guide consistent. Add user-visible changes to the unreleased section of [CHANGELOG.md](CHANGELOG.md).

Report bugs with a minimal reproduction, the source revision, operating system, tool versions, and backend. Feature proposals should describe the application requirement and an example of the desired API, rather than only naming an implementation.

Contributions are submitted under the project's [Apache-2.0 license](LICENSE). Preserve third-party copyright and license notices when changing bundled dependencies.
