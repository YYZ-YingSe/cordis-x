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

The project invokes these tools but does not install them. `PROTOC` and `CORDIS_PROTOC_GEN_ES` can select generator executables; otherwise they are found on `PATH`. CMake dependencies use `CMAKE_PREFIX_PATH` or the corresponding package variables.

For TypeScript, code generation, and process tests:

```sh
npm ci --ignore-scripts
python3 -m pip install -r tools/codegen/requirements.txt -r tests/requirements.txt
export PATH="$PWD/node_modules/.bin:$PATH"
export CORDIS_TEST_PYTHON="$(python3 -c 'import sys; print(sys.executable)')"
```

Use a Python virtual environment if your Python installation requires one. The npm workspaces share the root lockfile. Keep tool versions in [the generator workspace](tools/codegen/typescript/package.json) and runtime dependencies in their owning packages.

On macOS, C++ ABI unloading requires a private static Protobuf runtime and host-shared Abseil. The developer-provided Protobuf build must incorporate the [thread-storage adaptation](sdk/cpp/tools/protobuf/macos-thread-storage.patch) and its [header](sdk/cpp/tools/protobuf/cordis_thread_storage.h) and [implementation](sdk/cpp/tools/protobuf/cordis_thread_storage.cc). CMake checks for that support. WASI needs its own target libraries; a native dependency prefix cannot be used for a Wasm build.

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
