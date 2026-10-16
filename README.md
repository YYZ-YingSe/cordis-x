# Cordis-X

English · [简体中文](README.zh-CN.md)

Cordis-X is an embeddable plugin runtime written in Rust. It manages service dependencies, asynchronous work, resources, and code replacement across native Rust, C ABI libraries, WebAssembly components, and separate processes.

An application uses Cordis-X to give plugins access to services and to control their lifetime. Plugin authors implement those services with the Rust, C++, or TypeScript SDK. Users install and manage plugins through the application, which decides where plugins come from and what they may do.

The project is under active development. APIs, protocols, and generated bindings may change together. The workspace is currently consumed from source; its Rust crates and npm packages are not configured for publication.

## Plugin lifetimes

A plugin can register a listener, start a background task, open a resource, and call another plugin. Replacing its code means accounting for all of that work.

Cordis-X tracks these relationships in the runtime:

- **Dependencies determine activation.** A consumer declares the services it needs and waits until suitable providers are available.
- **Effects carry cleanup.** A plugin registers the inverse of work that must be undone. Cleanup runs in the owning scope, in reverse order.
- **Resources belong to their callers.** A resource can outlive the call that created it. Its owner controls disposal, and admitted calls drain before it closes.
- **Cancellation preserves responsibility.** Dropping a waiting future does not mean that remote work stopped or that a resource was released.
- **Replacement preserves existing work.** Ordinary provider replacement coordinates dependent lifetimes. A Broker can route new calls to a new implementation while existing resources keep their original implementation alive.

Cleanup failures and uncertain remote outcomes remain visible. The runtime does not report successful release when it cannot establish that release happened.

## A small native example

Services in the native Rust API carry ordinary Rust values. Calling them does not require a wire format or generated bindings.

```rust
use cordis_sdk::{Plugin, Result, Runtime, Service};
use std::sync::Arc;

fn greeting() -> Service<String> {
    Service::new("example.greeting", "1")
}

#[tokio::main]
async fn main() -> Result<()> {
    let runtime = Runtime::new();

    let provider = runtime.install(
        Plugin::new("greeting", |ctx| async move {
            ctx.defer("greeting cleanup", |_| async {
                println!("greeting: stopped");
                Ok(())
            })?;
            ctx.provide(&greeting(), Arc::new("Hello from Cordis-X".to_owned()))
        })
        .provides(&greeting()),
    )?;

    let consumer = runtime.install(
        Plugin::new("reader", |ctx| async move {
            let message = ctx.dependency(&greeting())?
                .with(|value, _| Ok(value.clone()))?;
            println!("{message}");
            Ok(())
        })
        .requires(&greeting()),
    )?;

    consumer.ready().await?;
    provider.restart().await?;
    consumer.ready().await?;
    runtime.shutdown().await
}
```

Restarting the provider causes the consumer to leave its old activation and activate against the new provider. Shutdown cleans up the remaining activations.

To try this lifecycle with an existing example, run from a checkout with Rust 1.96 or later and `protoc` available on `PATH`:

```sh
cargo run --locked -p cordis-sdk --example lifecycle
```

The [lifecycle example](sdk/rust/native/examples/lifecycle.rs) shows a consumer cleaning up while its database dependency is still open. The [Broker example](sdk/rust/native/examples/broker.rs) shows a transaction remaining on the old implementation after new requests switch to its replacement:

```sh
cargo run --locked -p cordis-sdk --example broker
```

## Execution backends

The host runtime is Rust. Plugins can run through the following SDK paths:

| Backend | Plugin languages in this repository | Execution boundary |
| --- | --- | --- |
| Native | Rust | Typed services inside the host process |
| C ABI | Rust, C++ | A shared library loaded into the host process |
| WebAssembly | Rust, C++ | A Component Model instance hosted by Wasmtime |
| Process | Rust, C++, TypeScript on Node.js | A child process using framed stdio or HTTP |

The portable SDKs expose services, resources, dependency calls, events, middleware, child plugins, scopes, tasks, and cleanup effects. Backend capabilities remain explicit: the basic Wasm world exports services and resources; the managed world also imports host context operations.

Native plugins and C ABI libraries execute with the host process's privileges. A process boundary does not by itself sandbox a plugin. Wasm access depends on the imports and WASI capabilities the host grants, together with its configured execution limits.

The C++ process adapter requires POSIX. C++ Wasm builds require WASI target libraries, which the optional setup scripts can build. The repository contains Linux, macOS, and Windows CI configurations; that configuration alone is not a claim that every backend and platform combination has been validated.

## Setting up the tools

The optional [setup scripts](tools/setup/setup.py) prepare Rust, Python, Node.js and npm tools, `protoc`, native C++ dependencies, and the C++ Wasm toolchain. Choose a directory for the tools; it does not determine where an application stores plugins or generated code.

On macOS or Ubuntu/Debian, install the system prerequisites if needed, then run:

```sh
bash tools/setup/system.sh
python3 tools/setup/setup.py --prefix "$HOME/.local/share/cordis-tools"
source "$HOME/.local/share/cordis-tools/env.sh"
```

The setup entry point requires Python 3.12+. On macOS, `system.sh` prints the installed Python command if it is not on `PATH`. On Windows, use PowerShell:

```powershell
./tools/setup/system.ps1
# Open a new terminal after system installation.
py -3.12 tools/setup/setup.py --prefix "$env:LOCALAPPDATA/CordisTools"
. "$env:LOCALAPPDATA/CordisTools/env.ps1"
```

For a subset, pass `--only rust protoc`, or invoke a tool's script such as `python3 tools/setup/cpp.py --prefix /path/to/tools`. Installation happens only when requested; build and generation commands do not install tools. See [the contribution guide](CONTRIBUTING.md#development-setup) for coverage, versions, and prerequisites.

## Contracts and generated code

Cross-language services use Protobuf messages with Cordis declarations for service identity, dependencies, resource operations, control operations, and business errors. The compiler emits standard message types alongside typed clients, server interfaces, dispatch code, and descriptors.

For example, an addition service can declare both required operands and its overflow error:

```proto
syntax = "proto3";
package example;

import "cordis/options.proto";
import "cordis/types.proto";

message AddArgs {
  optional sint64 left = 1 [(cordis.schema.required) = true];
  optional sint64 right = 2 [(cordis.schema.required) = true];
}

service Adder {
  option (cordis.schema.contract) = {
    name: "example.adder"
    revision: "1"
    config: "cordis.data.Unit"
  };
  rpc Add(AddArgs) returns (cordis.data.Int64) {
    option (cordis.schema.errors) = "overflow";
  }
}
```

The business implementation supplies the addition; generated code handles dispatch and the declared message contract. The same service is implemented in [Rust](sdk/rust/examples/portable/src/adder.rs), [C++](sdk/cpp/examples/adder.cpp), and [TypeScript](sdk/typescript/examples/adder.ts).

Generation uses `protoc`, with `protoc-gen-es` for TypeScript. After preparing the tools and installing the repository's npm dependencies, a project build script can invoke:

```sh
npm ci --ignore-scripts
npm run codegen -- generate \
  --input /path/to/plugin/api.proto \
  --proto-path /path/to/plugin \
  --out /path/to/plugin/generated \
  --languages rust,cpp,typescript
```

Input, output, and module wiring belong to the consuming project's build configuration. Cordis-X does not require applications to adopt this repository's directory layout.

## Embedding in an application

Applications select the SDK and backends they need. Rust consumers use workspace or path dependencies; C++ consumers use CMake `add_subdirectory`; TypeScript consumes the source SDK through the npm workspace or an application build.

The application registers plugin sources and grants dependencies and capabilities. The Loader can apply declarative configuration, prepare artifacts, and coordinate changes through the runtime. The application remains responsible for its interface, plugin discovery and distribution, storage locations, and trust decisions.

| Example | What it demonstrates |
| --- | --- |
| [Native lifecycle](sdk/rust/native/examples/lifecycle.rs) | Dependency activation, restart, and ordered cleanup |
| [Stepwise effects](sdk/rust/native/examples/stepwise.rs) | Asynchronous initialization with a cleanup action for each completed step |
| [Broker](sdk/rust/native/examples/broker.rs) | Switching new requests while retaining old resources |
| [Loader](sdk/rust/native/examples/loader.rs) | Application-registered plugin sources and configuration changes |
| [Portable resources](sdk/rust/examples/portable/src/resources.rs) | Shared resource business logic across execution adapters |
| [Workflows](sdk/typescript/examples/workflows.ts) | Generated events, middleware, child factories, and value publication |

## Repository layout

| Path | Contents |
| --- | --- |
| `crates/` | Runtime, loader, execution hosts, contracts, and compiler |
| `sdk/` | Rust, C++, and TypeScript plugin APIs and examples |
| `protocols/` | Shared ABI headers, WIT interfaces, and Protobuf definitions |
| `examples/` | Shared example contracts, resources, and build configuration |
| `tests/` | Shared test contracts, fixtures, and build configuration |
| `tools/codegen/` | Repository generation entry points and tool version declarations |
| `tools/setup/` | Optional environment installers and pinned tool releases |

Generated code lives separately from handwritten source. Tools and target libraries are supplied by the developer; build commands do not install them automatically.

## Contributing

Bug reports, fixes, and examples are welcome. Include a small reproduction, the execution backend, and the expected lifecycle behavior when reporting a problem. For development setup and test commands, see [Contributing](CONTRIBUTING.md). Changes are recorded in the [changelog](CHANGELOG.md).

## License

Cordis-X is licensed under [Apache-2.0](LICENSE). See [NOTICE](NOTICE) for attribution and [third-party notices](THIRD_PARTY_NOTICES.md) for bundled dependencies and their licenses.
