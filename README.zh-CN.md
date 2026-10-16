# Cordis-X

[English](README.md) · 简体中文

Cordis-X 是用 Rust 编写的可嵌入插件运行时，负责服务依赖、异步任务、资源管理和代码替换，支持原生 Rust、C ABI 动态库、WebAssembly 组件及独立进程。

应用通过 Cordis-X 向插件提供服务，并管理插件的生命周期。插件开发者使用 Rust、C++ 或 TypeScript SDK 实现服务；使用者则通过应用安装和管理插件。插件从哪里获取、可以访问哪些能力，由应用决定。

项目仍处于开发阶段，API、协议和生成代码会随设计一同调整。目前通过源码工作区接入，Rust crate 和 npm 包尚未配置为公开发布。

## 插件的生命周期

一个插件可能注册监听器、启动后台任务、创建资源，还可能调用其他插件。替换它的代码时，这些工作都需要有明确的去向。

Cordis-X 在运行时跟踪这些关系：

- **依赖决定激活顺序。** 消费方声明需要的服务，在合适的提供方就绪后激活。
- **副作用带有清理操作。** 插件为需要撤销的工作登记逆操作，在所属作用域中按逆序清理。
- **资源归调用者所有。** 资源可以在创建它的调用结束后继续使用，由持有方控制释放；已经进入的调用排空后，资源才会关闭。
- **取消等待不丢弃责任。** 丢弃一个等待中的 Future，不代表远端工作已经停止，也不代表资源已经释放。
- **代码替换保留已有工作的归属。** 普通提供方替换会协调依赖它的插件。Broker 则可以将新调用切换到新实现，让已有资源继续使用原来的实现。

清理失败和无法确认的远端结果会保留在运行时状态中。无法确认释放已经完成时，运行时不会将其报告为成功。

## 一个原生示例

原生 Rust API 中的服务直接携带 Rust 值，调用不需要消息编码或生成绑定。

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

重启提供方时，消费方会退出原来的激活，再绑定新的提供方。关闭运行时会清理剩余的激活实例。

准备 Rust 1.96 或更新版本，并将 `protoc` 加入 `PATH` 后，可以在仓库根目录运行现有示例，观察这一过程：

```sh
cargo run --locked -p cordis-sdk --example lifecycle
```

[生命周期示例](sdk/rust/native/examples/lifecycle.rs)展示了消费方清理时，其依赖的数据库连接仍然可用。[Broker 示例](sdk/rust/native/examples/broker.rs)展示了新请求切换到新实现后，已有事务继续使用旧实现：

```sh
cargo run --locked -p cordis-sdk --example broker
```

## 执行后端

宿主运行时使用 Rust。仓库提供以下插件 SDK 路径：

| 后端 | 仓库支持的插件语言 | 执行边界 |
| --- | --- | --- |
| Native | Rust | 宿主进程内的类型化服务 |
| C ABI | Rust、C++ | 加载到宿主进程的动态库 |
| WebAssembly | Rust、C++ | 由 Wasmtime 托管的 Component Model 实例 |
| Process | Rust、C++、Node.js 上的 TypeScript | 通过带帧边界的标准输入输出或 HTTP 通信的子进程 |

可移植 SDK 提供服务、资源、依赖调用、事件、中间件、子插件、作用域、任务和清理副作用。不同后端的能力仍然明确区分：基础 Wasm world 导出服务和资源，managed world 还可以导入宿主 Context 操作。

原生插件和 C ABI 动态库拥有宿主进程的权限。独立进程本身不构成插件沙箱。Wasm 能访问什么，取决于宿主授予的导入和 WASI 能力，以及配置的执行限制。

C++ 进程适配器要求 POSIX 环境；C++ Wasm 构建需要开发者准备对应的 WASI 目标库。仓库包含 Linux、macOS 和 Windows 的 CI 配置，但配置文件本身不代表所有后端与平台组合都已完成验证。

## 契约与生成代码

跨语言服务使用 Protobuf 消息，并通过 Cordis 声明描述服务身份、依赖、资源操作、控制操作和业务错误。编译器在标准消息类型之外，生成类型化客户端、服务端接口、分派代码和描述数据。

例如，加法服务可以同时声明两个必填操作数，以及溢出时返回的业务错误：

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

业务代码实现加法，生成代码处理分派和已声明的消息契约。同一服务已有 [Rust](sdk/rust/examples/portable/src/adder.rs)、[C++](sdk/cpp/examples/adder.cpp) 和 [TypeScript](sdk/typescript/examples/adder.ts) 实现。

生成需要 `protoc`，TypeScript 还需要 `protoc-gen-es`。准备工具并安装仓库的 npm 依赖后，项目的构建脚本可以调用：

```sh
npm ci --ignore-scripts
npm run codegen -- generate \
  --input /path/to/plugin/api.proto \
  --proto-path /path/to/plugin \
  --out /path/to/plugin/generated \
  --languages rust,cpp,typescript
```

输入、输出和模块引用由使用方的构建配置决定。应用不需要采用 Cordis-X 仓库的目录结构。

## 接入应用

应用按需选择 SDK 和执行后端。Rust 使用工作区或路径依赖，C++ 使用 CMake `add_subdirectory`，TypeScript 通过 npm workspace 或应用自己的构建流程接入 SDK 源码。

应用注册插件来源，并授予依赖和能力。Loader 可以应用声明式配置、准备插件制品，再通过运行时协调变更。应用界面、插件发现与分发、存储位置和信任策略仍由应用开发者决定。

| 示例 | 展示的行为 |
| --- | --- |
| [原生生命周期](sdk/rust/native/examples/lifecycle.rs) | 依赖激活、重启与清理顺序 |
| [分步骤副作用](sdk/rust/native/examples/stepwise.rs) | 为异步初始化中已完成的步骤保留清理操作 |
| [Broker](sdk/rust/native/examples/broker.rs) | 切换新请求，同时保留旧资源 |
| [Loader](sdk/rust/native/examples/loader.rs) | 由应用注册插件来源并处理配置变更 |
| [可移植资源](sdk/rust/examples/portable/src/resources.rs) | 多种执行适配器共用资源业务逻辑 |
| [工作流](sdk/typescript/examples/workflows.ts) | 生成的事件、中间件、子插件工厂和值发布接口 |

## 仓库结构

| 路径 | 内容 |
| --- | --- |
| `crates/` | 运行时、Loader、执行宿主、契约与编译器 |
| `sdk/` | Rust、C++、TypeScript 插件 API 及示例 |
| `protocols/` | 共用的 ABI 头文件、WIT 接口与 Protobuf 定义 |
| `examples/` | 共用的示例契约、资源和构建配置 |
| `tests/` | 共用的测试契约、夹具和构建配置 |
| `tools/codegen/` | 仓库生成入口和工具版本声明 |

生成代码与手写源码分开存放。工具链和目标库由开发者准备，构建命令不会自动安装。

## 参与贡献

欢迎提交问题、修复和示例。报告问题时，请提供最小复现、使用的执行后端，以及预期的生命周期行为。开发环境和测试命令见[贡献指南](CONTRIBUTING.zh-CN.md)，变更记录见 [Changelog](CHANGELOG.md)。

## 许可证

Cordis-X 使用 [Apache-2.0](LICENSE) 许可证。版权声明见 [NOTICE](NOTICE)，仓库内第三方依赖及其许可证见[第三方声明](THIRD_PARTY_NOTICES.md)。
