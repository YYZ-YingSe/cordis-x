# Changelog / 变更记录

This file records user-visible changes. Development work is listed under Unreleased until a release is made.

本文件记录对使用者可见的变化。尚未发布的开发内容列在 Unreleased 下，不视为已发布版本。

## Unreleased

### Runtime and SDKs / 运行时与 SDK

- Dependency-driven activation, scoped effects, caller-owned resources, events, middleware, and managed tasks.
  按依赖激活插件，提供作用域副作用、调用者所有的资源、事件、中间件和受管理任务。
- Native Rust, C ABI, process, and WebAssembly execution paths, with Rust, C++, and TypeScript SDKs for their supported backends.
  提供原生 Rust、C ABI、进程和 WebAssembly 执行路径，以及对应的 Rust、C++、TypeScript SDK。
- Provider replacement and Broker routing that retain in-flight work and the implementation behind existing resources.
  提供方替换与 Broker 路由保留在途工作和已有资源所依赖的实现。
- Declarative loading, artifact preparation, and configuration updates through application-registered sources.
  通过应用注册的来源进行声明式加载、制品准备和配置更新。

### Contracts and builds / 契约与构建

- Protobuf-based generation of messages, clients, server interfaces, resource operations, and control operations for Rust, C++, and TypeScript.
  从 Protobuf 定义生成 Rust、C++、TypeScript 消息、客户端、服务端接口、资源操作和控制操作。
- Separate handwritten resources and generated code, with concrete paths owned by framework, example, and test build configuration.
  手写资源与生成代码分离，具体路径由框架、示例和测试的构建配置管理。
- Source workspace integration and developer-managed external toolchains.
  通过源码工作区接入，外部工具链由开发者管理。
- Optional setup scripts for Rust, Python, TypeScript, Protobuf, native C++, and Wasm, with an explicit installation prefix; CI uses the same scripts to prepare its dependencies.
  提供 Rust、Python、TypeScript、Protobuf、原生 C++ 和 Wasm 的可选搭建脚本，安装目录由使用者指定；CI 使用同一套脚本准备依赖。

### Project / 项目

- English and Chinese project introductions and contribution guides, issue forms, and a pull request template.
  提供中英文项目介绍、贡献指南、Issue 表单和 Pull Request 模板。
