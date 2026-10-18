# Third-party notices / 第三方声明

Cordis-X's own code is licensed under Apache-2.0. The bundled libraries below retain their original licenses and notices. Their license texts are included alongside the source.

Cordis-X 自有代码使用 Apache-2.0。下列随仓库提供的依赖保留原有许可证与声明，许可证正文与源码一同保存。

| Library / 依赖 | Source version / 来源版本 | License / 许可证 | Included source / 仓库位置 |
| --- | --- | --- | --- |
| JSON for Modern C++ | 3.12.0 | [MIT](sdk/cpp/third_party/nlohmann/LICENSE.MIT) | [nlohmann/json.hpp](sdk/cpp/third_party/nlohmann/json.hpp) |
| Corral | Commit `5d1153705395640c9f9f8c91e242c9932cd0bd0e` | [MIT](sdk/cpp/third_party/corral/COPYING) | [corral](sdk/cpp/third_party/corral) |
| Standalone Asio | 1.38.2 | [Boost Software License 1.0](sdk/cpp/third_party/asio/LICENSE_1_0.txt) | [asio](sdk/cpp/third_party/asio) |

## Local adaptations / 本地适配

The bundled nlohmann/json and Asio headers are unmodified. Cordis-X selects Asio's existing thread-key storage option for native libraries that need to unload.

仓库内 nlohmann/json 和 Asio 头文件未修改。对于需要卸载的原生动态库，Cordis-X 选择 Asio 已有的线程键存储选项。

Corral has local portability changes: native return-address capture is disabled on WebAssembly, and executor pointers can use a storage accessor. The native adapter uses pthread storage on macOS so thread-local destructors do not keep a retired plugin library loaded. The masked task tag is explicitly converted to its integer type to avoid an MSVC narrowing diagnostic. These changes do not replace Corral's coroutine, cancellation, or task ownership algorithms.

Corral 包含本地移植修改：在 WebAssembly 上关闭原生返回地址采集，并允许执行器指针使用存储访问器。macOS 原生适配器使用 pthread 存储，避免线程局部析构记录阻止已退出的插件动态库卸载。对掩码后的任务标签显式转换为对应整数类型，消除 MSVC 的窄化告警。这些修改没有替换 Corral 的协程、取消或任务所有权算法。

Corral's callback portal omits a dead return after its terminating assertion. A local MSVC C4702 diagnostic override is limited to `BasicNursery::wrapError`, preserving the defensive assertion for custom error policies after built-in non-returning policies. Other code retains unreachable-code diagnostics.

Corral 的回调入口移除了终止断言后的不可达返回。MSVC C4702 的局部诊断覆盖仅限 `BasicNursery::wrapError`，保留内置不返回错误策略之后对自定义错误策略的防御断言。其他代码继续执行不可达代码检查。

## External dependencies / 外部依赖

Rust and JavaScript dependencies are declared in Cargo and npm manifests and resolved by the repository lockfiles. Their upstream licenses remain applicable. Protobuf and Abseil C++ libraries, WASI SDK, and other compiler tools are provided by the developer and are not included as tool binaries in this source tree.

Rust 和 JavaScript 依赖在 Cargo、npm 清单中声明，由仓库锁文件确定版本，仍使用各自的上游许可证。Protobuf、Abseil 的 C++ 库、WASI SDK 及其他编译工具由开发者提供，本源码树不包含这些工具的二进制发行包。

The [Protobuf adaptation files](sdk/cpp/tools/protobuf) describe the macOS thread-storage change required by the native ABI build. They do not change the license of the external Protobuf or Abseil distribution.

[Protobuf 适配文件](sdk/cpp/tools/protobuf)提供原生 ABI 构建需要的 macOS 线程存储修改，不改变外部 Protobuf 或 Abseil 发行包的许可证。
