# 参与 Cordis-X 开发

[English](CONTRIBUTING.md) · 简体中文

尽量让一次修改能够独立说明和评审。新增 API 或改变生命周期行为前，先通过 Issue 描述使用场景与预期行为，再展开较大的实现。问题修复和文字更正可以直接提交 Pull Request。

Issue 和 Pull Request 均可使用中文或英文。讨论代码和取舍，避免人身攻击，不公开他人的私人信息。提供足够的上下文，让其他贡献者能够复现结果或判断设计。

## 开发环境

以下命令均在仓库根目录执行。Shell 示例使用 POSIX 语法，Windows 请通过所用 Shell 设置对应的环境变量。

按修改范围准备工具：

| 范围 | 前提 |
| --- | --- |
| 原生 Rust 运行时 | Rust 1.96+、Cargo |
| Protobuf 生成与 Rust wire 绑定 | `protoc`；已提交的 C++ 生成代码使用 Protobuf 34.1 |
| TypeScript SDK 与生成 | Node.js 24+、npm |
| 框架描述资源生成与 Python 测试对端 | Python 3.10+ 及已声明的 Protobuf Python 依赖 |
| 原生 C++ | C++20 编译器、CMake 3.20+、Protobuf 34.1.0 和 Abseil 20260817 的 CMake packages |
| Rust ABI 绑定 | libclang |
| Rust Wasm | Rust 的 `wasm32-unknown-unknown` 目标 |
| C++ Wasm | WASI SDK、`wit-bindgen` 0.61.1、宿主 `protoc`、为 WASI 构建的 Protobuf/Abseil 库 |

构建命令调用环境中已有的工具。可以自行准备，也可以显式运行 `tools/setup/` 中的安装脚本。`PROTOC` 和 `CORDIS_PROTOC_GEN_ES` 可以指定生成器程序，否则从 `PATH` 查找。CMake 依赖通过 `CMAKE_PREFIX_PATH` 或对应的包变量配置。

### 可选的搭建脚本

搭建脚本需要 Python 3.12+。系统前提脚本覆盖使用 Homebrew 的 macOS、使用 apt 的 Ubuntu 24.04+/Debian 13+ 和使用 winget 的 Windows x64。它们通过系统工具安装编译器、Python 和 Git，安装过程可能要求管理员权限。Apple 命令行工具通过系统安装窗口完成；Windows 安装前提后需要重新打开终端。其他 Linux 发行版可通过自己的包管理器准备同样的前提。

```sh
bash tools/setup/system.sh
python3 tools/setup/setup.py --prefix "$HOME/.local/share/cordis-tools" --jobs 4
source "$HOME/.local/share/cordis-tools/env.sh"
```

```powershell
./tools/setup/system.ps1
# 在新终端中执行：
py -3.12 tools/setup/setup.py --prefix "$env:LOCALAPPDATA/CordisTools" --jobs 4
. "$env:LOCALAPPDATA/CordisTools/env.ps1"
```

`--prefix` 必填，工具、下载文件、依赖源码和编译缓存均存放在该目录中。脚本不修改 shell 配置，不替换系统的 Rust/Node；Rust 的 Cargo/rustup home 和 Python 虚拟环境位于该目录中。Node 依赖通过 `npm ci --ignore-scripts` 安装到当前仓库。重复执行会复用下载和 CMake 构建结果。安装结束后，在需要使用工具的终端加载生成的环境文件；安装脚本无法激活父进程的 shell。该目录包含绝对路径，安装后请保留原位置。

每种工具链都有独立入口，接受相同的 `--prefix` 和 `--jobs` 参数，并自动包含自身需要的依赖：

| `tools/setup/` 中的脚本 | 安装内容 |
| --- | --- |
| `rust.py` | Rust/Cargo 1.98.1、rustfmt、Clippy、`wasm32-unknown-unknown` |
| `python.py` | 虚拟环境、仓库 Python 依赖、CMake 4.4.4、Ninja 1.13.0、libclang 18.1.1 |
| `typescript.py` | Node.js 24.14.1 及 npm；根 npm 锁文件中的 TypeScript、`protoc-gen-es` 和 esbuild |
| `protoc.py` | 宿主 `protoc` 34.1 及标准 `.proto` 文件 |
| `cpp.py` | Python 构建工具、`protoc`、原生静态 Protobuf 34.1.0 和 Abseil 20260817.0；macOS 使用共享 Abseil 并加入线程存储适配 |
| `wasm.py` | Rust/Python 工具、宿主 `protoc`、WASI SDK 34.0、`wit-bindgen` 0.61.1、为 WASI 编译的静态 Protobuf/Abseil、`cordis-component-pack` |
| `setup.py` | 上述全部工具，或 `--only` 选择的部分 |

例如，`python3 tools/setup/setup.py --prefix /path/to/tools --only rust protoc` 准备 Rust/protoc 路径。代码生成和 TypeScript 开发再加入 `python typescript`；完整集成测试再加入 `cpp wasm`。原生 C++ 使用系统前提步骤提供的编译器。Windows 同时安装 Debug 和 Release 依赖，以匹配使用方的 MSVC 运行库。

原生 C++ 使用系统编译器，也可以通过 `CC`/`CXX` 显式选择。Windows 由 CMake 选择已安装的 Visual Studio 实例，包括 VS 2026，不再假定某个固定 IDE 版本。

[versions.json](tools/setup/versions.json)固定官方二进制、源码归档及 SHA-256；Python 工具版本在 [requirements.txt](tools/setup/requirements.txt)，npm 和 Cargo 依赖使用各自锁文件。系统编译器和 Python 的补丁版本跟随所用包管理器。二进制安装配方覆盖 Linux/macOS x64、ARM64，以及 Windows x64。平台实际验收仍由 CI 执行，有安装配方本身不代表后端已通过验证。

CI 使用同一套脚本，通过 `--github-env` 把环境传给后续步骤。安装失败会直接中止 job，不会跳过缺失后端后报告测试成功。普通 Cargo、CMake、npm 生成和 SDK 命令均不调用安装脚本。

### 使用已有环境

修改 TypeScript、运行代码生成或进程测试时，先准备依赖：

```sh
npm ci --ignore-scripts
python3 -m pip install -r tools/codegen/requirements.txt -r tests/requirements.txt
export PATH="$PWD/node_modules/.bin:$PATH"
export CORDIS_TEST_PYTHON="$(python3 -c 'import sys; print(sys.executable)')"
```

如果 Python 环境要求使用虚拟环境，请先创建并激活。npm workspace 共用根锁文件。生成工具版本在[生成工具 workspace](tools/codegen/typescript/package.json)中声明，运行依赖放在实际使用它的包中。

macOS 上的 C++ ABI 卸载需要私有静态 Protobuf 和由宿主共享的 Abseil。自行提供的 Protobuf 构建需要包含[线程存储适配](sdk/cpp/tools/protobuf/macos-thread-storage.patch)及其[头文件](sdk/cpp/tools/protobuf/cordis_thread_storage.h)、[实现](sdk/cpp/tools/protobuf/cordis_thread_storage.cc)。可选的 `cpp.py` 安装脚本会完成这一步，SDK CMake 只检查对应支持。WASI 使用单独构建的目标库，不能将原生依赖前缀用于 Wasm 构建。

## 源码与生成文件

修改 `.proto` 源文件，不直接修改生成的客户端或消息类型。更新仓库内的全部生成结果：

```sh
npm run generate
```

该命令先生成框架资源，再构建 `cordis-codegen`，随后生成共用的示例和测试契约。它使用已安装的 npm 工具版本，不会下载工具链。

生成路径由三个构建入口管理：

- [框架](tools/codegen/build.py)：公共协议资源与 SDK 协议模块。
- [示例](examples/build.py)：输入清单为 [examples/contracts.json](examples/contracts.json)。
- [测试](tests/build.py)：输入清单为 [tests/contracts.json](tests/contracts.json)，以及测试专用的协议绑定和资源。

手写资源、手写代码和生成产物分开存放。业务代码通过具名 crate、npm 包或 C++ 头文件导入。具体路径写在构建脚本、Cargo build.rs、包 exports 和 CMake 配置中。新增语言应扩展现有编译器后端，不再建立第二套契约系统。

## 设计约定

Cordis-X 仍处于开发阶段。尚未发布的接口发生变化时，同步更新实现、调用方、示例和测试，并删除被替代的路径。没有实际已发布使用方的兼容需求时，不增加旧名称别名、协议探测或迁移层。

生命周期行为是 API 的一部分。修改需要保留所有权、授权、在途排空、取消语义和清理责任。尤其不能根据调用者取消等待，就认定远端操作没有执行。

校验策略由宿主绑定选择：默认使用 `checked`，受信绑定可以明确选择 `contract`，诊断使用 `audit`。避免每经过一个后端就重复解码或全量检查。内存表示、硬资源上限、权限和资源所有权仍须执行。

## 运行测试

使用已有的 Cargo、Node 和 CTest 入口。运行与改动有关的测试，并如实记录执行范围；没有可用环境的平台不能记作通过。

原生运行时修改：

```sh
cargo test --locked -p cordis-kernel -p cordis-runtime
```

契约编译器修改，需要准备 `protoc`、`protoc-gen-es`、Node 和原生 C++ 依赖：

```sh
cargo test --locked -p cordis-codegen
```

TypeScript 修改：

```sh
npm run test:typescript
npx --no-install tsc -p sdk/typescript/tsconfig.json
```

原生 C++ 修改：

```sh
cmake -S sdk/cpp -B target/cpp \
  -DCMAKE_PREFIX_PATH=/path/to/native/dependencies \
  -DCORDIS_BUILD_EXAMPLES=ON -DBUILD_TESTING=ON
cmake --build target/cpp --config Debug --parallel 2
ctest --test-dir target/cpp -C Debug --output-on-failure
```

Windows 默认关闭 C++ 进程适配器。运行 Wasm 测试前，先准备目标编译器与依赖库。`CORDIS_WASM_RUSTC` 可以指定单独的 Rust guest 编译器。[C++ 组件构建工具](sdk/cpp/tools/build-component.py)接受显式的工具链、依赖、源码和输出路径，具体参数可用 `--help` 查看。

具备所需后端环境后，工作区检查命令为：

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets \
  --features cordis-wasm/loader,cordis-abi/loader,cordis-remote/loader \
  --locked -- -D warnings
cargo test --workspace \
  --features cordis-wasm/loader,cordis-abi/loader,cordis-remote/loader --locked
```

部分集成测试会构建并启动真实插件，不要同时对同一个输出目录运行其他构建。复用已有构建产物，无需每次测试前清理缓存。

## 提交修改

说明要解决的问题、修改后的行为和验证方法。涉及生命周期时，说明已经进入的工作和已有资源如何处理。现有测试不能覆盖新增行为时，补充相应回归测试；纯文档修改不需要运行无关测试。

修改契约时一并提交重新生成的产物。保持中英文 README 和贡献指南内容一致。对使用者可见的变化写入 [CHANGELOG.md](CHANGELOG.md) 的未发布部分。

报告问题时提供最小复现、源码版本、操作系统、工具版本和执行后端。提出功能建议时，先说明应用的需求和期望的 API 示例，再讨论实现方式。

贡献采用项目的 [Apache-2.0 许可证](LICENSE)。修改仓库内第三方依赖时，保留其版权和许可证声明。
