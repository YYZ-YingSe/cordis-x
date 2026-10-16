#include "json_fixture.hpp"
#include <atomic>
#include <chrono>
#include <cordis/adapters/abi/abi.hpp>
#include <filesystem>
#include <fstream>
#include <set>
#include <thread>

#ifndef CORDIS_ABI_CODE_VERSION
#define CORDIS_ABI_CODE_VERSION "v1"
#endif

using namespace cordis::abi;
using cordis::Async;
namespace {
struct Watch {
  std::mutex mutex;
  std::set<std::string> paths;
  void add(std::string path) {
    std::lock_guard<std::mutex> lock(mutex);
    paths.insert(path);
  }
  void log(std::string event) {
    std::lock_guard<std::mutex> lock(mutex);
    for (const auto &path : paths)
      std::ofstream(path, std::ios::app) << event << '\n';
  }
  ~Watch() { log("unload"); }
} watch;
void trace(Value config, std::string event) {
  std::lock_guard<std::mutex> lock(watch.mutex);
  std::ofstream(config.at("trace").get<std::string>(), std::ios::app)
      << event << '\n';
}
Async<void> gate(Value config, std::string key) {
  const auto path = config.value(key, std::string{});
  if (path.empty())
    co_return;
  trace(config, key + "-entered");
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (!std::filesystem::exists(path)) {
    if (std::chrono::steady_clock::now() > deadline)
      throw std::runtime_error("fixture gate timeout");
    co_await cordis::native::sleep(std::chrono::milliseconds(1));
  }

  co_return;
}
struct Counter final : fixture::JsonResource {
  Value config;
  int64_t value;
  Counter(Value config, int64_t initial)
      : config(std::move(config)), value(initial) {}
  Async<Value> invoke_json(std::string method, Value args) override {
    (co_await gate(config, "resource_gate"));
    if (method == "code-version")
      co_return CORDIS_ABI_CODE_VERSION;
    if (method == "add")
      value += args.get<int64_t>();
    co_return value;
  }
  Async<void> release() override {
    trace(config, "release");
    (co_await gate(config, "release_gate"));
    if (config.value("fail_release", false))
      throw Rejected("cleanup", "release failed");

    co_return;
  }
};
struct Fixture final : fixture::JsonService {
  Value config;
  explicit Fixture(Value config) : config(std::move(config)) {
    trace(this->config, "create");
  }
  ~Fixture() override { trace(config, "destroy"); }
  Async<void> activate() override {
    trace(config, "activate");
    if (config.value("fail_activate", false))
      throw Rejected("startup", "activation failed");

    co_return;
  }
  Async<void> deactivate() override {
    trace(config, "deactivate");
    if (config.value("fail_deactivate", false))
      throw Rejected("cleanup", "deactivation failed");

    co_return;
  }
  Async<Value> invoke_json(std::string method, Value args) override {
    trace(config, "invoke");
    if (method == "code-version" || method == "hold-version") {
      if (method == "hold-version")
        (co_await gate(config, "invoke_gate"));
      co_return CORDIS_ABI_CODE_VERSION;
    }
    if (method == "throw")
      throw std::runtime_error("uncertain operation");
    if (method == "reject")
      throw Rejected("business", "known failure");
    if (method == "hold")
      (co_await gate(config, "invoke_gate"));
    if (method == "add")
      co_return args.at("left").get<int64_t>() +
          args.at("right").get<int64_t>() + config.value("offset", int64_t{0});
    co_return args;
  }
  Async<std::unique_ptr<Resource>> acquire_json(std::string kind,
                                                Value args) override {
    trace(config, "acquire");
    (co_await gate(config, "acquire_gate"));
    if (kind == "reject")
      throw Rejected("kind", "known rejection");
    if (kind == "unknown")
      throw std::runtime_error("unknown acquisition");
    co_return std::make_unique<Counter>(config,
                                        args.value("initial", int64_t{0}));
  }
};
struct Model {
  static cordis::Value interface() {
    return cordis::contract::value_interface(true);
  }
  static std::string contract_name() { return "example.calculator"; }
  static std::string contract_revision() { return "1"; }
  static std::unique_ptr<Service> create(Value config) {
    watch.add(config.at("trace").get<std::string>());
    // Construction is a local C ABI boundary, deliberately blocked by this
    // fixture to verify that cancellation retains its creation responsibility.
    const auto path = config.value("create_gate", std::string{});
    if (!path.empty()) {
      trace(config, "create_gate-entered");
      const auto deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(10);
      while (!std::filesystem::exists(path)) {
        if (std::chrono::steady_clock::now() > deadline)
          throw std::runtime_error("creation gate timeout");
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }
    if (config.value("reject_create", false))
      throw Rejected("create", "known rejection");
    if (config.value("unknown_create", false))
      throw std::runtime_error("unknown creation");
    return std::make_unique<Fixture>(config);
  }
};
using Invoke = decltype(CordisAbiApi{}.invoke);
std::atomic<Invoke> original_invoke;
std::atomic<void (*)(CordisAbiResult *)> original_release;
void release_result(CordisAbiResult *result) noexcept {
  try {
    watch.log("result-free");
  } catch (...) {
  }
  original_release.load()(result);
}
void invoke(uint64_t id, CordisAbiSlice method, const CordisAbiBuffer *args,
            CordisAbiCompletion done) noexcept {
  struct Pending {
    bool invalid;
    CordisAbiCompletion done;
    static void finish(void *data, CordisAbiResult result) noexcept {
      std::unique_ptr<Pending> pending(static_cast<Pending *>(data));
      if (pending->invalid)
        result.value = {nullptr, 1};
      detail::complete(pending->done, result);
    }
  };
  try {
    auto pending = std::make_unique<Pending>(Pending{
        method.len == 7 &&
            std::string_view(reinterpret_cast<const char *>(method.data),
                             method.len) == "invalid",
        done});
    CordisAbiCompletion completion{pending.get(), Pending::finish};
    pending.release();
    original_invoke.load()(id, method, args, completion);
  } catch (...) {
    detail::complete(done, detail::fallback());
  }
}

} // namespace
extern "C" CORDIS_ABI_EXPORT uint32_t cordis_abi_v1(uint32_t version,
                                                    CordisAbiApi *out,
                                                    size_t size) {
  auto result = Host<Model>::entry(version, out, size);
  if (!result) {
    original_invoke = out->invoke;
    out->invoke = invoke;
    original_release = out->release_result;
    out->release_result = release_result;
  }
  return result;
}
