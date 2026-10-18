#include "json_fixture.hpp"
#include <cordis/adapters/abi/context.hpp>
#include <fstream>
#include <optional>
#include <set>

#ifndef CORDIS_ABI_CODE_VERSION
#define CORDIS_ABI_CODE_VERSION "v1"
#endif
using namespace cordis::abi;
using cordis::Async;
namespace {
struct Watch {
  std::mutex mutex;
  std::set<std::string> paths;
  ~Watch() {
    for (auto &path : paths)
      std::ofstream(path, std::ios::app) << "unload\n";
  }
} watch;
std::mutex saved_mutex;
std::optional<Context> old_instance_context;
void trace(Value config, std::string event) {
  std::lock_guard<std::mutex> lock(watch.mutex);
  std::ofstream(config.at("trace").get<std::string>(), std::ios::app)
      << event << '\n';
}
struct ResourceImpl : fixture::JsonContextResource {
  Async<Value> invoke_json(Context ctx, std::string, Value args) override {
    co_return (co_await fixture::invoke_json(ctx, "example.logger",
                                             "resource_call", args));
  }
  Async<void> release(Context ctx) override {
    (co_await fixture::invoke_json(ctx, "example.logger", "resource_release",
                                   CORDIS_ABI_CODE_VERSION));

    co_return;
  }
};
struct Fixture : fixture::JsonContextService {
  Value config;
  std::mutex mutex;
  uint64_t next = 1;
  std::unordered_map<uint64_t, Effect> effects;
  std::optional<Context> saved;
  explicit Fixture(Value config) : config(std::move(config)) {
    trace(this->config, "create");
  }
  ~Fixture() override { trace(config, "destroy"); }
  Async<void> activate(Context ctx) override {
    trace(config, "activate");
    (co_await fixture::invoke_json(ctx, "example.logger", "activate",
                                   CORDIS_ABI_CODE_VERSION));
    if (config.value("startup_effect", false))
      (co_await subscribe(ctx, "startup"));
    if (config.value("fail_activate", false))
      throw Rejected("startup", "startup failed");

    co_return;
  }
  Async<void> deactivate(Context ctx) override {
    trace(config, "deactivate");
    (co_await fixture::invoke_json(ctx, "example.logger", "deactivate",
                                   CORDIS_ABI_CODE_VERSION));

    co_return;
  }
  Async<uint64_t> subscribe(Context ctx, std::string label) {
    auto captured_config = this->config;
    auto effect = (co_await ctx.defer(
        label, [captured_config, label](Context cleanup) -> Async<void> {
          trace(captured_config, "cleanup:" + label);
          (co_await fixture::invoke_json(cleanup, "example.logger", "cleanup",
                                         std::string(CORDIS_ABI_CODE_VERSION) +
                                             ":" + label));
          if (captured_config.value("fail_cleanup", false))
            throw Rejected("cleanup", "inverse failed");

          co_return;
        }));
    std::lock_guard<std::mutex> lock(mutex);
    auto key = next++;
    effects.emplace(key, effect);
    co_return key;
  }
  Async<Value> invoke_json(Context ctx, std::string method,
                           Value args) override {
    if (method == "save-global") {
      std::lock_guard<std::mutex> lock(saved_mutex);
      old_instance_context = ctx;
      co_return nullptr;
    }
    if (method == "old-host") {
      std::optional<Context> previous;
      {
        std::lock_guard<std::mutex> lock(saved_mutex);
        previous = old_instance_context;
      }
      co_return (co_await fixture::invoke_json(*previous, "example.logger",
                                               "old-host", nullptr));
    }
    if (method == "cleanup-guards") {
      auto own = std::make_shared<Effect>();
      *own =
          (co_await ctx.defer("guards", [own](Context cleanup) -> Async<void> {
            std::optional<Rejected> caught_2;
            try {
              (co_await own->dispose());
              throw std::runtime_error("self disposal unexpectedly returned");
            } catch (const Rejected &e) {
              caught_2 = e;
            }
            if (caught_2) {
              const auto &e = *caught_2;
              (co_await fixture::invoke_json(cleanup, "example.logger",
                                             "dispose_guard", e.code));
            }
            std::optional<Rejected> caught_1;
            try {
              (co_await cleanup.defer(
                  "invalid", [](Context) -> Async<void> { co_return; }));
              throw std::runtime_error(
                  "cleanup registration unexpectedly returned");
            } catch (const Rejected &e) {
              caught_1 = e;
            }
            if (caught_1) {
              const auto &e = *caught_1;
              (co_await fixture::invoke_json(cleanup, "example.logger",
                                             "defer_guard", e.code));
            }

            co_return;
          }));
      co_return nullptr;
    }
    if (method == "subscribe")
      co_return (co_await subscribe(ctx, args.get<std::string>()));
    if (method == "dispose") {
      Effect effect;
      {
        std::lock_guard<std::mutex> lock(mutex);
        effect = effects.at(args.get<uint64_t>());
      }
      (co_await effect.dispose());
      co_return nullptr;
    }
    if (method == "save") {
      std::lock_guard<std::mutex> lock(mutex);
      saved = ctx;
      co_return nullptr;
    }
    if (method == "expired") {
      std::optional<Context> previous;
      {
        std::lock_guard<std::mutex> lock(mutex);
        previous = saved;
      }
      co_return (co_await fixture::invoke_json(*previous, "example.logger",
                                               "expired", nullptr));
    }
    if (method == "undeclared")
      co_return (
          co_await fixture::invoke_json(ctx, "not.declared", "write", args));
    if (method == "echo")
      co_return args;
    if (method == "version")
      co_return CORDIS_ABI_CODE_VERSION;
    co_return (
        co_await fixture::invoke_json(ctx, "example.logger", method, args));
  }
  Async<std::shared_ptr<ContextResource>> acquire_json(Context ctx, std::string,
                                                       Value args) override {
    (co_await fixture::invoke_json(ctx, "example.logger", "resource_acquire",
                                   args));
    co_return std::make_shared<ResourceImpl>();
  }
};
struct Model {
  static cordis::Value interface() {
    return cordis::contract::value_interface(true);
  }
  static std::string contract_name() { return "example.context"; }
  static std::string contract_revision() { return "1"; }
  static Value dependencies() { return {{"example.logger", "1"}}; }
  static Value dependency_interfaces() {
    return {{"example.logger", cordis::contract::value_interface(true)}};
  }
  static std::shared_ptr<ContextService> create(Value config) {
    {
      std::lock_guard<std::mutex> lock(watch.mutex);
      watch.paths.insert(config.at("trace").get<std::string>());
    }
    return std::make_shared<Fixture>(config);
  }
};
} // namespace
extern "C" CORDIS_ABI_EXPORT uint32_t cordis_abi_v1(uint32_t version,
                                                    CordisAbiApi *out,
                                                    size_t size) {
  return ContextHost<Model>::entry(version, out, size);
}
extern "C" CORDIS_ABI_EXPORT uint32_t
cordis_abi_context_v1(uint32_t version, CordisAbiContextApi *out, size_t size) {
  return ContextHost<Model>::context_entry(version, out, size);
}
