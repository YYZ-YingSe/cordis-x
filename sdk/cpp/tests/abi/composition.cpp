#include "json_fixture.hpp"
#include <cordis/adapters/abi/composition.hpp>
#include <cordis/adapters/abi/reactive.hpp>
#include <fstream>
#include <set>
using namespace cordis::abi;
using cordis::Async;
namespace {
struct Watch {
  std::mutex mutex;
  std::set<std::string> paths;
  ~Watch() {
    for (const auto &path : paths)
      std::ofstream(path, std::ios::app) << "unload\n";
  }
} watch;
void trace(Value config, std::string event) {
  std::lock_guard<std::mutex> lock(watch.mutex);
  std::ofstream(config.at("trace").get<std::string>(), std::ios::app)
      << event << '\n';
}
struct Fixture : fixture::JsonContextService {
  Value config;
  std::mutex mutex;
  std::vector<Child> children;
  std::optional<Context> saved;
  explicit Fixture(Value config) : config(std::move(config)) {
    trace(this->config, "create");
  }
  ~Fixture() override { trace(config, "destroy"); }
  size_t add(Child child) {
    std::lock_guard<std::mutex> lock(mutex);
    children.push_back(child);
    return children.size() - 1;
  }
  Child child(Value index) {
    std::lock_guard<std::mutex> lock(mutex);
    return children.at(index.get<size_t>());
  }
  Async<void> activate(Context ctx) override {
    trace(config, "activate");
    saved = ctx;
    if (config.contains("child")) {
      auto c = (co_await fixture::install_json(ctx, "worker", config["child"]));
      add(c);
      if (config.value("ready", true))
        (co_await c.ready());
    }
    if (!config.value("missing", false)) {
      Value info = {{"version", 1}};
      co_await fixture::provide_json(ctx, "info", std::move(info));
    }
    if (config.value("duplicate", false))
      (co_await fixture::provide_json(ctx, "info", 2));
    if (config.value("hold", false))
      (co_await fixture::invoke_json(ctx, "example.logger", "hold"));
    if (config.value("fail", false))
      throw Rejected("startup", "initialization failed");

    co_return;
  }
  Async<void> deactivate(Context ctx) override {
    trace(config, "deactivate");
    try {
      (co_await fixture::install_json(ctx, "worker"));
    } catch (const Rejected &e) {
      trace(config, "cleanup-install:" + e.code);
    }
    (co_await fixture::invoke_json(ctx, "example.logger", "deactivate"));

    co_return;
  }
  Async<Value> invoke_json(Context ctx, std::string method,
                           Value args) override {
    if (method == "echo")
      co_return args;
    if (method == "install")
      co_return add((co_await fixture::install_json(
          ctx, args.value("factory", "worker"),
          args.value("config", Value::object()))));
    if (method == "ready")
      co_return (co_await child(args).ready());
    if (method == "settled")
      co_return (co_await child(args).settled());
    if (method == "status")
      co_return (co_await child(args).status());
    if (method == "dispose") {
      (co_await child(args).dispose());
      co_return nullptr;
    }
    if (method == "provide") {
      (co_await fixture::provide_json(ctx, args.value("alias", "info"),
                                      args.value("value", Value())));
      co_return nullptr;
    }
    if (method == "expired")
      co_return add((co_await fixture::install_json(*saved, "worker")));
    if (method == "scope") {
      auto scope = (co_await ctx.scope("children"));
      auto value = (co_await scope.run([this](Context nested) -> Async<Value> {
        auto c =
            (co_await fixture::install_json(nested, "worker", Value::object()));
        (co_await c.ready());
        co_return add(c);
      }));
      (co_await scope.dispose());
      co_return value;
    }
    if (method == "terminal") {
      co_return (co_await fixture::run_json(
          ctx, "chain", nullptr, [this](Context nested, Value) -> Async<Value> {
            auto c = (co_await fixture::install_json(nested, "worker",
                                                     Value::object()));
            (co_await c.ready());
            co_return add(c);
          }));
    }
    throw Rejected("method", "unknown method");
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
  static Value composition() {
    return {{"factories", {"worker"}},
            {"provisions",
             {{"info", {{"name", "example.info"}, {"revision", "1"}}}}}};
  }
  static Value bindings() {
    return {{"events", Value::object()},
            {"middleware",
             {{"chain", {{"name", "example.chain"}, {"revision", "1"}}}}}};
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
extern "C" CORDIS_ABI_EXPORT uint32_t cordis_abi_v1(uint32_t v,
                                                    CordisAbiApi *out,
                                                    size_t size) {
  return ContextHost<Model>::entry(v, out, size);
}
extern "C" CORDIS_ABI_EXPORT uint32_t
cordis_abi_context_v1(uint32_t v, CordisAbiContextApi *out, size_t size) {
  return ContextHost<Model>::context_entry(v, out, size);
}
extern "C" CORDIS_ABI_EXPORT uint32_t cordis_abi_composition_v1(
    uint32_t v, CordisAbiCompositionApi *out, size_t size) {
  return ContextHost<Model>::composition_entry(v, out, size);
}
#ifdef COMPOSITION_REACTIVE
extern "C" CORDIS_ABI_EXPORT uint32_t
cordis_abi_managed_v1(uint32_t v, CordisAbiManagedApi *out, size_t size) {
  return ContextHost<Model>::managed_entry(v, out, size);
}
extern "C" CORDIS_ABI_EXPORT uint32_t
cordis_abi_reactive_v1(uint32_t v, CordisAbiReactiveApi *out, size_t size) {
  return ContextHost<Model>::reactive_entry(v, out, size);
}
#endif
