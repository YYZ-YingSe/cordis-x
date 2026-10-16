#include "dynamic.hpp"
#include <cordis/adapters/abi/context.hpp>

using namespace cordis::abi;
using cordis::Async;
namespace {
struct Notifications : ContextService {
  std::mutex mutex;
  uint64_t next = 1;
  std::unordered_map<uint64_t, Effect> subscriptions;

  Async<cordis::Message> invoke(Context ctx, std::string method,
                                cordis::Message args) override {
    co_return example::message(
        co_await invoke_json(ctx, std::move(method), example::value(args)));
  }
  Async<Value> invoke_json(Context ctx, std::string method, Value args) {
    if (method == "log")
      co_return (co_await example::call(ctx, "example.logger", "write", args));
    if (method == "subscribe") {
      auto topic = args.get<std::string>();
      auto effect = (co_await ctx.defer(
          "subscription: " + topic, [topic](Context cleanup) -> Async<void> {
            (co_await example::call(cleanup, "example.logger", "write",
                                    "unsubscribe " + topic));

            co_return;
          }));
      std::lock_guard<std::mutex> lock(mutex);
      auto id = next++;
      subscriptions.emplace(id, effect);
      co_return id;
    }
    if (method == "unsubscribe") {
      Effect effect;
      {
        std::lock_guard<std::mutex> lock(mutex);
        auto found = subscriptions.find(args.get<uint64_t>());
        if (found == subscriptions.end())
          throw Rejected("closed", "unknown subscription");
        effect = found->second;
      }
      (co_await effect.dispose());
      co_return nullptr;
    }
    throw Rejected("method", "unknown method");
  }
};
struct Model {
  static cordis::Value interface() {
    return cordis::contract::value_interface(true);
  }
  static std::string contract_name() { return "example.notifications"; }
  static std::string contract_revision() { return "1"; }
  static Value dependencies() { return {{"example.logger", "1"}}; }
  static Value dependency_interfaces() {
    return {{"example.logger", cordis::contract::value_interface(true)}};
  }
  static std::shared_ptr<ContextService> create(Value) {
    return std::make_shared<Notifications>();
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
