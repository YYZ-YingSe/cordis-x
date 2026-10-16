#pragma once
#include "message.hpp"
#include <cordis/cordis.hpp>
#include <cordis/core/contract.hpp>
#include <mutex>
using namespace cordis;
// Raw arguments intentionally bypass generated business dispatch in failure
// tests.
class ProbeClient final : public Service {
  std::mutex mutex;
  std::optional<ResourceHandle> resource;

public:
  Async<Message> invoke(Context ctx, std::string method,
                        Message args) override {
    if (method == "scoped") {
      auto counter =
          (co_await ctx.acquire("example.counters", "counter", args));
      auto scope = (co_await ctx.scope("counter scope"));
      (co_await scope.ready());
      auto result =
          (co_await scope.run([counter](Context inner) -> Async<Value> {
            (co_await inner.defer(
                "read before close", [counter](Context cleanup) -> Async<void> {
                  (co_await counter.invoke(cleanup, "get",
                                           test::body(Value::object())));
                }));
            co_return test::json(co_await counter.invoke(
                inner, "get", test::body(Value::object())));
          }));
      (co_await scope.dispose());
      (co_await counter.dispose(ctx));
      co_return test::body(result);
    }
    if (method == "acquire") {
      auto acquired =
          (co_await ctx.acquire("example.counters", "counter", args));
      std::lock_guard lock(mutex);
      resource = std::move(acquired);
      co_return test::body(nullptr);
    }
    const auto saved = [this] {
      std::lock_guard lock(mutex);
      if (!resource)
        throw RpcError("empty", "acquire a counter first");
      return *resource;
    }();
    // Neither a user mutex nor the original callback spans a host call.
    if (method == "dispose") {
      (co_await saved.dispose(ctx));
      co_return test::body(nullptr);
    }
    co_return (co_await saved.invoke(ctx, method, args));
  }
};
struct ProbeClientModel {
  static cordis::Value interface() {
    return cordis::contract::value_interface(true);
  }
  static std::string contract_name() { return "example.counter-client"; }
  static std::string contract_revision() { return "1"; }
  static Value dependency_interfaces() {
    return {{"example.counters", cordis::contract::value_interface(true)}};
  }
  static Value dependencies() { return {{"example.counters", "1"}}; }
  static std::shared_ptr<Service> create(const Value &) {
    return std::make_shared<ProbeClient>();
  }
};
