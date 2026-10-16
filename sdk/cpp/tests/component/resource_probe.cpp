#include "../message.hpp"
#include <cordis/adapters/component.hpp>

namespace probe {
using namespace cordis;
namespace backend = cordis::component::detail;
bool enabled(const Value &args, const std::string &key) {
  return args.is_object() && args.value(key, false);
}
class Object final : public Resource {
  Value args;
  Context old;

public:
  Object(Value args, Context old)
      : args(std::move(args)), old(std::move(old)) {}
  Async<Message> invoke(Context ctx, std::string method, Message) override {
    co_return test::body(co_await invoke_json(ctx, std::move(method)));
  }
  Async<Value> invoke_json(Context ctx, std::string method) {
    if (method == "expired") {
      try {
        co_await old.invoke("example.logger", "must-not-run",
                            test::body(nullptr));
      } catch (const RpcError &) {
        co_return true;
      }
      co_return false;
    }
    co_return test::json(
        co_await ctx.invoke("example.logger", "call", test::body(args)));
  }
  Async<void> close(Context ctx) override {
    if (enabled(args, "probe_cleanup")) {
      bool rejected = false;
      try {
        co_await ctx.acquire("example.logger", "forbidden",
                             test::body(nullptr));
      } catch (const RpcError &e) {
        rejected = e.code == "cleanup_resource";
      }
      if (!rejected)
        throw std::runtime_error("cleanup acquired a resource");
    }
    co_await ctx.invoke("example.logger", "release", test::body(args));
    if (enabled(args, "panic_release"))
      throw std::runtime_error("close exception injection");
    if (enabled(args, "trap_release"))
      __builtin_trap();
    if (enabled(args, "fail_release"))
      throw RpcError("close_failed", "retain resource");
  }
};
class Service final : public cordis::Service {
public:
  Async<void> activate(Context ctx) override {
    co_await ctx.invoke("example.logger", "activate", test::body(nullptr));
  }
  Async<void> deactivate(Context ctx) override {
    co_await ctx.invoke("example.logger", "deactivate", test::body(nullptr));
  }
  Async<Message> invoke(Context ctx, std::string method,
                        Message args) override {
    co_return test::body(
        co_await invoke_json(ctx, std::move(method), test::json(args)));
  }
  Async<Value> invoke_json(Context ctx, std::string method, Value args) {
    if (method == "reentered")
      co_return 42;
    if (method == "raw")
      co_return co_await backend::request(
          backend::current()->shared_from_this(),
          args.at("command").get<std::string>(), args.at("args"));
    co_return test::json(
        co_await ctx.invoke("example.logger", method, test::body(args)));
  }
  Async<std::shared_ptr<Resource>> acquire(Context ctx, std::string,
                                           Message input) override {
    auto args = test::json(input);
    co_await ctx.invoke("example.logger", "acquire", test::body(args));
    if (enabled(args, "cancelled_import")) {
      struct Orphan {
        std::string token = backend::current()->token;
        std::string command = "invoke";
        backend::Buffer buffer{
            backend::Payload(Value{{"dependency", "example.logger"},
                                   {"method", "reject-import"}},
                             test::body(nullptr))};
        cordis_plugin_host_call_args_t args{backend::borrowed(token),
                                            backend::borrowed(command),
                                            buffer.view()};
        cordis_plugin_host_result_payload_failure_t result{};
      };
      Orphan orphan;
      auto status = cordis_plugin_host_call(&orphan.args, &orphan.result);
      auto handle = MANAGED_PLUGIN_SUBTASK_HANDLE(status);
      co_await ctx.invoke("example.logger", "reject-barrier",
                          test::body(nullptr));
      auto cancelled = managed_plugin_subtask_cancel(handle);
      if (MANAGED_PLUGIN_SUBTASK_STATE(cancelled) !=
          MANAGED_PLUGIN_SUBTASK_RETURNED_CANCELLED)
        throw std::runtime_error("import cancellation was not acknowledged");
      managed_plugin_subtask_drop(handle);
      co_await ctx.invoke("example.logger", "orphan-cancelled",
                          test::body(nullptr));
    }
    if (enabled(args, "panic_acquire"))
      throw std::runtime_error("acquire exception injection");
    if (enabled(args, "trap_acquire"))
      __builtin_trap();
    if (enabled(args, "unknown"))
      throw backend::Unknown("uncertain acquisition");
    if (enabled(args, "reject"))
      throw RpcError("factory_rejected", "no acquired resource");
    co_return std::make_shared<Object>(args, ctx);
  }
};
struct Model {
  static cordis::Value interface() {
    return cordis::contract::value_interface(true);
  }
  static std::string contract_name() { return "example.counters"; }
  static std::string contract_revision() { return "1"; }
  static Value dependencies() { return {{"example.logger", "1"}}; }
  static Value dependency_interfaces() {
    return {{"example.logger", cordis::contract::value_interface(true)}};
  }
  static std::shared_ptr<cordis::Service> create(const Value &) {
    return std::make_shared<Service>();
  }
};
} // namespace probe
CORDIS_COMPONENT_PLUGIN(probe::Model)
