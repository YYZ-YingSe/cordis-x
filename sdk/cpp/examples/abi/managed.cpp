#include "dynamic.hpp"
#include <cordis/adapters/abi/managed.hpp>
using namespace cordis::abi;
using cordis::Async;
namespace {
struct Connection {
  Scope scope;
  Task worker;
};
struct Connections : ContextService {
  std::mutex mutex;
  uint64_t next = 1;
  std::unordered_map<uint64_t, std::shared_ptr<Connection>> connections;
  Async<cordis::Message> invoke(Context ctx, std::string method,
                                cordis::Message args) override {
    co_return example::message(
        co_await invoke_json(ctx, std::move(method), example::value(args)));
  }
  Async<Value> invoke_json(Context ctx, std::string method, Value args) {
    if (method == "open") {
      auto connection = std::make_shared<Connection>();
      connection->scope = (co_await ctx.scope("connection"));
      (co_await connection->scope.run([connection](
                                          Context child) -> Async<Value> {
        (co_await child.defer("connection", [](Context cleanup) -> Async<void> {
          (co_await example::call(cleanup, "example.logger", "write",
                                  "connection closed"));

          co_return;
        }));
        auto install = (co_await child.effect(
            "subscriptions", [step = 0](Context) mutable -> Async<Step> {
              auto label = "subscription " + std::to_string(++step);
              Inverse inverse{
                  label, [label](Context cleanup) -> Async<void> {
                    (co_await example::call(cleanup, "example.logger", "write",
                                            label + " removed"));

                    co_return;
                  }};
              co_return step == 2 ? Step::complete(inverse)
                                  : Step::yielded(inverse);
            }));
        (co_await install.ready());
        connection->worker =
            (co_await child.spawn("worker", [](Context task) -> Async<void> {
              (co_await example::call(task, "example.logger", "write",
                                      "worker started"));
              (co_await task.cancelled());
              (co_await example::call(task, "example.logger", "write",
                                      "worker stopped"));
              (co_await task.defer(
                  "last contribution", [](Context cleanup) -> Async<void> {
                    (co_await example::call(cleanup, "example.logger", "write",
                                            "last contribution cleaned"));

                    co_return;
                  }));

              co_return;
            }));
        co_return nullptr;
      }));
      std::lock_guard<std::mutex> lock(mutex);
      auto id = next++;
      connections.emplace(id, connection);
      co_return id;
    }
    if (method == "close") {
      std::shared_ptr<Connection> connection;
      {
        std::lock_guard<std::mutex> lock(mutex);
        connection = connections.at(args.get<uint64_t>());
      }
      (co_await connection->scope.dispose());
      (co_await connection->worker.join());
      co_return nullptr;
    }
    throw Rejected("method", "unknown method");
  }
};
struct Model {
  static cordis::Value interface() {
    return cordis::contract::value_interface(true);
  }
  static std::string contract_name() { return "example.managed"; }
  static std::string contract_revision() { return "1"; }
  static Value dependencies() { return {{"example.logger", "1"}}; }
  static Value dependency_interfaces() {
    return {{"example.logger", cordis::contract::value_interface(true)}};
  }
  static std::shared_ptr<ContextService> create(Value) {
    return std::make_shared<Connections>();
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
extern "C" CORDIS_ABI_EXPORT uint32_t
cordis_abi_managed_v1(uint32_t version, CordisAbiManagedApi *out, size_t size) {
  return ContextHost<Model>::managed_entry(version, out, size);
}
