#pragma once
#include <cordis/adapters/abi/abi.hpp>
#include <cordis/adapters/abi/context.hpp>
// Fault-injection models deliberately declare google.protobuf.Value business
// messages. These explicit JSON helpers describe fixture business logic only;
// every actual ABI call uses the current protobuf buffer carrier.
namespace fixture {
using cordis::Async;
using cordis::Message;
using cordis::Value;
inline Message body(const Value &value) {
  return cordis::protobuf::encode_binary(
      cordis::protobuf::decode<google::protobuf::Value>(value));
}
inline Value json(const Message &value) {
  return cordis::protobuf::encode(
      cordis::protobuf::decode_binary<google::protobuf::Value>(value));
}
inline Async<Value> invoke_json(cordis::abi::Context ctx,
                                std::string dependency, std::string method,
                                Value args = nullptr) {
  co_return json(co_await ctx.invoke(std::move(dependency), std::move(method),
                                     body(args)));
}
struct JsonResource : cordis::abi::Resource {
  virtual Async<Value> invoke_json(std::string, Value) = 0;
  Async<Message> invoke(std::string method, Message args) final {
    co_return body(co_await invoke_json(std::move(method), json(args)));
  }
};
struct JsonService : cordis::abi::Service {
  virtual Async<Value> invoke_json(std::string, Value) = 0;
  virtual Async<std::unique_ptr<cordis::abi::Resource>>
  acquire_json(std::string, Value) {
    throw cordis::Refusal("unsupported", "no resource");
    co_return nullptr;
  }
  Async<Message> invoke(std::string method, Message args) final {
    co_return body(co_await invoke_json(std::move(method), json(args)));
  }
  Async<std::unique_ptr<cordis::abi::Resource>> acquire(std::string kind,
                                                        Message args) final {
    co_return co_await acquire_json(std::move(kind), json(args));
  }
};
struct JsonContextResource : cordis::abi::ContextResource {
  virtual Async<Value> invoke_json(cordis::abi::Context, std::string,
                                   Value) = 0;
  Async<Message> invoke(cordis::abi::Context ctx, std::string method,
                        Message args) final {
    co_return body(
        co_await invoke_json(std::move(ctx), std::move(method), json(args)));
  }
};
struct JsonContextService : cordis::abi::ContextService {
  virtual Async<Value> invoke_json(cordis::abi::Context, std::string,
                                   Value) = 0;
  virtual Async<std::shared_ptr<cordis::abi::ContextResource>>
  acquire_json(cordis::abi::Context, std::string, Value) {
    throw cordis::Refusal("unsupported", "no resource");
    co_return nullptr;
  }
  Async<Message> invoke(cordis::abi::Context ctx, std::string method,
                        Message args) final {
    co_return body(
        co_await invoke_json(std::move(ctx), std::move(method), json(args)));
  }
  Async<std::shared_ptr<cordis::abi::ContextResource>>
  acquire(cordis::abi::Context ctx, std::string kind, Message args) final {
    co_return co_await acquire_json(std::move(ctx), std::move(kind),
                                    json(args));
  }
};
} // namespace fixture
#include <cordis/adapters/abi/composition.hpp>
#include <cordis/adapters/abi/reactive.hpp>
namespace fixture {
inline Value flow_json(const cordis::Flow &flow) {
  return flow.value ? Value{{"kind", "break"}, {"value", json(*flow.value)}}
                    : Value{{"kind", "continue"}};
}
inline Value flows_json(const std::vector<cordis::Flow> &flows) {
  Value out = Value::array();
  for (const auto &flow : flows)
    out.push_back(flow_json(flow));
  return out;
}
inline Async<Value> parallel_json(cordis::abi::Context ctx, std::string name,
                                  Value args) {
  co_return flows_json(co_await ctx.parallel(std::move(name), body(args)));
}
inline Async<Value> serial_json(cordis::abi::Context ctx, std::string name,
                                Value args) {
  co_return flow_json(co_await ctx.serial(std::move(name), body(args)));
}
inline Async<cordis::abi::Dispatch> emit_json(cordis::abi::Context ctx,
                                              std::string name, Value args,
                                              bool serial = false) {
  co_return co_await ctx.emit(std::move(name), body(args), serial);
}
inline Async<Value> next_json(cordis::abi::Next next, Value args) {
  co_return json(co_await next.run(body(args)));
}
inline Async<cordis::abi::Subscription>
on_json(cordis::abi::Context ctx, std::string name,
        std::function<Async<cordis::Flow>(cordis::abi::Context, Value)> handler,
        bool initializing = false) {
  co_return co_await ctx.on(
      std::move(name),
      [handler = std::move(handler)](auto ctx, Message args) {
        return handler(std::move(ctx), json(args));
      },
      initializing);
}
inline Async<cordis::abi::Subscription> middleware_json(
    cordis::abi::Context ctx, std::string name,
    std::function<Async<Value>(cordis::abi::Context, Value, cordis::abi::Next)>
        handler) {
  co_return co_await ctx.middleware(
      std::move(name),
      [handler = std::move(handler)](auto ctx, Message args,
                                     auto next) -> Async<Message> {
        co_return body(
            co_await handler(std::move(ctx), json(args), std::move(next)));
      });
}
inline Async<Value>
run_json(cordis::abi::Context ctx, std::string name, Value args,
         std::function<Async<Value>(cordis::abi::Context, Value)> terminal) {
  co_return json(co_await ctx.run(
      std::move(name), body(args),
      [terminal = std::move(terminal)](auto ctx,
                                       Message input) -> Async<Message> {
        co_return body(co_await terminal(std::move(ctx), json(input)));
      }));
}
inline Async<cordis::abi::Child> install_json(cordis::abi::Context ctx,
                                              std::string name,
                                              Value config = nullptr) {
  co_return co_await ctx.install(std::move(name), body(config));
}
inline Async<void> provide_json(cordis::abi::Context ctx, std::string name,
                                Value value) {
  co_await ctx.provide(std::move(name), body(value));
}
} // namespace fixture
