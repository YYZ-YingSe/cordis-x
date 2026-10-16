#pragma once
#include <cordis/adapters/abi/managed.hpp>

namespace cordis::abi {
namespace detail {
inline Async<HostReply>
reactive_call(CordisAbiContext ctx, const CordisAbiReactiveHostApi *api,
              uint32_t operation, uint64_t target = 0, uint64_t callback = 0,
              std::string alias = {}, Payload args = {}) {
  if (!api)
    throw Refusal("unsupported", "Reactive extension unavailable");
  Output input;
  Budget budget;
  auto raw = input.build(args, budget);
  co_return co_await receive(
      ctx.api, ctx.host,
      operation == CORDIS_ABI_EVENT_ON ||
          operation == CORDIS_ABI_MIDDLEWARE_USE ||
          operation == CORDIS_ABI_EVENT_EMIT,
      [&](uint64_t caller, CordisAbiCompletion completion) {
        api->call(ctx.host, ctx.grant, operation, target, callback,
                  slice(alias), &raw, caller, completion);
      });
}
inline std::vector<Flow> flows(const Payload &payload) {
  if (!payload.control.is_array())
    throw std::runtime_error("invalid event result");
  std::vector<Flow> out;
  std::size_t body = 0;
  for (const auto &tag : payload.control) {
    if (tag == "continue")
      out.push_back(Continue());
    else if (tag == "break" && body < payload.messages.size())
      out.push_back(Break(payload.messages[body++]));
    else
      throw std::runtime_error("invalid event result");
  }
  if (body != payload.messages.size())
    throw std::runtime_error("unexpected event body");
  return out;
}
struct ReactiveHandle {
  CordisAbiContext context{};
  const CordisAbiReactiveHostApi *api = nullptr;
  uint64_t id = 0;
  ReactiveHandle() = default;
  ReactiveHandle(CordisAbiContext context, const CordisAbiReactiveHostApi *api,
                 uint64_t id)
      : context(context), api(api), id(id) {
    this->context.grant = 0;
  }
  Async<Payload> call(uint32_t op) const {
    return cordis::detail::invoke_owned(
        [*this](uint32_t op) -> Async<Payload> {
          co_return (co_await reactive_call(context, api, op, id)).first;
        },
        op);
  }
};
} // namespace detail
class Subscription : private detail::ReactiveHandle {
  friend class Context;
  using ReactiveHandle::ReactiveHandle;

public:
  Subscription() = default;
  Async<void> dispose() const {
    return cordis::detail::invoke_owned([*this]() -> Async<void> {
      (co_await call(CORDIS_ABI_SUBSCRIPTION_DISPOSE)).control_only();
      co_return;
    });
  }
};
class Dispatch : private detail::ReactiveHandle {
  friend class Context;
  using ReactiveHandle::ReactiveHandle;

public:
  Dispatch() = default;
  Async<std::vector<Flow>> join() const {
    return cordis::detail::invoke_owned([*this]() -> Async<std::vector<Flow>> {
      co_return detail::flows(co_await call(CORDIS_ABI_DISPATCH_JOIN));
    });
  }
  Async<Value> snapshot() const {
    return cordis::detail::invoke_owned([*this]() -> Async<Value> {
      co_return (co_await call(CORDIS_ABI_DISPATCH_STATUS)).control_only();
    });
  }
};
class Next {
  CordisAbiContext context_{};
  const CordisAbiReactiveHostApi *api_ = nullptr;
  uint64_t id_ = 0;
  friend class Context;
  Next(CordisAbiContext ctx, const CordisAbiReactiveHostApi *api, uint64_t id)
      : context_(ctx), api_(api), id_(id) {}

public:
  Next() = default;
  Async<Message> run(Message input) const {
    return cordis::detail::invoke_owned(
        [*this](Message input) -> Async<Message> {
          co_return (co_await detail::reactive_call(context_, api_,
                                                    CORDIS_ABI_NEXT_RUN, id_, 0,
                                                    {}, Payload(input)))
              .first.one();
        },
        std::move(input));
  }
};
inline Async<Subscription>
Context::on(std::string alias,
            std::function<Async<Flow>(Context, Message)> handler,
            bool initializing) const {
  return cordis::detail::invoke_owned(
      [*this](std::string alias,
              std::function<Async<Flow>(Context, Message)> handler,
              bool initializing) -> Async<Subscription> {
        auto callback = callbacks_->insert_payload(
            [handler = std::move(handler)](uint32_t op, Context ctx,
                                           Payload args) -> Async<Payload> {
              if (op == CORDIS_ABI_CALLBACK_RELEASE) {
                args.control_only();
                co_return Payload();
              }
              if (op != CORDIS_ABI_EVENT_CALL)
                throw Refusal("operation", "invalid event callback");
              co_return session::flow_payload(
                  co_await handler(ctx, args.one()));
            });
        try {
          auto id = (co_await detail::reactive_call(
                         raw_, reactive_, CORDIS_ABI_EVENT_ON, 0, callback,
                         alias, Payload(Value(initializing))))
                        .second;
          co_return Subscription(raw_, reactive_, id);
        } catch (const Rejected &) {
          callbacks_->erase_managed(callback);
          throw;
        }
      },
      std::move(alias), std::move(handler), initializing);
}
inline Async<Subscription> Context::middleware(
    std::string alias,
    std::function<Async<Message>(Context, Message, Next)> handler) const {
  return cordis::detail::invoke_owned(
      [*this](std::string alias,
              std::function<Async<Message>(Context, Message, Next)> handler)
          -> Async<Subscription> {
        auto callback = callbacks_->insert_payload(
            [handler = std::move(handler)](uint32_t op, Context ctx,
                                           Payload args) -> Async<Payload> {
              if (op == CORDIS_ABI_CALLBACK_RELEASE) {
                args.control_only();
                co_return Payload();
              }
              if (op != CORDIS_ABI_MIDDLEWARE_CALL)
                throw Refusal("operation", "invalid middleware callback");
              co_return Payload(co_await handler(
                  ctx, args.one(),
                  Next(ctx.raw_, ctx.reactive_,
                       args.control.at("next").get<uint64_t>())));
            });
        try {
          auto id = (co_await detail::reactive_call(raw_, reactive_,
                                                    CORDIS_ABI_MIDDLEWARE_USE,
                                                    0, callback, alias))
                        .second;
          co_return Subscription(raw_, reactive_, id);
        } catch (const Rejected &) {
          callbacks_->erase_managed(callback);
          throw;
        }
      },
      std::move(alias), std::move(handler));
}
inline Async<Dispatch> Context::emit(std::string alias, Message input,
                                     bool serial) const {
  return cordis::detail::invoke_owned(
      [*this](std::string alias, Message input,
              bool serial) -> Async<Dispatch> {
        auto id = (co_await detail::reactive_call(
                       raw_, reactive_, CORDIS_ABI_EVENT_EMIT, 0, 0, alias,
                       Payload(Value{{"serial", serial}}, input)))
                      .second;
        co_return Dispatch(raw_, reactive_, id);
      },
      std::move(alias), std::move(input), serial);
}
inline Async<std::vector<Flow>> Context::parallel(std::string alias,
                                                  Message input) const {
  return cordis::detail::invoke_owned(
      [*this](std::string alias, Message input) -> Async<std::vector<Flow>> {
        co_return co_await (co_await emit(alias, input)).join();
      },
      std::move(alias), std::move(input));
}
inline Async<Flow> Context::serial(std::string alias, Message input) const {
  return cordis::detail::invoke_owned(
      [*this](std::string alias, Message input) -> Async<Flow> {
        auto values = co_await (co_await emit(alias, input, true)).join();
        for (auto &flow : values)
          if (flow.value)
            co_return std::move(flow);
        co_return Continue();
      },
      std::move(alias), std::move(input));
}
inline Async<Message>
Context::run(std::string alias, Message input,
             std::function<Async<Message>(Context, Message)> terminal) const {
  return cordis::detail::invoke_owned(
      [*this](std::string alias, Message input,
              std::function<Async<Message>(Context, Message)> terminal)
          -> Async<Message> {
        auto callback = callbacks_->insert_payload(
            [terminal = std::move(terminal)](uint32_t op, Context ctx,
                                             Payload args) -> Async<Payload> {
              if (op != CORDIS_ABI_TERMINAL_CALL)
                throw Refusal("operation", "invalid terminal callback");
              co_return Payload(co_await terminal(ctx, args.one()));
            });
        try {
          auto result = (co_await detail::reactive_call(
                             raw_, reactive_, CORDIS_ABI_MIDDLEWARE_RUN, 0,
                             callback, alias, Payload(input)))
                            .first.one();
          callbacks_->erase_managed(callback);
          co_return result;
        } catch (const Rejected &) {
          callbacks_->erase_managed(callback);
          throw;
        }
      },
      std::move(alias), std::move(input), std::move(terminal));
}
} // namespace cordis::abi
