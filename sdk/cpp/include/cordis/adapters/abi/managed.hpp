#pragma once
#include <cordis/adapters/abi/context.hpp>
#include <optional>

namespace cordis::abi {
inline Async<Value> Context::metadata(std::string name,
                                      std::string revision) const {
  return cordis::detail::invoke_owned(
      [*this](std::string name, std::string revision) -> Async<Value> {
        if (!managed_)
          throw Refusal("unsupported", "Managed extension unavailable");
        detail::Output input;
        detail::Budget budget;
        const Value args = revision;
        auto value = input.build(args, budget);
        co_return (co_await detail::receive(
                       raw_.api, raw_.host, false,
                       [&](uint64_t caller, CordisAbiCompletion completion) {
                         managed_->call(raw_.host, raw_.grant,
                                        CORDIS_ABI_CONTEXT_METADATA, 0, 0,
                                        detail::slice(name), &value, caller,
                                        completion);
                       }))
            .first.control_only();
      },
      std::move(name), std::move(revision));
}
struct Inverse {
  std::string label;
  std::function<Async<void>(Context)> cleanup;
};
struct Step {
  bool done;
  std::optional<Inverse> inverse;
  std::optional<Rejected> error;
  static Step yielded(std::optional<Inverse> inverse = std::nullopt) {
    return {false, std::move(inverse), std::nullopt};
  }
  static Step complete(std::optional<Inverse> inverse = std::nullopt) {
    return {true, std::move(inverse), std::nullopt};
  }
  static Step failed(Rejected error,
                     std::optional<Inverse> inverse = std::nullopt) {
    return {true, std::move(inverse), std::move(error)};
  }
};
namespace detail {
inline Async<HostReply>
managed_call(CordisAbiContext ctx, const CordisAbiManagedHostApi *api,
             uint32_t operation, uint64_t target = 0, uint64_t callback = 0,
             std::string label = {}, Value args = nullptr) {
  if (!api)
    throw Refusal("unsupported", "Managed extension unavailable");
  Output input;
  Budget budget;
  auto value = input.build(args, budget);
  co_return (co_await detail::receive(
      ctx.api, ctx.host,
      operation == CORDIS_ABI_SEQUENCE_REGISTER ||
          operation == CORDIS_ABI_SCOPE_CREATE ||
          operation == CORDIS_ABI_TASK_SPAWN,
      [&](uint64_t caller, CordisAbiCompletion completion) {
        api->call(ctx.host, ctx.grant, operation, target, callback,
                  slice(label), &value, caller, completion);
      }));
}
struct ManagedHandle {
  CordisAbiContext context{};
  const CordisAbiManagedHostApi *api = nullptr;
  uint64_t id = 0;
  std::shared_ptr<Callbacks> callbacks;
  ManagedHandle() = default;
  ManagedHandle(CordisAbiContext context, const CordisAbiManagedHostApi *api,
                uint64_t id, std::shared_ptr<Callbacks> callbacks)
      : context(context), api(api), id(id), callbacks(std::move(callbacks)) {
    this->context.grant =
        0; // Independent handles never retain invocation authority.
  }
  Async<Value> call(uint32_t operation, uint64_t callback = 0) const {
    return cordis::detail::invoke_owned(
        [*this](uint32_t operation, uint64_t callback) -> Async<Value> {
          co_return (
              co_await managed_call(context, api, operation, id, callback))
              .first.control_only();
        },
        std::move(operation), std::move(callback));
  }
};
struct Producer {
  std::function<Async<Step>(Context)> next;
  std::unordered_map<uint64_t, Inverse> inverses;
  // The native driver serializes steps and drains production before recovery.
  Async<Value> call(uint32_t operation, Context ctx, Value args) {
    if (operation == CORDIS_ABI_SEQUENCE_RELEASE) {
      if (!inverses.empty())
        throw Refusal("owned", "sequence inverses remain");
      co_return nullptr;
    }
    auto step = args.get<uint64_t>();
    if (operation == CORDIS_ABI_SEQUENCE_UNDO) {
      auto found = inverses.find(step);
      if (found == inverses.end())
        throw Refusal("closed", "unknown sequence inverse");
      (co_await found->second.cleanup(ctx));
      inverses.erase(found);
      co_return nullptr;
    }
    if (operation != CORDIS_ABI_SEQUENCE_NEXT)
      throw Refusal("operation", "invalid sequence callback");
    Step result = Step::complete();
    try {
      result = (co_await next(ctx));
    } catch (const Rejected &error) {
      result = Step::failed(error);
    }
    Value label = nullptr;
    if (result.inverse) {
      label = result.inverse->label;
      if (!inverses.emplace(step, std::move(*result.inverse)).second)
        throw std::runtime_error("duplicate sequence step");
    }
    Value error = nullptr;
    if (result.error)
      error = {{"code", result.error->code}, {"message", result.error->what()}};
    co_return Value{
        {"done", result.done}, {"inverse", label}, {"error", error}};
  }
};
} // namespace detail
class Sequence : private detail::ManagedHandle {
  friend class Context;
  using ManagedHandle::ManagedHandle;

public:
  Sequence() = default;
  Async<Value> ready() const {
    return cordis::detail::invoke_owned([*this]() -> Async<Value> {
      co_return (co_await call(CORDIS_ABI_SEQUENCE_READY));
    });
  }
  Async<void> dispose() const {
    return cordis::detail::invoke_owned([*this]() -> Async<void> {
      (co_await call(CORDIS_ABI_SEQUENCE_DISPOSE));
      co_return;
    });
  }
  Async<Value> snapshot() const {
    return cordis::detail::invoke_owned([*this]() -> Async<Value> {
      co_return (co_await call(CORDIS_ABI_SEQUENCE_STATUS));
    });
  }
};
class Task : private detail::ManagedHandle {
  friend class Context;
  using ManagedHandle::ManagedHandle;

public:
  Task() = default;
  Async<void> join() const {
    return cordis::detail::invoke_owned([*this]() -> Async<void> {
      (co_await call(CORDIS_ABI_TASK_JOIN));
      co_return;
    });
  }
  Async<Value> snapshot() const {
    return cordis::detail::invoke_owned([*this]() -> Async<Value> {
      co_return (co_await call(CORDIS_ABI_TASK_STATUS));
    });
  }
};
class Scope : private detail::ManagedHandle {
  friend class Context;
  using ManagedHandle::ManagedHandle;

public:
  Scope() = default;
  Async<void> ready() const {
    return cordis::detail::invoke_owned([*this]() -> Async<void> {
      (co_await call(CORDIS_ABI_SCOPE_READY));
      co_return;
    });
  }
  Async<void> dispose() const {
    return cordis::detail::invoke_owned([*this]() -> Async<void> {
      (co_await call(CORDIS_ABI_SCOPE_DISPOSE));
      co_return;
    });
  }
  Async<Value> snapshot() const {
    return cordis::detail::invoke_owned([*this]() -> Async<Value> {
      co_return (co_await call(CORDIS_ABI_SCOPE_STATUS));
    });
  }
  Async<Value> run(std::function<Async<Value>(Context)> callback) const {
    return cordis::detail::invoke_owned(
        [*this](std::function<Async<Value>(Context)> callback) -> Async<Value> {
          if (!callbacks)
            throw Refusal("closed", "empty scope");
          auto id = callbacks->insert_managed(
              [callback = std::move(callback)](uint32_t operation, Context ctx,
                                               Value) -> Async<Value> {
                if (operation != CORDIS_ABI_SCOPE_CALL)
                  throw Refusal("operation", "invalid scope callback");
                co_return (co_await callback(ctx));
              });
          try {
            co_return (co_await call(CORDIS_ABI_SCOPE_RUN, id));
          } catch (const Rejected &) {
            callbacks->erase_managed(id);
            throw;
          }
          // Unknown outcomes retain the callback and native cleanup
          // responsibility.
        },
        std::move(callback));
  }
};
inline Async<Sequence>
Context::effect(std::string label,
                std::function<Async<Step>(Context)> next) const {
  return cordis::detail::invoke_owned(
      [*this](std::string label,
              std::function<Async<Step>(Context)> next) -> Async<Sequence> {
        auto producer = std::make_shared<detail::Producer>();
        producer->next = std::move(next);
        auto callback = callbacks_->insert_managed(
            [producer](uint32_t op, Context ctx, Value args) -> Async<Value> {
              co_return (co_await producer->call(op, ctx, args));
            });
        try {
          auto id = (co_await detail::managed_call(raw_, managed_,
                                                   CORDIS_ABI_SEQUENCE_REGISTER,
                                                   0, callback, label))
                        .second;
          co_return Sequence(raw_, managed_, id, callbacks_);
        } catch (const Rejected &) {
          callbacks_->erase_managed(callback);
          throw;
        }
      },
      std::move(label), std::move(next));
}
inline Async<Scope> Context::scope(std::string label) const {
  return cordis::detail::invoke_owned(
      [*this](std::string label) -> Async<Scope> {
        auto id = (co_await detail::managed_call(
                       raw_, managed_, CORDIS_ABI_SCOPE_CREATE, 0, 0, label))
                      .second;
        co_return Scope(raw_, managed_, id, callbacks_);
      },
      std::move(label));
}
inline Async<Task>
Context::spawn(std::string label,
               std::function<Async<void>(Context)> run) const {
  return cordis::detail::invoke_owned(
      [*this](std::string label,
              std::function<Async<void>(Context)> run) -> Async<Task> {
        auto callback = callbacks_->insert_managed(
            [run = std::move(run)](uint32_t op, Context ctx,
                                   Value) -> Async<Value> {
              if (op != CORDIS_ABI_TASK_CALL)
                throw Refusal("operation", "invalid task callback");
              (co_await run(ctx));
              co_return nullptr;
            });
        try {
          auto id =
              (co_await detail::managed_call(
                   raw_, managed_, CORDIS_ABI_TASK_SPAWN, 0, callback, label))
                  .second;
          co_return Task(raw_, managed_, id, callbacks_);
        } catch (const Rejected &) {
          callbacks_->erase_managed(callback);
          throw;
        }
      },
      std::move(label), std::move(run));
}
inline Async<void> Context::cancelled() const {
  return cordis::detail::invoke_owned([*this]() -> Async<void> {
    (co_await detail::managed_call(raw_, managed_, CORDIS_ABI_TASK_CANCELLED));

    co_return;
  });
}
inline Async<bool>
Context::cancelled_for(std::chrono::milliseconds duration) const {
  return cordis::detail::invoke_owned(
      [*this](std::chrono::milliseconds duration) -> Async<bool> {
        co_return (co_await detail::managed_call(raw_, managed_,
                                                 CORDIS_ABI_TASK_CANCELLED, 0,
                                                 0, {}, duration.count()))
            .first.control_only()
            .get<bool>();
      },
      std::move(duration));
}
} // namespace cordis::abi
