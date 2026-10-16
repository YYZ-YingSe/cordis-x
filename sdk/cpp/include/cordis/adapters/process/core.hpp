#pragma once
#include "host.hpp"
#include <cordis/adapters/context.hpp>

namespace cordis::adapters {
template <> struct Advanced<process::Context> {
  static void fill(Context::Operations &ops, const process::Context &backend) {
    resources(ops, backend);
    ops.effect = [backend](std::string label,
                           std::function<Async<EffectStep>(Context)> next)
        -> Async<EffectSequence> {
      auto sequence = co_await backend.effect(
          std::move(label),
          [next = std::move(next)](auto ctx) -> Async<process::EffectStep> {
            auto step = co_await next(context(std::move(ctx)));
            std::optional<process::Inverse> inverse;
            if (step.inverse) {
              inverse = process::Inverse{
                  step.inverse->label,
                  [cleanup =
                       std::move(step.inverse->cleanup)](auto cleanup_ctx) {
                    return cleanup(context(std::move(cleanup_ctx)));
                  }};
            }
            co_return process::EffectStep{step.done, std::move(inverse),
                                          std::move(step.error)};
          });
      co_return EffectSequence([sequence] { return sequence.ready(); },
                               [sequence] { return sequence.dispose(); },
                               [sequence] { return sequence.snapshot(); });
    };
    ops.scope = [backend](std::string label) -> Async<Scope> {
      co_return scope(co_await backend.scope(std::move(label)));
    };
    ops.spawn =
        [backend](std::string label,
                  std::function<Async<void>(Context)> run) -> Async<Task> {
      co_return task(co_await backend.spawn(
          std::move(label), [run = std::move(run)](auto ctx) {
            return run(context(std::move(ctx)));
          }));
    };
    ops.cancelled_for = [backend](std::chrono::milliseconds duration) {
      return backend.cancelled_for(duration);
    };
    ops.cancelled = [backend] { return backend.cancelled(); };
    ops.on = [backend](std::string event,
                       std::function<Async<Flow>(Context, Message)> callback)
        -> Async<Effect> {
      auto handle = co_await backend.on(
          std::move(event),
          [callback = std::move(callback)](Message args,
                                           auto ctx) -> Async<Flow> {
            co_return (
                co_await callback(context(std::move(ctx)), std::move(args)));
          });
      co_return Effect([handle] { return handle.dispose(); });
    };
    ops.emit = [backend](std::string event, Message args) -> Async<Dispatch> {
      co_return dispatch(
          co_await backend.emit(std::move(event), std::move(args)));
    };
    ops.serial = [backend](std::string event, Message args) -> Async<Flow> {
      co_return co_await backend.serial(std::move(event), std::move(args));
    };
    ops.middleware =
        [backend](
            std::string name,
            std::function<Async<Message>(Context, Message, Next)> callback)
        -> Async<Effect> {
      auto handle = co_await backend.middleware(
          std::move(name),
          [callback = std::move(callback)](Message args, auto ctx, auto next) {
            return callback(context(std::move(ctx)), std::move(args),
                            Next([next](Message value) {
                              return next.run(std::move(value));
                            }));
          });
      co_return Effect([handle] { return handle.dispose(); });
    };
    ops.run =
        [backend](std::string name, Message args,
                  std::function<Async<Message>(Context, Message)> terminal) {
          return backend.waterfall(
              std::move(name), std::move(args),
              [terminal = std::move(terminal)](Message value, auto ctx) {
                return terminal(context(std::move(ctx)), std::move(value));
              });
        };
    ops.install = [backend](std::string factory,
                            Message config) -> Async<Child> {
      co_return child(
          co_await backend.install(std::move(factory), std::move(config)));
    };
    ops.provide = [backend](std::string name, Message value) {
      return backend.provide(std::move(name), std::move(value));
    };
  }
};
} // namespace cordis::adapters
