#pragma once
#include "error.hpp"
#include "handles.hpp"
#include "value.hpp"
#include <functional>

namespace cordis {
class Context;

// Callback authority comes from the selected adapter. Copies do not extend its
// lifetime. No Session token, C function table, codec or socket enters this
// API.
class Context {
public:
  struct Operations {
    Validation validation = Validation::Checked;
    std::function<Async<Message>(std::string, std::string, Message)> invoke;
    std::function<Async<Value>(std::string, std::string)> metadata;
    std::function<Async<Effect>(std::string,
                                std::function<Async<void>(Context)>)>
        defer;
    std::function<Async<EffectSequence>(
        std::string, std::function<Async<EffectStep>(Context)>)>
        effect;
    std::function<Async<Scope>(std::string)> scope;
    std::function<Async<Task>(std::string, std::function<Async<void>(Context)>)>
        spawn;
    std::function<Async<bool>(std::chrono::milliseconds)> cancelled_for;
    std::function<Async<void>()> cancelled;
    std::function<Async<Effect>(std::string,
                                std::function<Async<Flow>(Context, Message)>)>
        on;
    std::function<Async<Dispatch>(std::string, Message)> emit;
    std::function<Async<Flow>(std::string, Message)> serial;
    std::function<Async<Effect>(
        std::string, std::function<Async<Message>(Context, Message, Next)>)>
        middleware;
    std::function<Async<Message>(
        std::string, Message, std::function<Async<Message>(Context, Message)>)>
        run;
    std::function<Async<Child>(std::string, Message)> install;
    std::function<Async<void>(std::string, Message)> provide;
    std::function<Async<ResourceHandle>(std::string, std::string, Message)>
        acquire;
    std::function<Async<Message>(std::any, std::string, Message)>
        resource_invoke;
    std::function<Async<void>(std::any)> resource_dispose;
  };

private:
  std::shared_ptr<const Operations> operations_;
  friend class ResourceHandle;
  template <class F> static const F &require(const F &f) {
    if (!f)
      throw Refusal("unsupported", "operation unavailable in this adapter");
    return f;
  }

public:
  explicit Context(Operations operations)
      : operations_(std::make_shared<Operations>(std::move(operations))) {
    if (!operations_->invoke || !operations_->metadata || !operations_->defer)
      throw std::invalid_argument("incomplete Context adapter");
  }
  Validation validation() const { return operations_->validation; }
  Async<Message> invoke(std::string dependency, std::string method,
                        Message args) const {
    return detail::invoke_owned(operations_->invoke, dependency, method, args);
  }
  Async<Value> metadata(std::string name, std::string revision) const {
    return detail::invoke_owned(operations_->metadata, name, revision);
  }
  Async<ResourceHandle> acquire(std::string dependency, std::string kind,
                                Message args) const {
    return detail::invoke_owned(require(operations_->acquire), dependency, kind,
                                args);
  }
  Async<Effect> defer(std::string label,
                      std::function<Async<void>(Context)> cleanup) const {
    return detail::invoke_owned(operations_->defer, label, std::move(cleanup));
  }
  Async<EffectSequence>
  effect(std::string label,
         std::function<Async<EffectStep>(Context)> next) const {
    return detail::invoke_owned(require(operations_->effect), label,
                                std::move(next));
  }
  Async<Scope> scope(std::string label) const {
    return detail::invoke_owned(require(operations_->scope), label);
  }
  Async<Task> spawn(std::string label,
                    std::function<Async<void>(Context)> task) const {
    return detail::invoke_owned(require(operations_->spawn), label,
                                std::move(task));
  }
  Async<bool> cancelled_for(std::chrono::milliseconds duration) const {
    return detail::invoke_owned(require(operations_->cancelled_for), duration);
  }
  Async<void> cancelled() const {
    return detail::invoke_owned(require(operations_->cancelled));
  }
  Async<Effect> on(std::string event,
                   std::function<Async<Flow>(Context, Message)> handler) const {
    return detail::invoke_owned(require(operations_->on), event,
                                std::move(handler));
  }
  Async<Dispatch> emit(std::string event, Message args) const {
    return detail::invoke_owned(require(operations_->emit), event, args);
  }
  Async<std::vector<Flow>> parallel(std::string event, Message args) const {
    return detail::invoke_owned(
        [ctx = *this](std::string event,
                      Message args) -> Async<std::vector<Flow>> {
          auto dispatch = co_await ctx.emit(std::move(event), std::move(args));
          co_return co_await dispatch.join();
        },
        std::move(event), std::move(args));
  }
  Async<Flow> serial(std::string event, Message args) const {
    return detail::invoke_owned(require(operations_->serial), event, args);
  }
  Async<Effect> middleware(
      std::string name,
      std::function<Async<Message>(Context, Message, Next)> handler) const {
    return detail::invoke_owned(require(operations_->middleware), name,
                                std::move(handler));
  }
  Async<Message>
  run(std::string name, Message args,
      std::function<Async<Message>(Context, Message)> terminal = {}) const {
    if (!terminal)
      terminal = [](Context, Message value) -> Async<Message> {
        co_return value;
      };
    return detail::invoke_owned(require(operations_->run), name, args,
                                std::move(terminal));
  }
  Async<Child> install(std::string factory, Message config) const {
    return detail::invoke_owned(require(operations_->install), factory, config);
  }
  Async<void> provide(std::string service, Message value) const {
    return detail::invoke_owned(require(operations_->provide),
                                std::move(service), std::move(value));
  }
};
inline Async<Message> ResourceHandle::invoke(Context ctx, std::string method,
                                             Message args) const {
  return detail::invoke_owned(
      Context::require(ctx.operations_->resource_invoke), state_,
      std::move(method), std::move(args));
}
inline Async<void> ResourceHandle::dispose(Context ctx) const {
  return detail::invoke_owned(
      Context::require(ctx.operations_->resource_dispose), state_);
}
} // namespace cordis
