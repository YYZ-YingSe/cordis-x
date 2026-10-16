#pragma once
#include <cordis/core/context.hpp>

namespace cordis::adapters {
template <class Backend>
void resources(Context::Operations &ops, const Backend &backend) {
  using Handle = typename Backend::ResourceHandle;
  ops.acquire = [backend](std::string dependency, std::string kind,
                          Message args) -> Async<ResourceHandle> {
    co_return detail::ResourceAccess::make(co_await backend.acquire(
        std::move(dependency), std::move(kind), std::move(args)));
  };
  ops.resource_invoke = [backend](std::any state, std::string method,
                                  Message args) {
    const auto *handle = std::any_cast<Handle>(&state);
    if (!handle)
      throw Refusal("invalid_handle", "resource belongs to another adapter");
    return backend.resource_invoke(*handle, std::move(method), std::move(args));
  };
  ops.resource_dispose = [backend](std::any state) {
    const auto *handle = std::any_cast<Handle>(&state);
    if (!handle)
      throw Refusal("invalid_handle", "resource belongs to another adapter");
    return backend.resource_dispose(*handle);
  };
}
template <class Backend> struct Advanced {
  static void fill(Context::Operations &, const Backend &) {}
};
// Adapter-only bridge. Callback expiry and ownership stay with the backend.
template <class Backend> Context context(Backend backend) {
  Context::Operations operations;
  operations.validation = backend.validation();
  operations.invoke = [backend](std::string dependency, std::string method,
                                Message args) {
    return backend.invoke(std::move(dependency), std::move(method),
                          std::move(args));
  };
  operations.metadata = [backend](std::string name, std::string revision) {
    return backend.metadata(std::move(name), std::move(revision));
  };
  operations.defer =
      [backend](std::string label,
                std::function<Async<void>(Context)> cleanup) -> Async<Effect> {
    auto effect = co_await backend.defer(
        std::move(label), [cleanup = std::move(cleanup)](auto ctx) {
          return cleanup(context(std::move(ctx)));
        });
    co_return Effect([effect] { return effect.dispose(); });
  };
  Advanced<Backend>::fill(operations, backend);
  return Context(std::move(operations));
}

template <class Backend> Scope scope(Backend backend) {
  return Scope([backend] { return backend.ready(); },
               [backend] { return backend.dispose(); },
               [backend] { return backend.snapshot(); },
               [backend](std::function<Async<Value>(Context)> run) {
                 return backend.run([run = std::move(run)](auto ctx) {
                   return run(context(std::move(ctx)));
                 });
               });
}
template <class Backend> Task task(Backend backend) {
  return Task([backend] { return backend.join(); },
              [backend] { return backend.snapshot(); });
}
template <class Backend> Dispatch dispatch(Backend backend) {
  return Dispatch([backend] { return backend.join(); },
                  [backend] { return backend.snapshot(); });
}
template <class Backend> Child child(Backend backend) {
  return Child([backend] { return backend.ready(); },
               [backend] { return backend.settled(); },
               [backend] { return backend.status(); },
               [backend] { return backend.dispose(); });
}
} // namespace cordis::adapters
