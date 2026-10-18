#pragma once
#include "context.hpp"
#include <memory>

namespace cordis {
struct Resource {
  virtual ~Resource() = default;
  virtual Async<Message> invoke(Context ctx, std::string method,
                                Message args) = 0;
  virtual Async<void> close(Context ctx) = 0;
};

struct Service {
  virtual bool typed_dispatch() const { return false; }
  virtual ~Service() = default;
  virtual Async<void> activate(Context) { co_return; }
  virtual Async<Message> invoke(Context ctx, std::string method,
                                Message args) = 0;
  virtual Async<std::shared_ptr<Resource>> acquire(Context, std::string kind,
                                                   Message) {
    // Keep rejection lazy and retain the call's parameters until completion.
    co_return co_await detail::failed<std::shared_ptr<Resource>>(
        Refusal("unknown_resource_kind", kind));
  }
  virtual Async<void> deactivate(Context) { co_return; }
};
} // namespace cordis
