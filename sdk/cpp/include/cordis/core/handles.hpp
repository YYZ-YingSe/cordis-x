#pragma once
#include "async.hpp"
#include "error.hpp"
#include "message.hpp"
#include "value.hpp"
#include <any>
#include <chrono>
#include <functional>
#include <optional>
#include <vector>

namespace cordis {
class Context;
namespace detail {
struct ResourceAccess;
}

// An owned dependency capability, distinct from the Resource business object.
// Copies do not transfer ownership; every operation uses a current Context.
class ResourceHandle {
  std::any state_;
  explicit ResourceHandle(std::any state) : state_(std::move(state)) {}
  friend class Context;
  friend struct detail::ResourceAccess;

public:
  Async<Message> invoke(Context ctx, std::string method, Message args) const;
  Async<void> dispose(Context ctx) const;
};

namespace detail {
// Adapter construction only. Business code never receives wire IDs or grants.
struct ResourceAccess {
  template <class T> static ResourceHandle make(T state) {
    return ResourceHandle(std::any(std::move(state)));
  }
};
} // namespace detail

// Handles carry adapter operations, not protocol IDs. Destruction never
// disposes a Runtime-owned contribution.
class Effect {
  std::function<Async<void>()> dispose_;

public:
  explicit Effect(std::function<Async<void>()> dispose)
      : dispose_(std::move(dispose)) {}
  Async<void> dispose() const { return detail::invoke_owned(dispose_); }
};
struct Inverse {
  std::string label;
  std::function<Async<void>(Context)> cleanup;
};
struct EffectStep {
  bool done;
  std::optional<Inverse> inverse;
  std::optional<RpcError> error;
  static EffectStep yielded(std::optional<Inverse> inverse = {}) {
    return {false, std::move(inverse), {}};
  }
  static EffectStep complete(std::optional<Inverse> inverse = {}) {
    return {true, std::move(inverse), {}};
  }
  static EffectStep failed(RpcError error,
                           std::optional<Inverse> inverse = {}) {
    return {true, std::move(inverse), std::move(error)};
  }
};
class EffectSequence {
  std::function<Async<std::string>()> ready_;
  std::function<Async<void>()> dispose_;
  std::function<Async<Value>()> snapshot_;

public:
  EffectSequence(std::function<Async<std::string>()> ready,
                 std::function<Async<void>()> dispose,
                 std::function<Async<Value>()> snapshot)
      : ready_(std::move(ready)), dispose_(std::move(dispose)),
        snapshot_(std::move(snapshot)) {}
  Async<std::string> ready() const { return detail::invoke_owned(ready_); }
  Async<void> dispose() const { return detail::invoke_owned(dispose_); }
  Async<Value> snapshot() const { return detail::invoke_owned(snapshot_); }
};
class Scope {
  std::function<Async<void>()> ready_, dispose_;
  std::function<Async<Value>()> snapshot_;
  std::function<Async<Value>(std::function<Async<Value>(Context)>)> run_;

public:
  Scope(std::function<Async<void>()> ready,
        std::function<Async<void>()> dispose,
        std::function<Async<Value>()> snapshot,
        std::function<Async<Value>(std::function<Async<Value>(Context)>)> run)
      : ready_(std::move(ready)), dispose_(std::move(dispose)),
        snapshot_(std::move(snapshot)), run_(std::move(run)) {}
  Async<void> ready() const { return detail::invoke_owned(ready_); }
  Async<void> dispose() const { return detail::invoke_owned(dispose_); }
  Async<Value> snapshot() const { return detail::invoke_owned(snapshot_); }
  Async<Value> run(std::function<Async<Value>(Context)> callback) const {
    return detail::invoke_owned(run_, std::move(callback));
  }
};
class Task {
  std::function<Async<void>()> join_;
  std::function<Async<Value>()> snapshot_;

public:
  Task(std::function<Async<void>()> join,
       std::function<Async<Value>()> snapshot)
      : join_(std::move(join)), snapshot_(std::move(snapshot)) {}
  Async<void> join() const { return detail::invoke_owned(join_); }
  Async<Value> snapshot() const { return detail::invoke_owned(snapshot_); }
};
struct Flow {
  std::optional<Message> value;
};
inline Flow Continue() { return {}; }
inline Flow Break(Message value) { return {std::move(value)}; }
class Dispatch {
  std::function<Async<std::vector<Flow>>()> join_;
  std::function<Async<Value>()> snapshot_;

public:
  Dispatch(std::function<Async<std::vector<Flow>>()> join,
           std::function<Async<Value>()> snapshot)
      : join_(std::move(join)), snapshot_(std::move(snapshot)) {}
  Async<std::vector<Flow>> join() const { return detail::invoke_owned(join_); }
  Async<Value> snapshot() const { return detail::invoke_owned(snapshot_); }
};
class Next {
  std::function<Async<Message>(Message)> run_;

public:
  explicit Next(std::function<Async<Message>(Message)> run)
      : run_(std::move(run)) {}
  Async<Message> run(Message args) const {
    return detail::invoke_owned(run_, args);
  }
};
class Child {
  std::function<Async<Value>()> ready_, settled_, status_;
  std::function<Async<void>()> dispose_;

public:
  Child(std::function<Async<Value>()> ready,
        std::function<Async<Value>()> settled,
        std::function<Async<Value>()> status,
        std::function<Async<void>()> dispose)
      : ready_(std::move(ready)), settled_(std::move(settled)),
        status_(std::move(status)), dispose_(std::move(dispose)) {}
  Async<Value> ready() const { return detail::invoke_owned(ready_); }
  Async<Value> settled() const { return detail::invoke_owned(settled_); }
  Async<Value> status() const { return detail::invoke_owned(status_); }
  Async<void> dispose() const { return detail::invoke_owned(dispose_); }
};
} // namespace cordis
