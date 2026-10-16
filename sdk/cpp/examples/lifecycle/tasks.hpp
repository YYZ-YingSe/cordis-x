#pragma once
#include "managed.hpp"
namespace example::lifecycle {
class Heartbeats final : public ::lifecycle::HeartbeatsServer {
  using Dependencies = ::lifecycle::HeartbeatsDependencies;
  struct Connection {
    std::optional<Scope> scope;
    Task task;
  };
  ::lifecycle::TaskConfig config_;
  std::mutex mutex_;
  std::set<std::string> names_;
  std::map<std::string, Connection> connections_;
  Connection find(const std::string &name) {
    std::lock_guard lock(mutex_);
    auto it = connections_.find(name);
    if (it == connections_.end())
      throw RpcError("unknown_connection", name);
    return it->second;
  }
  Async<void> open_connection(Context ctx, std::string name) {
    {
      std::lock_guard lock(mutex_);
      if (!names_.insert(name).second)
        throw RpcError("duplicate_connection", name);
    }
    try {
      std::optional<Scope> scope;
      if (!config_.direct())
        scope = co_await ctx.scope("connection:" + name);
      auto start = [this, name, scope,
                    mode = config_.mode()](Context child) -> Async<Value> {
        co_await child.defer("connection", [name](Context cleanup) {
          return log<Dependencies>(std::move(cleanup),
                                   name + "/connection.close");
        });
        auto task = co_await child.spawn(
            "heartbeat", [name, mode](Context worker) -> Async<void> {
              co_await log<Dependencies>(worker, name + "/started");
              co_await worker.defer(
                  "task contribution", [name](Context cleanup) {
                    return log<Dependencies>(std::move(cleanup),
                                             name + "/task.cleanup");
                  });
              if (mode == ::lifecycle::TaskConfig::FAIL)
                throw RpcError("task_failed", "heartbeat failed");
              if (mode == ::lifecycle::TaskConfig::FINITE)
                co_return;
              while (!(
                  co_await worker.cancelled_for(std::chrono::milliseconds(50))))
                co_await log<Dependencies>(worker, name + "/heartbeat");
              co_await log<Dependencies>(worker, name + "/stopped");
              co_await worker.defer(
                  "last contribution", [name](Context cleanup) {
                    return log<Dependencies>(std::move(cleanup),
                                             name + "/last.cleanup");
                  });
            });
        std::lock_guard lock(mutex_);
        connections_.emplace(name, Connection{scope, std::move(task)});
        co_return Value::object();
      };
      if (scope)
        co_await scope->run(start);
      else
        co_await start(ctx);
    } catch (...) {
      // Failed registration does not erase Runtime-owned partial effects.
      std::lock_guard lock(mutex_);
      names_.erase(name);
      throw;
    }
  }

public:
  explicit Heartbeats(::lifecycle::TaskConfig config)
      : config_(std::move(config)) {}
  static std::shared_ptr<Heartbeats> create(::lifecycle::TaskConfig config) {
    return std::make_shared<Heartbeats>(std::move(config));
  }
  Async<void> activate(Context ctx) override {
    if (config_.fail_activate()) {
      co_await open_connection(std::move(ctx), "startup");
      throw RpcError("startup_failed", "after task registration");
    }
  }
  Async<Unit> open(Context ctx, Text args) override {
    co_await open_connection(std::move(ctx), args.value());
    co_return Unit{};
  }
  Async<Unit> join(Context, Text args) override {
    co_await find(args.value()).task.join();
    co_return Unit{};
  }
  Async<::lifecycle::TaskStatus> status(Context, Text args) override {
    co_return task_status(co_await find(args.value()).task.snapshot());
  }
  Async<Unit> release(Context, Text args) override {
    auto value = find(args.value());
    if (!value.scope)
      throw RpcError("direct_task", "direct tasks stop with their owner");
    co_await value.scope->dispose();
    co_return Unit{};
  }
  Async<void> deactivate(Context ctx) override {
    co_await log<Dependencies>(std::move(ctx), "tasks.closed");
  }
};
using HeartbeatsModel = ::lifecycle::HeartbeatsModel<Heartbeats>;
} // namespace example::lifecycle
