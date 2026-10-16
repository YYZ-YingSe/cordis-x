#pragma once
#include "managed.hpp"
namespace example::lifecycle {
class Connections final : public ::lifecycle::ConnectionsServer {
  using Dependencies = ::lifecycle::ConnectionsDependencies;
  struct Connection {
    Scope scope;
    std::mutex mutex;
    std::map<std::string, std::vector<char>> resources;
    explicit Connection(Scope value) : scope(std::move(value)) {}
    void add(const std::string &key, std::size_t size = 1) {
      std::lock_guard lock(mutex);
      resources.emplace(key, std::vector<char>(size));
    }
    void remove(const std::string &key) {
      std::lock_guard lock(mutex);
      resources.erase(key);
    }
  };
  ::lifecycle::ScopeConfig config_;
  std::mutex mutex_;
  std::set<std::string> names_;
  std::map<std::string, std::shared_ptr<Connection>> connections_;
  std::shared_ptr<Connection> find(const std::string &name) {
    std::lock_guard lock(mutex_);
    auto found = connections_.find(name);
    if (found == connections_.end())
      throw RpcError("unknown_connection", name);
    return found->second;
  }

public:
  explicit Connections(::lifecycle::ScopeConfig config)
      : config_(std::move(config)) {}
  static std::shared_ptr<Connections> create(::lifecycle::ScopeConfig config) {
    return std::make_shared<Connections>(std::move(config));
  }
  Async<void> activate(Context ctx) override {
    if (!config_.fail_activate())
      co_return;
    auto scope = co_await ctx.scope("startup");
    co_await scope.run([](Context child) -> Async<Value> {
      co_await child.defer("startup", [](Context cleanup) {
        return log<Dependencies>(std::move(cleanup), "startup.cleanup");
      });
      co_return Value::object();
    });
    throw RpcError("startup_failed", "after child registration");
  }
  Async<Unit> open(Context ctx, Text args) override {
    const auto name = args.value();
    {
      std::lock_guard lock(mutex_);
      if (!names_.insert(name).second)
        throw RpcError("duplicate_connection", name);
    }
    std::shared_ptr<Connection> connection;
    try {
      connection = std::make_shared<Connection>(
          co_await ctx.scope("connection:" + name));
      std::lock_guard lock(mutex_);
      connections_.emplace(name, connection);
    } catch (...) {
      std::lock_guard lock(mutex_);
      names_.erase(name);
      throw;
    }
    // Once the scope exists, a failed initializer must retain its handle and
    // original name: its partial resources still belong to Runtime recovery.
    co_await connection->scope.run([connection,
                                    name](Context child) -> Async<Value> {
      co_await log<Dependencies>(child, name + "/buffer.open");
      connection->add("buffer", 1024);
      co_await child.defer("buffer",
                           [connection, name](Context cleanup) -> Async<void> {
                             co_await log<Dependencies>(std::move(cleanup),
                                                        name + "/buffer.close");
                             connection->remove("buffer");
                           });
      auto subscriptions = co_await child.effect(
          "subscriptions",
          [connection, name, n = 0](Context step) mutable -> Async<EffectStep> {
            const auto current = ++n;
            co_await log<Dependencies>(
                step, name + "/subscribe:" + std::to_string(current));
            const auto key = "subscription:" + std::to_string(current);
            connection->add(key);
            Inverse inverse{
                key,
                [connection, name, key,
                 current](Context cleanup) -> Async<void> {
                  co_await log<Dependencies>(
                      std::move(cleanup),
                      name + "/unsubscribe:" + std::to_string(current));
                  connection->remove(key);
                }};
            co_return current == 2 ? EffectStep::complete(std::move(inverse))
                                   : EffectStep::yielded(std::move(inverse));
          });
      co_await subscriptions.ready();
      auto watcher = co_await child.scope("watcher");
      co_await watcher.run([connection, name](Context nested) -> Async<Value> {
        connection->add("watcher");
        co_await nested.defer(
            "watcher", [connection, name](Context cleanup) -> Async<void> {
              co_await log<Dependencies>(std::move(cleanup),
                                         name + "/watcher.close");
              connection->remove("watcher");
            });
        co_return Value::object();
      });
      co_return Value::object();
    });
    co_return Unit{};
  }
  Async<google::protobuf::UInt64Value> count(Context, Text args) override {
    auto value = find(args.value());
    std::lock_guard lock(value->mutex);
    co_return example::lifecycle::count(value->resources.size());
  }
  Async<::lifecycle::ScopeStatus> status(Context, Text args) override {
    co_return protobuf::decode<::lifecycle::ScopeStatus>(
        co_await find(args.value())->scope.snapshot());
  }
  Async<Unit> release(Context, Text args) override {
    co_await find(args.value())->scope.dispose();
    co_return Unit{};
  }
  Async<Text> run(Context, Text args) override {
    const auto name = args.value();
    auto value =
        co_await find(name)->scope.run([name](Context child) -> Async<Value> {
          co_await log<Dependencies>(std::move(child), name + "/run");
          Text result;
          result.set_value("ok");
          co_return protobuf::encode(result);
        });
    co_return protobuf::decode<Text>(value);
  }
  Async<Unit> contribute(Context, Text args) override {
    const auto name = args.value();
    auto connection = find(name);
    co_await connection->scope.run([name,
                                    connection](Context child) -> Async<Value> {
      co_await log<Dependencies>(child, name + "/late.enter");
      connection->add("late");
      co_await child.defer("late",
                           [name, connection](Context cleanup) -> Async<void> {
                             co_await log<Dependencies>(std::move(cleanup),
                                                        name + "/late.close");
                             connection->remove("late");
                           });
      co_return Value::object();
    });
    co_return Unit{};
  }
  Async<void> deactivate(Context ctx) override {
    co_await log<Dependencies>(std::move(ctx), "connections.closed");
  }
};
using ConnectionsModel = ::lifecycle::ConnectionsModel<Connections>;
} // namespace example::lifecycle
