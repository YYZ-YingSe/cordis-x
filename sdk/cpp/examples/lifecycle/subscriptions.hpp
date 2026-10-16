#pragma once
#include "managed.hpp"
#include <iostream>
namespace example::lifecycle {
template <class Server, class Logger>
class Subscriptions final : public Server {
  std::mutex mutex_;
  std::set<std::string> names_, listeners_;
  std::map<std::string, Effect> handles_;

public:
  static std::shared_ptr<Subscriptions> create(cordis::data::Unit) {
    return std::make_shared<Subscriptions>();
  }
  Async<Text> subscribe(Context ctx, Text args) override {
    auto name = args.value();
    {
      std::lock_guard lock(mutex_);
      if (!names_.insert(name).second)
        throw RpcError("duplicate_subscription", name);
    }
    try {
      co_await Logger::write(ctx, "opening:" + name);
      // Register recovery before publishing the local listener. The current
      // admitted call keeps its owner alive through this no-await commit.
      auto handle = co_await ctx.defer(
          "subscription:" + name, [this, name](Context cleanup) -> Async<void> {
            co_await Logger::write(std::move(cleanup), "released:" + name);
            std::lock_guard lock(mutex_);
            listeners_.erase(name);
            handles_.erase(name);
          });
      std::lock_guard lock(mutex_);
      handles_.emplace(name, std::move(handle));
      listeners_.insert(name);
    } catch (...) {
      std::lock_guard lock(mutex_);
      names_.erase(name);
      throw;
    }
    co_return args;
  }
  Async<Unit> unsubscribe(Context, Text args) override {
    auto handle = [&]() -> std::optional<Effect> {
      std::lock_guard lock(mutex_);
      auto found = handles_.find(args.value());
      return found == handles_.end() ? std::nullopt
                                     : std::optional<Effect>(found->second);
    }();
    if (handle)
      co_await handle->dispose();
    co_return Unit{};
  }
  Async<google::protobuf::UInt64Value> publish(Context, Text args) override {
    std::lock_guard lock(mutex_);
    for (const auto &name : listeners_)
      std::cerr << '[' << name << "] " << args.value() << '\n';
    co_return example::lifecycle::count(listeners_.size());
  }
  Async<google::protobuf::UInt64Value> count(Context, Unit) override {
    std::lock_guard lock(mutex_);
    co_return example::lifecycle::count(listeners_.size());
  }
  Async<void> deactivate(Context ctx) override {
    co_await Logger::write(std::move(ctx), "subscriptions.closed");
  }
};
using QuietSubscriptions =
    Subscriptions<::lifecycle::SubscriptionsServer, SilentLog>;
using AuditedSubscriptions =
    Subscriptions<::lifecycle::AuditedSubscriptionsServer, SubscriptionLog>;
using SubscriptionsModel = ::lifecycle::SubscriptionsModel<QuietSubscriptions>;
using AuditedSubscriptionsModel =
    ::lifecycle::AuditedSubscriptionsModel<AuditedSubscriptions>;
} // namespace example::lifecycle
