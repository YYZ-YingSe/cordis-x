#pragma once
#include "common.hpp"

namespace example::lifecycle {
using EventFlow = typed::Flow<::lifecycle::EventResult>;
inline ::lifecycle::EventDecision decision(EventFlow flow) {
  ::lifecycle::EventDecision result;
  if (flow.value)
    *result.mutable_stopped() = std::move(*flow.value);
  else
    result.mutable_continued();
  return result;
}
inline ::lifecycle::EventDecisions decisions(std::vector<EventFlow> flows) {
  ::lifecycle::EventDecisions result;
  for (auto &flow : flows)
    *result.add_values() = decision(std::move(flow));
  return result;
}
// A typed projection of the existing runtime diagnostic, not a new dispatch
// state machine. All readiness, cancellation and error retention stay native.
inline ::lifecycle::DispatchStatus dispatch_status(const Value &snapshot) {
  ::lifecycle::DispatchStatus result;
  const auto &outcome = snapshot.at("outcome");
  if (outcome.is_null())
    result.mutable_pending();
  else if (outcome.at("kind") == "success") {
    result.mutable_succeeded();
  } else if (outcome.at("kind") == "failure") {
    *result.mutable_failed() = protobuf::decode<::lifecycle::DispatchFailures>(
        Value{{"errors", outcome.at("errors")}});
  } else
    throw std::runtime_error("invalid dispatch outcome");
  return result;
}
class Events final : public ::lifecycle::EventsServer {
  using Controls = ::lifecycle::EventsControls;
  using Dependencies = ::lifecycle::EventsDependencies;
  using Dispatch = typed::TypedDispatch<::lifecycle::EventResult>;
  ::lifecycle::ListenerConfig config_;
  std::mutex mutex_;
  std::set<std::string> ids_;
  std::map<std::string, Effect> subscriptions_;
  std::optional<Dispatch> dispatch_;
  std::uint64_t calls_ = 0;
  Dispatch dispatch() {
    std::lock_guard lock(mutex_);
    if (!dispatch_)
      throw RpcError("no_dispatch", "no dispatch started");
    return *dispatch_;
  }
  Async<void> add_listener(Context ctx, ::lifecycle::ListenerSpec args) {
    const auto id = args.id();
    {
      std::lock_guard lock(mutex_);
      if (!ids_.insert(id).second)
        throw RpcError("duplicate_listener", id);
    }
    try {
      auto subscription = co_await Controls::tick().on(
          ctx,
          [this, id, behavior = args.behavior()](
              Context listener,
              ::lifecycle::TickValue value) -> Async<EventFlow> {
            {
              std::lock_guard lock(mutex_);
              if (calls_ == UINT64_MAX)
                throw RpcError("overflow", "event count exhausted");
              ++calls_;
            }
            co_await log<Dependencies>(listener, id + ".enter");
            co_await listener.defer(
                id + ".contribution", [id](Context cleanup) {
                  return log<Dependencies>(std::move(cleanup), id + ".cleanup");
                });
            if (behavior == ::lifecycle::ListenerSpec::NESTED &&
                !value.nested()) {
              value.set_nested(true);
              ::lifecycle::EventResult result;
              *result.mutable_nested() =
                  decisions(co_await Controls::tick().parallel(
                      listener, std::move(value)));
              co_return typed::Break(std::move(result));
            }
            if (behavior == ::lifecycle::ListenerSpec::FAIL)
              throw RpcError("closed", id + " failed");
            if (behavior == ::lifecycle::ListenerSpec::BREAK) {
              ::lifecycle::EventResult result;
              result.set_value(value.value());
              co_return typed::Break(std::move(result));
            }
            co_return typed::Continue<::lifecycle::EventResult>();
          });
      std::lock_guard lock(mutex_);
      subscriptions_.emplace(id, std::move(subscription));
    } catch (...) {
      std::lock_guard lock(mutex_);
      ids_.erase(id);
      throw;
    }
  }

public:
  explicit Events(::lifecycle::ListenerConfig config)
      : config_(std::move(config)) {}
  static std::shared_ptr<Events> create(::lifecycle::ListenerConfig config) {
    return std::make_shared<Events>(std::move(config));
  }
  Async<void> activate(Context ctx) override {
    if (config_.listen() || config_.fail_activate()) {
      ::lifecycle::ListenerSpec args;
      args.set_id("subscriber");
      args.set_behavior(::lifecycle::ListenerSpec::CONTINUE);
      co_await add_listener(ctx, std::move(args));
    }
    if (config_.fail_activate())
      throw RpcError("startup_failed", "after event registration");
  }
  Async<Unit> listen(Context ctx, ::lifecycle::ListenerSpec args) override {
    co_await add_listener(std::move(ctx), std::move(args));
    co_return Unit{};
  }
  Async<Unit> unsubscribe(Context, Text args) override {
    auto subscription = [&] {
      std::lock_guard lock(mutex_);
      auto found = subscriptions_.find(args.value());
      if (found == subscriptions_.end())
        throw RpcError("unknown_listener", args.value());
      return found->second;
    }();
    co_await subscription.dispose();
    co_return Unit{};
  }
  Async<::lifecycle::EventDecisions>
  parallel(Context ctx, ::lifecycle::TickValue args) override {
    co_return decisions(
        co_await Controls::tick().parallel(std::move(ctx), std::move(args)));
  }
  Async<::lifecycle::EventDecision>
  serial(Context ctx, ::lifecycle::TickValue args) override {
    co_return decision(
        co_await Controls::tick().serial(std::move(ctx), std::move(args)));
  }
  Async<Unit> emit(Context ctx, ::lifecycle::TickValue args) override {
    auto value =
        co_await Controls::tick().emit(std::move(ctx), std::move(args));
    std::lock_guard lock(mutex_);
    dispatch_ = std::move(value);
    co_return Unit{};
  }
  Async<::lifecycle::EventDecisions> join(Context, Unit) override {
    co_return decisions(co_await dispatch().join());
  }
  Async<::lifecycle::DispatchStatus> status(Context, Unit) override {
    auto current = dispatch();
    auto result = dispatch_status(co_await current.snapshot());
    if (result.has_succeeded())
      *result.mutable_succeeded() = decisions(co_await current.join());
    co_return result;
  }
  Async<google::protobuf::UInt64Value> count(Context, Unit) override {
    google::protobuf::UInt64Value result;
    std::lock_guard lock(mutex_);
    result.set_value(calls_);
    co_return result;
  }
  Async<Unit> start(Context ctx, Unit) override {
    co_await ctx.spawn("event heartbeat", [](Context worker) -> Async<void> {
      ::lifecycle::TickValue args;
      args.set_value(0);
      while (!(co_await worker.cancelled_for(std::chrono::milliseconds(50))))
        co_await Controls::tick().parallel(worker, args);
    });
    co_return Unit{};
  }
  Async<void> deactivate(Context ctx) override {
    co_await log<Dependencies>(std::move(ctx), "events.closed");
  }
};
using EventsModel = ::lifecycle::EventsModel<Events>;
} // namespace example::lifecycle
