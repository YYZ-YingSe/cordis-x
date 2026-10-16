#pragma once
#include <cordis/examples/portable.hpp>
#include <limits>
#include <mutex>
namespace example {
using namespace cordis;
using Number = google::protobuf::Int64Value;
using Unit = google::protobuf::Empty;
using Dynamic = google::protobuf::Value;
using Controls = portable::PortableCppControls;
using Dependencies = portable::PortableCppDependencies;
inline Number number(int64_t value) {
  Number result;
  result.set_value(value);
  return result;
}
inline Async<Value> log(Context ctx, Value value) {
  auto result = co_await Dependencies::logger().write(
      ctx, protobuf::decode<Dynamic>(value));
  co_return protobuf::encode(result);
}
class Counter final : public portable::PortableCppCounterServer {
  int64_t value;
  std::mutex mutex;

public:
  explicit Counter(int64_t value) : value(value) {}
  Async<portable::Metadata> metadata(Context ctx, Unit) override {
    co_return protobuf::decode<portable::Metadata>(
        co_await ctx.metadata("example.portable", "1"));
  }
  Async<Number> get(Context, Unit) override {
    std::lock_guard lock(mutex);
    co_return number(value);
  }
  Async<Number> add(Context, Number args) override {
    std::lock_guard lock(mutex);
    const auto delta = args.value();
    if ((delta > 0 && value > INT64_MAX - delta) ||
        (delta < 0 && value < INT64_MIN - delta))
      throw RpcError("overflow", "counter overflow");
    value += delta;
    co_return number(value);
  }
  Async<void> close(Context) override { co_return; }
};
class Calculator final : public portable::PortableCppServer {
  std::mutex mutex;
  uint64_t calls = 0;
  std::optional<Context> saved;

public:
  static std::shared_ptr<Calculator> create(cordis::data::Unit) {
    return std::make_shared<Calculator>();
  }
  Async<Dynamic> echo(Context, Dynamic args) override { co_return args; }
  Async<google::protobuf::UInt64Value> count(Context, Unit) override {
    std::lock_guard lock(mutex);
    if (calls == UINT64_MAX)
      throw RpcError("overflow", "call count overflow");
    google::protobuf::UInt64Value result;
    result.set_value(++calls);
    co_return result;
  }
  Async<Unit> save_context(Context ctx, Unit) override {
    std::lock_guard lock(mutex);
    saved = ctx;
    co_return Unit{};
  }
  Async<google::protobuf::BoolValue> expired(Context, Unit) override {
    auto old = [&] {
      std::lock_guard lock(mutex);
      return saved;
    }();
    if (!old)
      throw RpcError("empty", "no saved Context");
    google::protobuf::BoolValue result;
    try {
      co_await example::log(*old, "must not run");
    } catch (const RpcError &) {
      result.set_value(true);
    }
    co_return result;
  }
  Async<portable::Metadata> metadata(Context ctx, Unit) override {
    co_return protobuf::decode<portable::Metadata>(
        co_await ctx.metadata("example.portable", "1"));
  }
  Async<Dynamic> log(Context ctx, Dynamic args) override {
    co_return co_await Dependencies::logger().write(ctx, std::move(args));
  }
  Async<Unit> defer(Context ctx, Dynamic) override {
    auto effect = co_await ctx.defer(
        "portable contribution", [](Context cleanup) -> Async<void> {
          co_await example::log(cleanup, "portable cleanup");
        });
    co_await effect.dispose();
    co_return Unit{};
  }
  Async<Number> scope(Context ctx, Unit) override {
    auto scope = co_await ctx.scope("portable scope");
    co_await scope.ready();
    if (!(co_await scope.snapshot()).is_object())
      throw std::runtime_error("invalid scope snapshot");
    auto value = co_await scope.run([](Context inner) -> Async<Value> {
      co_await inner.defer("scope cleanup", [](Context cleanup) -> Async<void> {
        co_await example::log(cleanup, "scope-cleanup");
      });
      co_return co_await example::log(inner, 42);
    });
    co_await scope.dispose();
    co_return protobuf::decode<Number>(value);
  }
  Async<Unit> sequence(Context ctx, Unit) override {
    auto sequence = co_await ctx.effect(
        "portable sequence", [](Context) -> Async<EffectStep> {
          co_return EffectStep::complete(
              Inverse{"final inverse", [](Context cleanup) -> Async<void> {
                        co_await example::log(cleanup, "final-inverse");
                      }});
        });
    if ((co_await sequence.ready()) != "completed")
      throw std::runtime_error("sequence stopped");
    if (!(co_await sequence.snapshot()).is_object())
      throw std::runtime_error("invalid sequence snapshot");
    co_await sequence.dispose();
    co_return Unit{};
  }
  Async<Unit> task(Context ctx, Unit) override {
    auto task =
        co_await ctx.spawn("portable task", [](Context worker) -> Async<void> {
          co_await worker.cancelled();
          co_await example::log(worker, "task-stopped");
        });
    if (!(co_await task.snapshot()).is_object())
      throw std::runtime_error("invalid task snapshot");
    co_return Unit{};
  }
  Async<portable::Decisions> event(Context ctx, Unit) override {
    auto subscription = co_await Controls::message().on(
        ctx, [](Context inner, Number args) -> Async<typed::Flow<Number>> {
          co_await example::log(inner, args.value());
          co_return typed::Break(std::move(args));
        });
    auto serial = co_await Controls::message().serial(ctx, number(42));
    auto dispatch = co_await Controls::message().emit(ctx, number(42));
    if (!(co_await dispatch.snapshot()).is_object())
      throw std::runtime_error("invalid dispatch snapshot");
    auto flows = co_await dispatch.join();
    if (flows.size() != 1 || !flows[0].value || flows[0].value->value() != 42 ||
        !serial.value || serial.value->value() != 42)
      throw std::runtime_error("invalid event results");
    co_await subscription.dispose();
    portable::Decisions result;
    for (auto &flow : flows) {
      auto decision = result.add_values();
      if (flow.value)
        *decision->mutable_stopped() = std::move(*flow.value);
      else
        decision->mutable_continued();
    }
    co_return result;
  }
  Async<portable::Wrapped> middleware(Context ctx, Unit) override {
    auto subscription = co_await Controls::chain().register_handler(
        ctx,
        [](Context, Number args, typed::Next<Number, portable::Wrapped> next)
            -> Async<portable::Wrapped> {
          co_return co_await next.run(std::move(args));
        });
    auto result = co_await Controls::chain().run(
        ctx, number(42), [](Context, Number args) -> Async<portable::Wrapped> {
          portable::Wrapped result;
          result.set_wrapped(args.value());
          co_return result;
        });
    co_await subscription.dispose();
    co_return result;
  }
  Async<Unit> child(Context ctx, Unit) override {
    auto child = co_await Controls::worker().install(ctx, cordis::data::Unit{});
    co_await child.ready();
    co_await child.settled();
    if (!(co_await child.status()).is_object())
      throw std::runtime_error("invalid child status");
    co_await child.dispose();
    co_return Unit{};
  }
  Async<std::shared_ptr<portable::PortableCppCounterServer>>
  counter(Context, portable::CounterArgs args) override {
    co_return std::make_shared<Counter>(args.initial());
  }
};
using Model = portable::PortableCppModel<Calculator>;
} // namespace example
