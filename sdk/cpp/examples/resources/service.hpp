#pragma once
#include <cordis/examples/counters.hpp>
#include <limits>
#include <mutex>
namespace example::resources {
using namespace cordis;
using namespace fixtures_counters;
using Number = google::protobuf::Int64Value;
using Unit = google::protobuf::Empty;
inline Number number(int64_t value) {
  Number result;
  result.set_value(value);
  return result;
}
class Counter final : public CountersCounterServer {
  std::mutex mutex;
  int64_t value;

public:
  explicit Counter(int64_t value) : value(value) {}
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
  Async<void> close(Context ctx) override {
    fixtures::counters::CounterClosed closed;
    {
      std::lock_guard lock(mutex);
      closed.set_counter_closed(value);
    }
    co_await CountersDependencies::logger().write(ctx, std::move(closed));
  }
};
class Counters final : public CountersServer {
public:
  static std::shared_ptr<Counters> create(cordis::data::Unit) {
    return std::make_shared<Counters>();
  }
  Async<std::shared_ptr<CountersCounterServer>> counter(Context,
                                                        Number args) override {
    co_return std::make_shared<Counter>(args.value());
  }
};
class Client final : public CounterHolderServer {
  std::mutex mutex;
  std::optional<CountersCounterClient> resource;
  CountersCounterClient saved() {
    std::lock_guard lock(mutex);
    if (!resource)
      throw RpcError("empty", "acquire a counter first");
    return *resource;
  }

public:
  static std::shared_ptr<Client> create(cordis::data::Unit) {
    return std::make_shared<Client>();
  }
  Async<Unit> acquire(Context ctx, Number args) override {
    auto counter = co_await CounterHolderDependencies::counters().counter(
        ctx, std::move(args));
    {
      std::lock_guard lock(mutex);
      resource = std::move(counter);
    }
    co_return Unit{};
  }
  Async<Number> get(Context ctx, Unit args) override {
    co_return co_await saved().get(ctx, std::move(args));
  }
  Async<Number> add(Context ctx, Number args) override {
    co_return co_await saved().add(ctx, std::move(args));
  }
  Async<Unit> release(Context ctx, Unit) override {
    co_await saved().dispose(ctx);
    co_return Unit{};
  }
  Async<Number> scoped(Context ctx, Number args) override {
    auto counter = co_await CounterHolderDependencies::counters().counter(
        ctx, std::move(args));
    auto scope = co_await ctx.scope("counter scope");
    co_await scope.ready();
    // Scope transports Value; keep its conversion at this SDK boundary.
    auto result = co_await scope.run([counter](Context inner) -> Async<Value> {
      co_await inner.defer("read before close",
                           [counter](Context cleanup) -> Async<void> {
                             co_await counter.get(cleanup, Unit{});
                           });
      co_return protobuf::encode(co_await counter.get(inner, Unit{}));
    });
    co_await scope.dispose();
    co_await counter.dispose(ctx);
    co_return protobuf::decode<Number>(result);
  }
};
using CounterModel = CountersModel<Counters>;
using ClientModel = CounterHolderModel<Client>;
} // namespace example::resources
