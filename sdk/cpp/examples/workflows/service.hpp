#pragma once
#include <cordis/examples/workflows.hpp>
#include <limits>

namespace example::workflows {
using namespace cordis;
using Controls = ::workflows::WorkflowControls;
using Number = cordis::data::Int64;
using Tick = ::workflows::Tick;
using Unit = cordis::data::Unit;
inline Number number(std::int64_t value) {
  Number result;
  result.set_value(value);
  return result;
}
inline std::int64_t add(std::int64_t a, std::int64_t b) {
  if ((b > 0 && a > INT64_MAX - b) || (b < 0 && a < INT64_MIN - b))
    throw RpcError("overflow", "int64 overflow");
  return a + b;
}
inline ::workflows::Decision decision(typed::Flow<Number> flow) {
  ::workflows::Decision result;
  if (flow.value)
    *result.mutable_stopped() = std::move(*flow.value);
  else
    result.mutable_continued();
  return result;
}
class Workflow final : public ::workflows::WorkflowServer {
public:
  static std::shared_ptr<Workflow> create(Unit) {
    return std::make_shared<Workflow>();
  }
  Async<void> activate(Context ctx) override {
    co_await Controls::tick().on(
        ctx, [](Context, Tick args) -> Async<typed::Flow<Number>> {
          if (args.value() < 0)
            co_return typed::Continue<Number>();
          co_return typed::Break(number(add(args.value(), 1)));
        });
    co_await Controls::request().register_handler(
        ctx,
        [](Context, Tick args,
           typed::Next<Tick, Number> next) -> Async<Number> {
          if (args.value() == 0)
            co_return number(100);
          args.set_value(add(args.value(), 1));
          auto result = co_await next.run(std::move(args));
          result.set_value(add(result.value(), 3));
          co_return result;
        });
    ::workflows::Settings settings;
    settings.set_answer(42);
    co_await Controls::settings().provide(ctx, std::move(settings));
  }
  Async<Number> run(Context ctx, Tick args) override {
    co_return co_await Controls::request().run(
        ctx, std::move(args), [](Context, Tick args) -> Async<Number> {
          co_return number(add(args.value(), args.value()));
        });
  }
  Async<::workflows::Decision> serial(Context ctx, Tick args) override {
    co_return decision(co_await Controls::tick().serial(ctx, std::move(args)));
  }
  Async<::workflows::Decisions> parallel(Context ctx, Tick args) override {
    auto flows = co_await Controls::tick().parallel(ctx, std::move(args));
    ::workflows::Decisions result;
    for (auto &flow : flows)
      *result.add_values() = decision(std::move(flow));
    co_return result;
  }
  Async<Unit> spawn_worker(Context ctx,
                           ::workflows::WorkerConfig config) override {
    auto child = co_await Controls::worker().install(ctx, std::move(config));
    co_await child.ready();
    // The current owner retains the child; shutdown drains and disposes it.
    co_return Unit{};
  }
};
using Model = ::workflows::WorkflowModel<Workflow>;
} // namespace example::workflows
