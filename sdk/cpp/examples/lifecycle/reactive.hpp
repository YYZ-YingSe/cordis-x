#pragma once
#include "common.hpp"
namespace example::lifecycle {
class Reactive final : public ::lifecycle::ReactiveServer {
  using Controls = ::lifecycle::ReactiveControls;
  using Dependencies = ::lifecycle::ReactiveDependencies;

public:
  static std::shared_ptr<Reactive> create(cordis::data::Unit) {
    return std::make_shared<Reactive>();
  }
  Async<Unit> subscribe(Context ctx, Unit) override {
    co_await Controls::notice().on(
        ctx, [](Context listener, Number value) -> Async<typed::Flow<Unit>> {
          co_await log<Dependencies>(
              std::move(listener), "notice: " + std::to_string(value.value()));
          co_return typed::Continue<Unit>();
        });
    co_await Controls::calculate().register_handler(
        ctx,
        [](Context handler, Number input,
           typed::Next<Number, Number> next) -> Async<Number> {
          co_await log<Dependencies>(handler, "before calculation");
          auto result = co_await next.run(number(add(input.value(), 10)));
          co_await log<Dependencies>(handler, "after calculation");
          co_return number(add(result.value(), result.value()));
        });
    co_return Unit{};
  }
  Async<Number> process(Context ctx, Number args) override {
    auto result = co_await Controls::calculate().run(
        ctx, std::move(args), [](Context, Number input) -> Async<Number> {
          co_return number(add(input.value(), 1));
        });
    co_await Controls::notice().parallel(ctx, result);
    co_return result;
  }
};
using ReactiveModel = ::lifecycle::ReactiveModel<Reactive>;
} // namespace example::lifecycle
