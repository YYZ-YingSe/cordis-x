#include <cordis/examples/workflows.hpp>
#include "executor.hpp"
#include <cassert>
using namespace cordis;
using Controls = workflows::WorkflowControls;
using Number = cordis::data::Int64;
Message tick(int n) {
  workflows::Tick v;
  v.set_value(n);
  return protobuf::encode_binary(v);
}
int main() {
  TestExecutor executor;
  std::function<Async<cordis::Flow>(Context, Message)> listener;
  std::function<Async<Message>(Context, Message, cordis::Next)> handler;
  Context::Operations ops;
  ops.invoke = [](std::string, std::string, Message) -> Async<Message> {
    co_return Message("");
  };
  ops.metadata = [](std::string, std::string) -> Async<Value> {
    co_return nullptr;
  };
  ops.defer = [](std::string, auto) -> Async<Effect> {
    co_return Effect([]() -> Async<void> { co_return; });
  };
  ops.on = [&](std::string alias, auto callback) -> Async<Effect> {
    assert(alias == "tick");
    listener = callback;
    co_return Effect([]() -> Async<void> { co_return; });
  };
  ops.middleware = [&](std::string alias, auto callback) -> Async<Effect> {
    assert(alias == "request");
    handler = callback;
    co_return Effect([]() -> Async<void> { co_return; });
  };
  Context ctx(std::move(ops));
  int entered = 0;
  // A temporary generated control object is destroyed before this lazy call
  // starts.
  auto registration = Controls::tick().on(
      ctx, [&](Context, workflows::Tick args) -> Async<typed::Flow<Number>> {
        ++entered;
        if (args.value() == 1)
          co_return typed::Break(Number{});
        if (args.value() == 2)
          throw RpcError("undeclared", "fault");
        if (args.value() == 3)
          throw Refusal("overflow", "authority");
        co_return typed::Continue<Number>();
      });
  executor.wait(std::move(registration));
  try {
    executor.wait(listener(ctx, Message("")));
    assert(false);
  } catch (const Refusal &) {
  }
  assert(entered == 0);
  assert(!executor.wait(listener(ctx, tick(0))).value);
  for (auto n : {1, 2}) {
    try {
      executor.wait(listener(ctx, tick(n)));
      assert(false);
    } catch (const RpcError &) {
      assert(false);
    } catch (const std::exception &) {
    }
  }
  try {
    executor.wait(listener(ctx, tick(3)));
    assert(false);
  } catch (const Refusal &) {
  }
  executor.wait(Controls::request().register_handler(
      ctx,
      [](Context, workflows::Tick args,
         typed::Next<workflows::Tick, Number> next) -> Async<Number> {
        try {
          co_await next.run(workflows::Tick{});
          assert(false);
        } catch (const Refusal &) {
        }
        co_return co_await next.run(std::move(args));
      }));
  int next_calls = 0;
  cordis::Next next([&](Message value) -> Async<Message> {
    ++next_calls;
    co_return value;
  });
  assert(protobuf::decode_binary<Number>(
             executor.wait(handler(ctx, tick(7), next)))
             .value() == 7);
  assert(next_calls == 1);
  int created = 0;
  for (const auto &value : std::vector<Message>{
           Message(""), Message(std::string("\x0a\x01\xff", 3)),
           Message(std::string("\x78\x01", 2))}) {
    try {
      Controls::worker().build(value, [&](workflows::WorkerConfig) {
        ++created;
        return 0;
      });
      assert(false);
    } catch (const Refusal &) {
    }
  }
  assert(created == 0);
  assert(Controls::worker().build(
             protobuf::encode_binary(protobuf::decode<workflows::WorkerConfig>(
                 Value{{"label", "x"}})),
             [](workflows::WorkerConfig c) { return c.label(); }) == "x");
  try {
    Controls::settings().decode(Message(""));
    assert(false);
  } catch (const Refusal &) {
    assert(false);
  } catch (const std::exception &) {
  }
}
