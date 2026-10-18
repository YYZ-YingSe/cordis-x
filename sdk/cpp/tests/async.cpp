#include "message.hpp"
#include <cassert>
#include <cordis/cordis.hpp>
#include <deque>

struct Loop {
  std::deque<std::function<void()>> ready;
  bool running = false;
};
template <> struct corral::EventLoopTraits<Loop> {
  static void run(Loop &loop) {
    loop.running = true;
    while (loop.running) {
      assert(!loop.ready.empty());
      auto next = std::move(loop.ready.front());
      loop.ready.pop_front();
      next();
    }
  }
  static void stop(Loop &loop) { loop.running = false; }
  static bool isRunning(Loop &loop) { return loop.running; }
  static corral::EventLoopID eventLoopID(Loop &loop) {
    return corral::EventLoopID(&loop);
  }
};
struct Completion {
  Loop &loop;
  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> handle) {
    loop.ready.push_back([handle] { handle.resume(); });
  }
  void await_resume() {}
};

cordis::Async<void> check(Loop &loop) {
  using namespace cordis;
  auto capture = std::make_shared<std::string>(4096, 'x');
  std::weak_ptr<std::string> weak = capture;
  {
    Async<Message> invocation;
    Async<std::vector<Flow>> dispatch;
    {
      Context::Operations operations;
      operations.invoke = [capture, &loop](std::string dependency,
                                           std::string method,
                                           Message args) -> Async<Message> {
        co_await Completion{loop};
        assert(*capture == std::string(4096, 'x'));
        assert(dependency == "dependency" && method == "method");
        co_return args;
      };
      operations.metadata = [](std::string, std::string) -> Async<Value> {
        co_return nullptr;
      };
      operations.defer =
          [](std::string,
             std::function<Async<void>(Context)>) -> Async<Effect> {
        co_return Effect([]() -> Async<void> { co_return; });
      };
      operations.emit = [&loop](std::string event,
                                Message args) -> Async<Dispatch> {
        co_await Completion{loop};
        assert(event == "event");
        co_return Dispatch(
            [args, &loop]() -> Async<std::vector<Flow>> {
              co_await Completion{loop};
              co_return std::vector<Flow>{Break(args)};
            },
            []() -> Async<Value> { co_return nullptr; });
      };
      Context context(std::move(operations));
      invocation = context.invoke("dependency", "method",
                                  test::body(Value{{"answer", 42}}));
      dispatch = context.parallel("event", test::body(7));
    }
    capture.reset();
    // Both the Context and argument temporaries are gone before execution
    // starts.
    assert(!weak.expired());
    auto result = co_await std::move(invocation);
    assert(test::json(result) == Value({{"answer", 42}}));
    assert(test::json(*(co_await std::move(dispatch)).at(0).value) == 7);
  }
  assert(weak.expired());
}
int main() {
  Loop loop;
  corral::run(loop, check(loop));
  assert(loop.ready.empty());
}
