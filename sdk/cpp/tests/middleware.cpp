#include "binary.hpp"
#include "executor.hpp"
#include <condition_variable>
#include <cordis/adapters/process/middleware.hpp>
#include <deque>

static void check(bool ok) {
  if (!ok)
    throw std::runtime_error("middleware regression");
}
using Peer = test::Peer;
int main() {
  TestExecutor executor;
  Peer peer;
  auto registry = std::make_shared<cordis::process::MiddlewareRegistry>(
      cordis::process::Json{
          {"request", {{"name", "example.request"}, {"revision", "1"}}}});
  auto context = [&] {
    return cordis::process::Context(peer.channel, std::string(32, 'b'),
                                    {{"logger", "1"}}, nullptr, false, nullptr,
                                    nullptr, nullptr, {}, nullptr, registry);
  };
  auto original = context();
  corral::Event entered;
  std::promise<void> returned;
  std::future<cordis::Message> detached;
  auto registering = executor.start([&] {
    return original.middleware(
        "request",
        [&](auto args, auto,
            cordis::process::Next next) -> cordis::Async<cordis::Message> {
          detached = executor.start([next, args] { return next.run(args); });
          co_await entered;
          returned.set_value();
          co_return test::body(9);
        });
  });
  auto registration = peer.next();
  cordis::process::Next next(peer.channel, std::string(32, 'b'), "w0",
                             std::string(32, 'c'));
  auto invoke = executor.start([&] {
    return registry->invoke("m1", "w0", test::body(7), context(), next);
  });
  auto continuation = peer.next();
  check(continuation.command == "middleware_next");
  try {
    executor.wait(next.run(test::body(1)));
    check(false);
  } catch (const cordis::process::RpcError &e) {
    check(e.code == "duplicate_next");
  }
  executor.runtime.post([&] { entered.trigger(); });
  returned.get_future().wait();
  check(invoke.wait_for(std::chrono::milliseconds(20)) ==
        std::future_status::timeout);
  try {
    registry->release("m1");
    check(false);
  } catch (const cordis::process::RpcError &e) {
    check(e.code == "middleware_running");
  }
  peer.reply(continuation, 14);
  check(test::json(detached.get()) == 14);
  check(test::json(invoke.get().one()) == 9);
  try {
    executor.wait(next.run(test::body(1)));
    check(false);
  } catch (const cordis::process::RpcError &e) {
    check(e.code == "expired_next");
  }
  peer.reply(registration,
             {{"handler", "m1"}, {"grant", std::string(32, 'f')}});
  auto handle = registering.get();
  executor.wait(original.finish());
  auto disposing = executor.start([&] { return handle.dispose(); });
  auto disposal = peer.next();
  registry->release("m1");
  registry->release("m1");
  peer.reply(disposal, cordis::process::Json::object());
  disposing.get();
  check(registry->released());
  std::vector<std::future<cordis::process::Json>> waits;
  std::vector<cordis::session::Request> requests;
  for (int n = 0; n < 16; ++n) {
    waits.push_back(executor.start([&] {
      return peer.channel->request("middleware_next",
                                   cordis::process::Json::object());
    }));
    requests.push_back(peer.next());
  }
  auto run = executor.start([&] {
    return context().waterfall(
        "request", test::body(2),
        [](auto v, auto) -> cordis::Async<cordis::Message> { co_return v; });
  });
  auto request = peer.next();
  check(request.command == "middleware_run");
  check(test::json(
            executor
                .wait(registry->terminal("q1", "w1", test::body(3), context()))
                .one()) == 3);
  try {
    executor.wait(registry->terminal("q1", "w2", test::body(3), context()));
    check(false);
  } catch (const cordis::process::RpcError &e) {
    check(e.code == "closed");
  }
  peer.reply(request, 3);
  check(test::json(run.get()) == 3);
  for (auto req : requests)
    peer.reply(req, nullptr);
  for (auto &wait : waits)
    wait.get();
  check(registry->released());
  for (bool known : {true, false}) {
    auto current =
        std::make_shared<cordis::process::MiddlewareRegistry>(cordis::Value{
            {"request", {{"name", "example.request"}, {"revision", "1"}}}});
    auto pending = executor.start([&] {
      return current->run(
          peer.channel, "context", "request", test::body(1),
          [](auto args, auto) -> cordis::Async<cordis::Message> {
            co_return args;
          });
    });
    auto req = peer.next();
    using namespace cordis::session;
    peer.channel->receive(response(
        req, Outcome{known ? Outcome::Kind::refused : Outcome::Kind::unknown,
                     Payload{}, "lost", "lost"}));
    bool failed = false;
    try {
      pending.get();
    } catch (const std::exception &) {
      failed = true;
    }
    check(failed);
    check(current->released() == known);
  }
}
