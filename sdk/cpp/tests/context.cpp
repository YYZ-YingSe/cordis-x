#include "binary.hpp"
#include "executor.hpp"
static void check(bool condition) {
  if (!condition)
    throw std::runtime_error("context regression");
}
int main() {
  TestExecutor executor;
  test::Peer peer;
  cordis::process::Context ctx(peer.channel, std::string(32, 'b'),
                               {{"logger", "1"}});
  try {
    executor.wait(ctx.invoke("missing", "write", test::body(nullptr)));
    return 1;
  } catch (const cordis::RpcError &error) {
    check(error.code == "undeclared_dependency");
  }
  auto call = executor.start(
      [ctx] { return ctx.invoke("logger", "write", test::body("hello")); });
  auto request = peer.next();
  check(test::json(request.payload.one()) == "hello");
  auto response = cordis::session::response(
      request, {cordis::session::Outcome::Kind::success,
                cordis::session::Payload(test::body(42)),
                {},
                {}});
  auto stale = response;
  stale.operation = "99";
  try {
    peer.channel->receive(stale);
    return 1;
  } catch (const std::runtime_error &error) {
    check(dynamic_cast<const cordis::RpcError *>(&error) == nullptr);
  }
  peer.channel->receive(response);
  check(test::json(call.get()) == 42);
  try {
    peer.channel->receive(response);
    return 1;
  } catch (const std::runtime_error &error) {
    check(dynamic_cast<const cordis::RpcError *>(&error) == nullptr);
  }
  executor.wait(ctx.finish());
  try {
    executor.wait(ctx.invoke("logger", "write", test::body(nullptr)));
    return 1;
  } catch (const cordis::RpcError &error) {
    check(error.code == "closed");
  }
  cordis::process::Context next(peer.channel, std::string(32, 'c'),
                                {{"logger", "1"}});
  auto waiting = executor.start(
      [next] { return next.invoke("logger", "write", test::body(nullptr)); });
  peer.next();
  peer.channel->close();
  try {
    waiting.get();
    return 1;
  } catch (const std::runtime_error &error) {
    check(dynamic_cast<const cordis::RpcError *>(&error) == nullptr);
  }
  executor.wait(next.finish());
}
