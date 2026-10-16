// The core API needs no process host or C ABI declarations.
#include "executor.hpp"
#include <cassert>
#include <cordis/cordis.hpp>

int main() {
  TestExecutor executor;
  bool open = true;
  cordis::Context::Operations operations;
  operations.invoke =
      [&open](std::string, std::string,
              cordis::Message value) -> cordis::Async<cordis::Message> {
    if (!open)
      throw cordis::RpcError("closed", "callback ended");
    co_return value;
  };
  operations.metadata = [](std::string,
                           std::string) -> cordis::Async<cordis::Value> {
    co_return nullptr;
  };
  operations.defer = [](std::string, auto) -> cordis::Async<cordis::Effect> {
    co_return cordis::Effect([]() -> cordis::Async<void> { co_return; });
  };
  const cordis::Context ctx(std::move(operations));
  assert(executor.wait(ctx.invoke("echo", "echo", cordis::Message("opaque")))
             .bytes() == "opaque");
  const auto saved = ctx;
  open = false;
  try {
    executor.wait(saved.invoke("echo", "echo", cordis::Message("opaque")));
    return 1;
  } catch (const cordis::RpcError &error) {
    assert(error.code == "closed");
  }
}
