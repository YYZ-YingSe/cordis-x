// Provider constructors are private while the consumer keeps one public API.
#include <cordis/tests/public_service.hpp>
#include <cordis/tests/provider_local.hpp>
#include "executor.hpp"
#include <cassert>
struct Local : fixture_provider::EchoServer {
  std::string directory;
  explicit Local(std::string value) : directory(std::move(value)) {}
  static std::shared_ptr<Local> create(fixture::provider::Local config) {
    return std::make_shared<Local>(config.directory());
  }
  cordis::Async<fixture::public_api::Value> get(cordis::Context, fixture::public_api::Value) override {
    fixture::public_api::Value out;
    out.set_text(directory);
    co_return out;
  }
};
int main() {
  assert(fixture_public_api::EchoClient::contract() == fixture_provider::EchoClient::contract());
  assert(fixture_public_api::EchoClient::interface() == fixture_provider::EchoClient::interface());
  auto service = fixture_provider::EchoModel<Local>::create({{"directory", "/objects"}});
  TestExecutor executor;
  std::optional<cordis::Context> dispatch;
  cordis::Context::Operations ops;
  ops.invoke = [service, &dispatch](std::string, std::string method, cordis::Message args) -> cordis::Async<cordis::Message> {
    co_return co_await service->invoke(*dispatch, std::move(method), std::move(args));
  };
  ops.metadata=[](std::string, std::string) -> cordis::Async<cordis::Value> {co_return nullptr;};
  ops.defer=[](std::string, auto) -> cordis::Async<cordis::Effect> {
    co_return cordis::Effect([]() -> cordis::Async<void> {co_return;});
  };
  cordis::Context ctx(std::move(ops));
  dispatch=ctx;
  executor.wait(service->activate(ctx));
  auto result = executor.wait(fixture_public_api::EchoClient{}.get(ctx, {}));
  assert(result.text()=="/objects");
  executor.wait(service->deactivate(ctx));
}
