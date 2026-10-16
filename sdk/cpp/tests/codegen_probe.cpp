#include <cordis/tests/profile.hpp>
#include "executor.hpp"
#include <iostream>
class Probe final : public profile::ProbeServer {
public:
  int entered = 0;
  cordis::Async<profile::Data> echo(cordis::Context,
                                    profile::Data args) override {
    ++entered;
    co_return args;
  }
  cordis::Async<cordis::data::Int64> number(cordis::Context,
                                            cordis::data::Int64 args) override {
    ++entered;
    co_return args.value() == -1 ? cordis::data::Int64{} : args;
  }
  cordis::Async<cordis::data::Unit> fail(cordis::Context,
                                         cordis::data::Text args) override {
    ++entered;
    if (args.value() == "refused")
      throw cordis::Refusal("expected", "runtime refusal");
    if (args.value() == "unknown")
      throw std::runtime_error("unknown");
    throw cordis::RpcError(args.value(), "business error");
    co_return cordis::data::Unit{};
  }
};
int main() {
  TestExecutor executor;
  cordis::Context::Operations ops;
  ops.invoke = [](std::string, std::string,
                  cordis::Message) -> cordis::Async<cordis::Message> {
    throw cordis::Refusal("unbound", "no grants");
    co_return cordis::Message("");
  };
  ops.metadata = [](std::string, std::string) -> cordis::Async<cordis::Value> {
    co_return nullptr;
  };
  ops.defer = [](std::string, auto) -> cordis::Async<cordis::Effect> {
    throw cordis::Refusal("unbound", "no grants");
    co_return cordis::Effect([]() -> cordis::Async<void> { co_return; });
  };
  cordis::Context ctx(std::move(ops));
  for (std::string line; std::getline(std::cin, line);) {
    auto input = cordis::Value::parse(line);
    auto business = std::make_shared<Probe>();
    profile::ProbeExport service(business);
    cordis::Value result;
    try {
      const auto method = input.at("method").get<std::string>();
      auto args = [&]() -> cordis::Message {
        try {
          return method == "echo" ? cordis::protobuf::encode_binary(
                                        cordis::protobuf::decode<profile::Data>(
                                            input.at("args")))
                 : method == "number"
                     ? cordis::protobuf::encode_binary(
                           cordis::protobuf::decode<cordis::data::Int64>(
                               input.at("args")))
                     : cordis::protobuf::encode_binary(
                           cordis::protobuf::decode<cordis::data::Text>(
                               input.at("args")));
        } catch (const std::exception &e) {
          throw cordis::Refusal("invalid_argument", e.what());
        }
      }();
      auto output = executor.wait(service.invoke(ctx, method, std::move(args)));
      auto value =
          method == "echo"
              ? cordis::protobuf::encode(
                    cordis::protobuf::decode_binary<profile::Data>(output))
          : method == "number"
              ? cordis::protobuf::encode(
                    cordis::protobuf::decode_binary<cordis::data::Int64>(
                        output))
              : cordis::protobuf::encode(
                    cordis::protobuf::decode_binary<cordis::data::Unit>(
                        output));
      result = {{"kind", "success"}, {"value", std::move(value)}};
    } catch (const cordis::Refusal &) {
      result = {{"kind", "refused"}};
    } catch (const cordis::RpcError &) {
      result = {{"kind", "rejected"}};
    } catch (const std::exception &) {
      result = {{"kind", "unknown"}};
    }
    result["entered"] = business->entered;
    std::cout << result.dump() << '\n';
  }
}
