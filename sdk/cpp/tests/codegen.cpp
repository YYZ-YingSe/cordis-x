#include <cordis/examples/examples.hpp>
#include <cordis/tests/profile.hpp>
#include "executor.hpp"
#include <cassert>

class Adder final : public example::AdderServer {
public:
  static inline int created = 0;
  int calls = 0;
  static std::shared_ptr<Adder> create(cordis::data::Unit) {
    ++created;
    return std::make_shared<Adder>();
  }
  cordis::Async<cordis::data::Int64> add(cordis::Context,
                                         example::AddArgs args) override {
    ++calls;
    const auto a = args.left(), b = args.right();
    if ((b > 0 && a > INT64_MAX - b) || (b < 0 && a < INT64_MIN - b))
      throw example::failure(example::AdderAddError::Overflow, "overflow");
    const auto sum = a + b;
    cordis::data::Int64 result;
    result.set_value(sum);
    co_return result;
  }
};
example::AddArgs arguments(std::int64_t a, std::int64_t b) {
  example::AddArgs args;
  args.set_left(a);
  args.set_right(b);
  return args;
}
int main() {
  TestExecutor executor;
  try {
    (void)example::AdderModel<Adder>::create(true);
    assert(false);
  } catch (const cordis::Refusal &) {
    assert(Adder::created == 0);
  }
  // Policies distinguish provider promises, declared fields and closed-schema auditing.
  example::AddArgs missing;
  (void)cordis::protobuf::encode_binary(missing, cordis::Validation::Contract);
  for (auto policy : {cordis::Validation::Checked, cordis::Validation::Audit}) {
    bool refused = false;
    try { (void)cordis::protobuf::encode_binary(missing, policy); }
    catch (const std::exception &) { refused = true; }
    assert(refused);
  }
  auto binary = arguments(1, 2).SerializeAsString();
  binary.append("\x78\x01", 2);
  for (auto policy : {cordis::Validation::Contract, cordis::Validation::Checked})
    assert(cordis::protobuf::decode_binary<example::AddArgs>(cordis::Message(binary), policy).right() == 2);
  bool audited = false;
  try { (void)cordis::protobuf::decode_binary<example::AddArgs>(cordis::Message(binary), cordis::Validation::Audit); }
  catch (const std::exception &) { audited = true; }
  assert(audited);
  auto business = std::make_shared<Adder>();
  auto service = std::make_shared<example::AdderExport>(business);
  cordis::Context::Operations ops;
  std::optional<cordis::Context> current;
  ops.invoke = [service, &current](std::string dep, std::string method,
                                   cordis::Message args) {
    assert(dep == "example.adder");
    return service->invoke(*current, std::move(method), std::move(args));
  };
  ops.metadata = [](std::string, std::string) -> cordis::Async<cordis::Value> {
    co_return nullptr;
  };
  ops.defer = [](std::string, auto) -> cordis::Async<cordis::Effect> {
    co_return cordis::Effect([]() -> cordis::Async<void> { co_return; });
  };
  current.emplace(std::move(ops));
  // Both the temporary client and argument go away before this lazy coroutine
  // runs.
  auto call = example::AdderClient{}.add(*current, arguments(20, 22));
  assert(executor.wait(std::move(call)).value() == 42);
  assert(business->calls == 1);
  try {
    executor.wait(service->invoke(*current, "add", cordis::Message("")));
    assert(false);
  } catch (const cordis::Refusal &e) {
    assert(e.code == "invalid_argument");
  }
  assert(business->calls == 1);
  try {
    executor.wait(
        example::AdderClient{}.add(*current, arguments(INT64_MAX, 1)));
    assert(false);
  } catch (const cordis::RpcError &e) {
    assert(e.code == "overflow");
  }
  auto exported_call = example::AdderExport(business).invoke(
      *current, "add", cordis::protobuf::encode_binary(arguments(1, 2)));
  assert(cordis::protobuf::decode_binary<cordis::data::Int64>(
             executor.wait(std::move(exported_call)))
             .value() == 3);
  for (
      const auto &option : cordis::Value::parse(
          R"([{"none":"NULL_VALUE"},{"some":{"none":"NULL_VALUE"}},{"some":{"some":false}},{"some":{"some":true}}])")) {
    auto native = cordis::protobuf::decode<profile::OptionOptionBool>(option);
    assert(cordis::protobuf::encode(native) == option);
  }
  auto native = cordis::protobuf::decode<profile::Data>(cordis::Value::parse(
      R"({"int32Value":0,"option":{"some":{"some":false}},"int64Value":"-9223372036854775808","uint64Value":"18446744073709551615","binary":"AAH/","float32Value":1.25,"float64Value":-1.5,"position":{"values":[1,2,3]},"tree":{"children":[{}]}})"));
  assert(native.uint64_value() == UINT64_MAX);
  assert(
      cordis::protobuf::decode<profile::Data>(cordis::protobuf::encode(native))
          .SerializeAsString() == native.SerializeAsString());
  native.mutable_position()->mutable_values()->RemoveLast();
  try {
    cordis::protobuf::encode(native);
    assert(false);
  } catch (const std::exception &) {
  }
}
