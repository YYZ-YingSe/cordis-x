#include <cordis/examples/calculator_abi.hpp>
#include <cordis/adapters/abi.hpp>
#include <limits>
#ifdef CORDIS_PACKAGE_HELPER
#include "../../tests/package/helper.hpp"
#define CORDIS_ABI_CODE_VERSION CORDIS_HELPER_SYMBOL()
#endif
#ifndef CORDIS_ABI_CODE_VERSION
#define CORDIS_ABI_CODE_VERSION "v1"
#endif
using namespace cordis;
using namespace fixtures_calculator_abi;
using namespace fixtures::calculator_abi;
using Number = google::protobuf::Int64Value;
using Text = google::protobuf::StringValue;
using Unit = google::protobuf::Empty;
static Number number(int64_t value) {
  Number result;
  result.set_value(value);
  return result;
}
static Text version() {
  Text result;
  result.set_value(CORDIS_ABI_CODE_VERSION);
  return result;
}
static int64_t add(int64_t a, int64_t b) {
  if ((b > 0 && a > INT64_MAX - b) || (b < 0 && a < INT64_MIN - b))
    throw RpcError("overflow", "int64 addition overflow");
  return a + b;
}
class Counter final : public ServiceCounterServer {
  int64_t value;

public:
  explicit Counter(int64_t value) : value(value) {}
  Async<Number> get(Context, Unit) override { co_return number(value); }
  Async<Number> add(Context, Number args) override {
    value = ::add(value, args.value());
    co_return number(value);
  }
  Async<Text> code_version(Context, Unit) override { co_return version(); }
  Async<void> close(Context) override { co_return; }
};
class Calculator final : public ServiceServer {
public:
  static std::shared_ptr<Calculator> create(cordis::data::Unit) {
    return std::make_shared<Calculator>();
  }
  Async<Text> code_version(Context, Unit) override { co_return version(); }
  Async<google::protobuf::Value> echo(Context,
                                      google::protobuf::Value args) override {
    co_return args;
  }
  Async<Number> add(Context, AddArgs args) override {
    co_return number(::add(args.left(), args.right()));
  }
  Async<std::shared_ptr<ServiceCounterServer>>
  counter(Context, CounterArgs args) override {
    co_return std::make_shared<Counter>(args.initial());
  }
};
using Model = ServiceModel<Calculator>;
CORDIS_ABI_PLUGIN(Model)
