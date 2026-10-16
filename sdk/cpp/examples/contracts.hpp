#pragma once
#include <cordis/examples/pricing.hpp>
#include <cordis/core/contract.hpp>
namespace example::contracts {
inline cordis::Value logger() { return fixtures_pricing::LoggerClient::interface(); }
inline cordis::Value pricing() { return fixtures_pricing::PricingClient::interface(); }
inline cordis::Value method(cordis::Value args, cordis::Value result,
                            cordis::Value errors = cordis::Value::array()) {
  return {{"args", std::move(args)},
          {"result", std::move(result)},
          {"errors", std::move(errors)}};
}
inline cordis::Value message(const char *name) {
  return {{"message", name}, {"descriptor", cordis::contract::runtime_schema}};
}
inline cordis::Value service(cordis::Value methods) {
  return {
      {"controls", cordis::Value::object()},
      {"methods", {{"mode", "named"}, {"value", std::move(methods)}}},
      {"resources", {{"mode", "named"}, {"value", cordis::Value::object()}}},
      {"capabilities", cordis::Value::array()}};
}
} // namespace example::contracts
