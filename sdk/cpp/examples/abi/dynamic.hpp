#pragma once
#include <cordis/adapters/abi/context.hpp>
// These small lifecycle examples deliberately expose google.protobuf.Value.
// JSON objects are business data here; the ABI itself carries protobuf bytes.
namespace example {
inline cordis::Message message(const cordis::Value &value) {
  return cordis::protobuf::encode_binary(
      cordis::protobuf::decode<google::protobuf::Value>(value));
}
inline cordis::Value value(const cordis::Message &message) {
  return cordis::protobuf::encode(
      cordis::protobuf::decode_binary<google::protobuf::Value>(message));
}
inline cordis::Async<cordis::Value> call(cordis::abi::Context ctx,
                                         std::string dependency,
                                         std::string method,
                                         cordis::Value args) {
  co_return value(co_await ctx.invoke(std::move(dependency), std::move(method),
                                      message(args)));
}
} // namespace example
