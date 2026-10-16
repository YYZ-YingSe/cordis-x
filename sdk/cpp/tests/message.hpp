#pragma once
#include <cordis/core/protobuf.hpp>
namespace test {
inline cordis::Message body(const cordis::Value &value) {
  return cordis::protobuf::encode_binary(
      cordis::protobuf::decode<google::protobuf::Value>(value));
}
inline cordis::Value json(const cordis::Message &value) {
  return cordis::protobuf::encode(
      cordis::protobuf::decode_binary<google::protobuf::Value>(value));
}
} // namespace test
