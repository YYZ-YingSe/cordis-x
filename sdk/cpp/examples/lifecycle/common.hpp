#pragma once
#include <cordis/examples/lifecycle.hpp>
#include <limits>
#include <map>
#include <mutex>
#include <set>

namespace example::lifecycle {
using namespace cordis;
using Unit = google::protobuf::Empty;
using Number = google::protobuf::Int64Value;
using Text = google::protobuf::StringValue;
inline Number number(std::int64_t value) {
  Number result;
  result.set_value(value);
  return result;
}
inline std::int64_t add(std::int64_t a, std::int64_t b) {
  if ((b > 0 && a > INT64_MAX - b) || (b < 0 && a < INT64_MIN - b))
    throw RpcError("overflow", "int64 arithmetic overflow");
  return a + b;
}
template <class Dependencies> Async<void> log(Context ctx, std::string text) {
  Text args;
  args.set_value(std::move(text));
  co_await Dependencies::logger().write(std::move(ctx), std::move(args));
}
inline ::lifecycle::ChildStatus child_status(const Value &value) {
  return protobuf::decode<::lifecycle::ChildStatus>(value);
}
} // namespace example::lifecycle
