#pragma once
#include "common.hpp"
namespace example::lifecycle {
inline google::protobuf::UInt64Value count(std::uint64_t n) {
  google::protobuf::UInt64Value value;
  value.set_value(n);
  return value;
}
// A diagnostic projection only. The source handles retain all execution and
// recovery responsibility, including a failed or uncertain operation.
inline Value outcome_projection(const Value &value, bool sequence) {
  if (value.is_null())
    return {{"pending", Value::object()}};
  if (!value.is_object() || value.size() != 2)
    throw std::runtime_error("invalid managed outcome");
  const auto kind = value.at("kind").get<std::string>();
  if (kind == "success") {
    if (!sequence && !value.at("value").is_null())
      throw std::runtime_error("task success must have no payload");
    return {{"succeeded", sequence ? value.at("value") : Value::object()}};
  }
  if (kind == "failure" || kind == "refused")
    return {{kind == "failure" ? "failed" : "refused", value.at("error")}};
  throw std::runtime_error("invalid managed outcome kind");
}
inline ::lifecycle::SequenceStatus sequence_status(Value value) {
  value.at("forward") = outcome_projection(value.at("forward"), true);
  return protobuf::decode<::lifecycle::SequenceStatus>(value);
}
inline ::lifecycle::TaskStatus task_status(Value value) {
  value.at("outcome") = outcome_projection(value.at("outcome"), false);
  return protobuf::decode<::lifecycle::TaskStatus>(value);
}
inline ::lifecycle::Completion completion(std::string value) {
  return protobuf::decode<::lifecycle::Completion>(
      Value{{"completion", std::move(value)}});
}
struct SilentLog {
  static Async<void> write(Context, std::string) { co_return; }
};
struct SubscriptionLog {
  static Async<void> write(Context ctx, std::string text) {
    co_await log<::lifecycle::AuditedSubscriptionsDependencies>(
        std::move(ctx), std::move(text));
  }
};
} // namespace example::lifecycle
