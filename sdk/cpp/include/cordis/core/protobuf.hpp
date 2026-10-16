#pragma once
#include "message.hpp"
#include "protobuf-plan.hpp"
#include "protobuf-wire.hpp"
#include "value.hpp"
#include <absl/strings/escaping.h>
#include <cmath>
#include <cordis/options.pb.h>
#include <cordis/types.pb.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/empty.pb.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/message.h>
#include <google/protobuf/struct.pb.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/wrappers.pb.h>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utf8_validity.h>

namespace cordis::protobuf {
inline google::protobuf::util::JsonParseOptions parse_options() {
  google::protobuf::util::JsonParseOptions options;
  options.allow_legacy_nonconformant_behavior = false;
  return options;
}
inline google::protobuf::util::JsonPrintOptions print_options() {
  google::protobuf::util::JsonPrintOptions options;
  options.allow_legacy_nonconformant_behavior = false;
  return options;
}
inline void json_budget(const Value &value, std::size_t depth,
                        std::size_t &nodes) {
  if (depth > 96 || !nodes)
    throw std::runtime_error("JSON depth/node limit exceeded");
  --nodes;
  if (value.is_binary() || value.is_discarded())
    throw std::runtime_error("not a JSON value");
  if (value.is_number() && !std::isfinite(value.get<double>()))
    throw std::runtime_error("non-finite JSON number");
  if (value.is_structured())
    for (auto &child : value)
      json_budget(child, depth + 1, nodes);
}
inline void json_budget(const Value &value) {
  std::size_t nodes = max_nodes;
  json_budget(value, 0, nodes);
}
inline void json_precision(const google::protobuf::Descriptor *desc,
                           const Value &value, std::size_t depth = 0) {
  if (depth > max_depth)
    throw std::runtime_error("message nesting limit exceeded");
  const auto scalar = [](const Field *field, const Value &value) {
    if ((field->cpp_type() == Field::CPPTYPE_INT64 ||
         field->cpp_type() == Field::CPPTYPE_UINT64) &&
        value.is_number() &&
        std::abs(value.get<double>()) > static_cast<double>(max_integer))
      throw std::runtime_error(
          "int64/uint64 outside the JSON safe range must use a decimal string");
  };
  if (desc->full_name().starts_with("google.protobuf.")) {
    if (desc->file()->name() == "google/protobuf/wrappers.proto") {
      if (value.is_null())
        throw std::runtime_error("scalar message requires a value");
      scalar(desc->field(0), value);
    }
    return;
  }
  if (!value.is_object())
    return;
  for (int i = 0; i < desc->field_count(); ++i) {
    const auto *field = desc->field(i);
    auto it = value.find(std::string(field->json_name()));
    if (it == value.end())
      it = value.find(std::string(field->name()));
    if (it == value.end() || it->is_null())
      continue;
    const auto *item_field =
        field->is_map() ? field->message_type()->map_value() : field;
    const auto visit = [&](const Value &item) {
      scalar(item_field, item);
      if (item_field->cpp_type() == Field::CPPTYPE_MESSAGE)
        json_precision(item_field->message_type(), item, depth + 1);
    };
    if (field->is_map()) {
      if (it->is_object())
        for (const auto &item : *it)
          visit(item);
    } else if (field->is_repeated()) {
      if (it->is_array())
        for (const auto &item : *it)
          visit(item);
    } else
      visit(*it);
  }
}
template <class T> T decode(const Value &value) {
  json_budget(value);
  json_precision(T::descriptor(), value);
  T message;
  const auto status = google::protobuf::util::JsonStringToMessage(
      value.dump(), &message, parse_options());
  if (!status.ok())
    throw std::runtime_error(std::string(status.message()));
  validate(message);
  return message;
}
template <class T> Value encode(const T &message) {
  validate(message);
  std::string json;
  const auto status = google::protobuf::util::MessageToJsonString(
      message, &json, print_options());
  if (!status.ok())
    throw std::runtime_error(std::string(status.message()));
  return Value::parse(json);
}
template <class T>
T decode_binary(const cordis::Message &value,
                Validation policy = Validation::Contract) {
  T message;
  if (policy == Validation::Audit)
    audit_binary(reinterpret_cast<const uint8_t *>(value.bytes().data()),
                 static_cast<int>(value.bytes().size()),
                 message.GetDescriptor());
  if (!message.ParseFromString(value.bytes()))
    throw std::runtime_error("invalid protobuf body");
  validate(message, policy, true);
  return message;
}
template <class T>
cordis::Message encode_binary(const T &message,
                              Validation policy = Validation::Contract) {
  validate(message, policy);
  const auto size = message.ByteSizeLong();
  if (size > max_bytes)
    throw std::runtime_error("encoded message byte limit exceeded");
  if (!message.IsInitialized())
    throw std::runtime_error("uninitialized protobuf message");
  std::string bytes(size, '\0');
  auto *begin = reinterpret_cast<uint8_t *>(bytes.data());
  // Standard protobuf API: cached sizes belong to this unchanged const DTO.
  if (message.SerializeWithCachedSizesToArray(begin) != begin + size)
    throw std::runtime_error("protobuf encoding size mismatch");
  return cordis::Message(std::move(bytes));
}
class Schema {
  google::protobuf::DescriptorPool pool_{
      google::protobuf::DescriptorPool::generated_pool()};
  google::protobuf::DynamicMessageFactory factory_{&pool_};
  const Message *prototype_{};
  mutable std::once_flag plan_ready_;
  mutable std::unique_ptr<const Plan> plan_;
  const Plan &plan() const {
    std::call_once(plan_ready_, [this] {
      plan_ = std::make_unique<const Plan>(prototype_->GetDescriptor());
    });
    return *plan_;
  }

public:
  Schema(const std::string &name, const std::string &encoded) {
    (void)google::protobuf::Empty::descriptor();
    (void)google::protobuf::Value::descriptor();
    (void)google::protobuf::Int64Value::descriptor();
    (void)cordis::data::Unit::descriptor();
    std::string bytes;
    if (encoded.size() > 1398104 || !absl::Base64Unescape(encoded, &bytes) ||
        absl::Base64Escape(bytes) != encoded)
      throw std::runtime_error("invalid schema descriptor");
    google::protobuf::FileDescriptorSet files;
    if (!files.ParseFromString(bytes))
      throw std::runtime_error("invalid schema descriptor");
    for (const auto &file : files.file()) {
      if (pool_.FindFileByName(file.name()))
        continue;
      if (!pool_.BuildFile(file))
        throw std::runtime_error("invalid schema dependency");
    }
    const auto *descriptor = pool_.FindMessageTypeByName(name);
    if (descriptor && (descriptor->full_name() == "cordis.data.Lease" ||
                       descriptor->full_name() == "cordis.data.Child"))
      throw std::runtime_error("resource capabilities are not data");
    if (!descriptor)
      throw std::runtime_error("unknown message type");
    prototype_ = factory_.GetPrototype(descriptor);
  }
  void check(const cordis::Message &value,
             Validation policy = Validation::Checked) const {
    if (policy == Validation::Audit)
      audit_binary(reinterpret_cast<const uint8_t *>(value.bytes().data()),
                   static_cast<int>(value.bytes().size()),
                   prototype_->GetDescriptor());
    auto message = std::unique_ptr<Message>(prototype_->New());
    if (!message->ParseFromString(value.bytes()))
      throw std::runtime_error("invalid protobuf body");
    if (policy != Validation::Contract)
      plan().validate(*message, policy, true);
  }
  void check(const Value &value) const {
    json_budget(value);
    json_precision(prototype_->GetDescriptor(), value);
    auto message = std::unique_ptr<Message>(prototype_->New());
    const auto status = google::protobuf::util::JsonStringToMessage(
        value.dump(), message.get(), parse_options());
    if (!status.ok())
      throw std::runtime_error(std::string(status.message()));
    plan().validate(*message);
  }
};
} // namespace cordis::protobuf
