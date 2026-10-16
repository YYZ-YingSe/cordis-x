#pragma once
#include <cordis/core/handles.hpp>
#include <cordis/core/protobuf.hpp>
#include <transport.pb.h>
#include <variant>

namespace cordis::session {
using Json = Value;
inline constexpr std::size_t max_message = 1024 * 1024;
inline constexpr const char *content_type = "application/protobuf";
struct Payload {
  Value control = nullptr;
  std::vector<Message> messages;
  Payload() = default;
  Payload(Value value) : control(std::move(value)) {}
  Payload(Value value, Message body)
      : control(std::move(value)), messages{std::move(body)} {}
  explicit Payload(Message body) : messages{std::move(body)} {}
  Message one() const {
    if (messages.size() != 1)
      throw std::runtime_error("expected one business message");
    return messages.front();
  }
  Value control_only() const {
    if (!messages.empty())
      throw std::runtime_error("unexpected business message");
    return control;
  }
};
struct Identity {
  std::string incarnation, fiber, epoch;
  bool operator==(const Identity &) const = default;
};
struct Header {
  std::string session;
  Identity activation;
  std::string operation;
};
struct Request : Header {
  std::string command;
  Payload payload;
};
struct Outcome {
  enum class Kind { success, failure, refused, unknown } kind = Kind::success;
  Payload value;
  std::string code, message;
};
struct Response : Header {
  Outcome outcome;
};
using Frame = std::variant<Request, Response>;
inline Response response(const Request &request, Outcome outcome) {
  return {static_cast<const Header &>(request), std::move(outcome)};
}
inline Value identity_value(const Identity &identity) {
  return {{"incarnation", identity.incarnation},
          {"fiber", identity.fiber},
          {"epoch", identity.epoch}};
}
inline Payload flow_payload(const Flow &flow) {
  return flow.value ? Payload(Value{{"kind", "break"}}, *flow.value)
                    : Payload(Value{{"kind", "continue"}});
}
inline Flow parse_flow(const Payload &value) {
  if (!value.control.is_object() || value.control.size() != 1)
    throw std::runtime_error("invalid event flow");
  if (value.control.value("kind", "") == "continue" && value.messages.empty())
    return Continue();
  if (value.control.value("kind", "") == "break")
    return Break(value.one());
  throw std::runtime_error("invalid event flow");
}
inline void control_to_proto(const Value &value, google::protobuf::Value *out) {
  if (value.is_null())
    out->set_null_value(google::protobuf::NULL_VALUE);
  else if (value.is_boolean())
    out->set_bool_value(value.get<bool>());
  else if (value.is_number()) {
    if (!std::isfinite(value.get<double>()))
      throw std::runtime_error("non-finite control number");
    if (std::trunc(value.get<double>()) == value.get<double>() &&
        std::abs(value.get<double>()) > static_cast<double>(max_integer))
      throw std::runtime_error(
          "control integers outside safe range require a decimal string");
    out->set_number_value(value.get<double>());
  } else if (value.is_string())
    out->set_string_value(value.get_ref<const std::string &>());
  else if (value.is_array()) {
    auto *list = out->mutable_list_value();
    for (const auto &item : value)
      control_to_proto(item, list->add_values());
  } else if (value.is_object()) {
    auto *fields = out->mutable_struct_value()->mutable_fields();
    for (const auto &item : value.items())
      control_to_proto(item.value(), &(*fields)[item.key()]);
  } else
    throw std::runtime_error("invalid control value");
}
inline void validate_control(const Value &value) {
  std::size_t nodes = 8192, bytes = max_message;
  auto take = [&](std::size_t n) {
    if (n > bytes)
      throw std::runtime_error("control byte limit exceeded");
    bytes -= n;
  };
  auto walk = [&](auto &&self, const Value &v, std::size_t depth) -> void {
    if (depth > 16)
      throw std::runtime_error("control nesting too deep");
    if (!nodes--)
      throw std::runtime_error("control node limit exceeded");
    take(2);
    if (v.is_number()) {
      const auto n = v.get<double>();
      if (!std::isfinite(n) || (std::trunc(n) == n && std::abs(n) > static_cast<double>(max_integer)))
        throw std::runtime_error("invalid control number");
    }
    if (v.is_binary() || v.is_discarded()) throw std::runtime_error("invalid control value");
    if (v.is_string()) {
      const auto &text = v.get_ref<const std::string &>();
      take(text.size());
      if (!utf8_range::IsStructurallyValid(text)) throw std::runtime_error("invalid control Unicode");
    }
    else if (v.is_array())
      for (const auto &child : v)
        self(self, child, depth + 1);
    else if (v.is_object())
      for (const auto &entry : v.items()) {
        take(entry.key().size());
        if (!utf8_range::IsStructurallyValid(entry.key())) throw std::runtime_error("invalid control key");
        self(self, entry.value(), depth + 1);
      }
  };
  walk(walk, value, 0);
}
inline Value control_from_proto(const google::protobuf::Value &value) {
  using V = google::protobuf::Value;
  switch (value.kind_case()) {
  case V::kNullValue:
    if (value.null_value() != google::protobuf::NULL_VALUE) throw std::runtime_error("invalid null enum");
    return nullptr;
  case V::kBoolValue:
    return value.bool_value();
  case V::kNumberValue: {
    const double n = value.number_value();
    if (std::trunc(n) == n && std::abs(n) <= static_cast<double>(max_integer))
      return static_cast<int64_t>(n);
    return n;
  }
  case V::kStringValue:
    return value.string_value();
  case V::kListValue: {
    auto out = Value::array();
    for (const auto &item : value.list_value().values())
      out.push_back(control_from_proto(item));
    return out;
  }
  case V::kStructValue: {
    auto out = Value::object();
    for (const auto &item : value.struct_value().fields())
      out[item.first] = control_from_proto(item.second);
    return out;
  }
  default:
    throw std::runtime_error("missing control value branch");
  }
}
inline void payload_to_proto(const Payload &value, transport::Payload *out) {
  validate_control(value.control);
  if (value.messages.size() > 4096)
    throw std::runtime_error("too many business messages");
  auto remaining = max_message;
  for (const auto &body : value.messages) {
    if (body.bytes().size() > remaining)
      throw std::runtime_error("message too large");
    remaining -= body.bytes().size();
  }
  control_to_proto(value.control, out->mutable_control());
  for (const auto &body : value.messages)
    out->add_messages(body.bytes());
}
inline Payload payload_from_proto(transport::Payload &&value) {
  if (value.messages_size() > 4096)
    throw std::runtime_error("too many business messages");
  if (!value.has_control())
    throw std::runtime_error("missing control");
  Payload out(control_from_proto(value.control()));
  validate_control(out.control);
  for (auto &body : *value.mutable_messages())
    out.messages.emplace_back(std::move(body));
  return out;
}
inline std::vector<uint8_t> bounded(const google::protobuf::Message &value) {
  const auto size = value.ByteSizeLong();
  if (size > max_message)
    throw std::runtime_error("message too large");
  std::vector<uint8_t> bytes(size);
  if (!value.IsInitialized()) throw std::runtime_error("uninitialized protobuf frame");
  if (value.SerializeWithCachedSizesToArray(bytes.data()) != bytes.data() + size)
    throw std::runtime_error("protobuf encoding size mismatch");
  return bytes;
}
inline std::vector<uint8_t> encode(const Payload &value) {
  transport::Payload out;
  payload_to_proto(value, &out);
  return bounded(out);
}
inline Payload decode_payload(const uint8_t *data, std::size_t length, Validation policy = Validation::Checked) {
  if (length > max_message)
    throw std::runtime_error("message too large");
  transport::Payload value;
  if (!value.ParseFromArray(data, static_cast<int>(length)))
    throw std::runtime_error("invalid protobuf payload");
  if (policy == Validation::Audit)
    protobuf::audit_binary(data, static_cast<int>(length), value.GetDescriptor());
  return payload_from_proto(std::move(value));
}
inline std::vector<uint8_t> encode(const Frame &frame) {
  transport::Frame out;
  std::visit(
      [&](const auto &value) {
        out.set_session(value.session);
        out.set_operation(value.operation);
        auto *identity = out.mutable_activation();
        identity->set_incarnation(value.activation.incarnation);
        identity->set_fiber(value.activation.fiber);
        identity->set_epoch(value.activation.epoch);
        if constexpr (std::is_same_v<std::decay_t<decltype(value)>, Request>) {
          auto *request = out.mutable_request();
          request->set_command(value.command);
          payload_to_proto(value.payload, request->mutable_payload());
        } else {
          auto *response = out.mutable_response();
          const auto &result = value.outcome;
          if (result.kind == Outcome::Kind::success)
            payload_to_proto(result.value, response->mutable_success());
          else {
            auto *error = result.kind == Outcome::Kind::failure
                              ? response->mutable_rejected()
                          : result.kind == Outcome::Kind::refused
                              ? response->mutable_refused()
                              : response->mutable_unknown();
            error->set_code(result.code);
            error->set_message(result.message);
          }
        }
      },
      frame);
  return bounded(out);
}
inline Frame decode(const uint8_t *data, std::size_t length, Validation policy) {
  if (length > max_message)
    throw std::runtime_error("message too large");
  transport::Frame value;
  if (!value.ParseFromArray(data, static_cast<int>(length)))
    throw std::runtime_error("invalid protobuf frame");
  if (policy == Validation::Audit)
    protobuf::audit_binary(data, static_cast<int>(length), value.GetDescriptor());
  if (!value.has_activation())
    throw std::runtime_error("missing activation");
  Header header{value.session(),
                {value.activation().incarnation(), value.activation().fiber(),
                 value.activation().epoch()},
                value.operation()};
  if (value.has_request()) {
    if (!value.request().has_payload())
      throw std::runtime_error("missing payload");
    return Request{std::move(header), value.request().command(),
                   payload_from_proto(std::move(*value.mutable_request()->mutable_payload()))};
  }
  if (!value.has_response())
    throw std::runtime_error("missing frame direction");
  auto &result = *value.mutable_response();
  if (result.has_success())
    return Response{std::move(header),
                    Outcome{Outcome::Kind::success,
                            payload_from_proto(std::move(*result.mutable_success())),
                            {},
                            {}}};
  const auto *error = result.has_rejected()  ? &result.rejected()
                      : result.has_refused() ? &result.refused()
                      : result.has_unknown() ? &result.unknown()
                                             : nullptr;
  if (!error)
    throw std::runtime_error("missing outcome");
  auto kind = result.has_rejected()  ? Outcome::Kind::failure
              : result.has_refused() ? Outcome::Kind::refused
                                     : Outcome::Kind::unknown;
  return Response{std::move(header),
                  Outcome{kind, {}, error->code(), error->message()}};
}
inline Frame decode(const std::vector<uint8_t> &bytes, Validation policy = Validation::Checked) {
  return decode(bytes.data(), bytes.size(), policy);
}

} // namespace cordis::session
