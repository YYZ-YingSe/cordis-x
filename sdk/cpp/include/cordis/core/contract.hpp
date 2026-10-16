#pragma once
#include <cordis/runtime_schema.hpp>
#include "protobuf.hpp"
#include "value.hpp"
#include <set>
#include <stdexcept>

namespace cordis::contract {
inline Value value_interface(bool resources) {
  Value message = {{"message", "google.protobuf.Value"},
                   {"descriptor", runtime_schema}};
  Value method = {{"args", message}, {"result", message}, {"errors", {"*"}}};
  Value factories = {{"mode", "named"}, {"value", Value::object()}};
  if (resources)
    factories = {{"mode", "dynamic"},
                 {"value",
                  {{"args", message},
                   {"methods", {{"mode", "dynamic"}, {"value", method}}},
                   {"errors", {"*"}},
                   {"ownership", "caller"},
                   {"release", "drain_once"}}}};
  return {{"methods", {{"mode", "dynamic"}, {"value", method}}},
          {"resources", factories},
          {"controls", Value::object()},
          {"capabilities", Value::array()}};
}
inline void fail(const std::string &path, const std::string &message) {
  throw std::runtime_error(path + ": " + message);
}
inline void exact(const Value &value, std::initializer_list<const char *> keys,
                  const std::string &path) {
  if (!value.is_object() || value.size() != keys.size())
    fail(path, "invalid descriptor fields");
  for (auto key : keys)
    if (!value.contains(key))
      fail(path, "missing descriptor field");
}
inline void name(const std::string &name, const std::string &path) {
  if (name.empty() || name.size() > 1024)
    fail(path, "invalid identifier");
}
inline void take(size_t &budget, size_t depth, const std::string &path) {
  if (!budget || depth > 32)
    fail(path, "descriptor exceeds node/depth limit");
  --budget;
}
inline void names(Value &value, size_t &budget, const std::string &path) {
  if (!value.is_array())
    fail(path, "expected identifier set");
  std::set<std::string> seen;
  for (auto &item : value) {
    take(budget, 0, path);
    if (!item.is_string())
      fail(path, "expected identifier");
    const auto text = item.get<std::string>();
    name(text, path);
    if (!seen.insert(text).second)
      fail(path, "duplicate identifier");
  }
  value = seen;
}
inline void message(Value &value, size_t &budget, size_t depth,
                    const std::string &path) {
  take(budget, depth, path);
  exact(value, {"message", "descriptor"}, path);
  if (!value["message"].is_string() || !value["descriptor"].is_string())
    fail(path, "invalid message descriptor");
  name(value["message"].get<std::string>(), path);
  (void)protobuf::Schema(value["message"].get<std::string>(),
                         value["descriptor"].get<std::string>());
  if (value["descriptor"].get_ref<const std::string &>().size() > 1398104)
    fail(path, "schema byte limit exceeded");
}
inline void method(Value &value, size_t &budget, const std::string &path) {
  take(budget, 0, path);
  exact(value, {"args", "result", "errors"}, path);
  message(value["args"], budget, 0, path + ".args");
  message(value["result"], budget, 0, path + ".result");
  names(value["errors"], budget, path + ".errors");
}
template <typename Check>
void dispatch(Value &value, size_t &budget, const std::string &path,
              Check check) {
  take(budget, 0, path);
  exact(value, {"mode", "value"}, path);
  if (value["mode"] == "dynamic") {
    check(value["value"], budget, path);
    return;
  }
  if (value["mode"] != "named" || !value["value"].is_object())
    fail(path, "invalid dispatch");
  for (auto &item : value["value"].items()) {
    name(item.key(), path);
    check(item.value(), budget, path + "." + item.key());
  }
}
inline Value normalize(Value value) {
  size_t budget = 4096;
  exact(value, {"methods", "resources", "controls", "capabilities"},
        "interface");
  dispatch(value["methods"], budget, "methods", method);
  dispatch(value["resources"], budget, "resources",
           [](Value &r, size_t &budget, const std::string &path) {
             exact(r, {"args", "methods", "errors", "ownership", "release"},
                   path);
             if (r["ownership"] != "caller" || r["release"] != "drain_once")
               fail(path, "unsupported resource lifecycle");
             message(r["args"], budget, 0, path + ".args");
             names(r["errors"], budget, path + ".errors");
             dispatch(r["methods"], budget, path + ".methods", method);
           });
  names(value["capabilities"], budget, "capabilities");
  if (!value["controls"].is_object())
    fail("controls", "expected control map");
  for (auto &entry : value["controls"].items()) {
    const auto path = "controls." + entry.key();
    name(entry.key(), path);
    take(budget, 0, path);
    auto &control = entry.value();
    const auto kind = control.at("kind").get<std::string>();
    if (kind == "event" || kind == "middleware") {
      const auto member = kind == "event" ? "listener" : "handler";
      exact(control, {"kind", "contract", member}, path);
      method(control[member], budget, path + "." + member);
    } else if (kind == "factory") {
      exact(control, {"kind", "config", "errors"}, path);
      message(control["config"], budget, 0, path + ".config");
      names(control["errors"], budget, path + ".errors");
    } else if (kind == "provision") {
      exact(control, {"kind", "contract", "value"}, path);
      message(control["value"], budget, 0, path + ".value");
    } else
      fail(path, "unknown control kind");
    if (kind != "factory") {
      exact(control["contract"], {"name", "revision"}, path + ".contract");
      name(control["contract"]["name"].get<std::string>(), path);
      name(control["contract"]["revision"].get<std::string>(), path);
    }
  }
  return value;
}
inline void difference(const Value &expected, const Value &actual,
                       const std::string &path) {
  if (expected == actual)
    return;
  if (expected.is_object() && actual.is_object()) {
    if (expected.size() != actual.size())
      fail(path, "declared identities differ");
    for (auto &item : expected.items()) {
      if (!actual.contains(item.key()))
        fail(path + "." + item.key(), "missing declaration");
      difference(item.value(), actual[item.key()], path + "." + item.key());
    }
    return;
  }
  if (path.ends_with(".descriptor"))
    fail(path, "message definitions differ");
  fail(path, "expected " + expected.dump() + ", received " + actual.dump());
}
inline void compare(const Value &expected, const Value &actual) {
  difference(normalize(expected), normalize(actual), "interface");
}
inline const Value &operation(const Value &dispatch, const std::string &name) {
  if (dispatch.at("mode") == "dynamic")
    return dispatch.at("value");
  if (!dispatch.at("value").contains(name))
    fail(name, "undeclared operation");
  return dispatch.at("value").at(name);
}
template <class Data>
inline void check_value(const Value &descriptor, const Data &value,
                        const std::string &path, Validation policy = Validation::Checked) {
  if constexpr (std::is_same_v<Data, cordis::Message>) {
    if (policy == Validation::Contract) return;
  }
  try {
    protobuf::Schema schema(descriptor.at("message").get<std::string>(),
                            descriptor.at("descriptor").get<std::string>());
    if constexpr (std::is_same_v<Data, cordis::Message>) schema.check(value, policy);
    else schema.check(value);
  } catch (const std::exception &error) {
    fail(path, error.what());
  }
}
} // namespace cordis::contract
