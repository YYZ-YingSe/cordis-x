#include "../examples/resources/contract.hpp"
#include <cassert>
#include <cordis/cordis.hpp>

template <typename F> void rejected(F action, const std::string &path) {
  try {
    action();
    assert(false);
  } catch (const std::exception &e) {
    assert(std::string(e.what()).find(path) != std::string::npos);
  }
}
void raw_business_audit() {
  using cordis::Validation;
  const cordis::protobuf::Schema schema("google.protobuf.Value", cordis::contract::runtime_schema);
  const std::string bodies[] = {
      std::string{42, 11, 10, 9, 10, 1, 'k', 18, 2, 8, 0, 120, 120},
      std::string{42, 2, 120, 120, 8, 0}};
  for (const auto &bytes : bodies) {
    const cordis::Message body(bytes);
    for (auto policy : {Validation::Contract, Validation::Checked}) {
      (void)cordis::protobuf::decode_binary<google::protobuf::Value>(body, policy);
      schema.check(body, policy);
    }
    rejected([&] { (void)cordis::protobuf::decode_binary<google::protobuf::Value>(body, Validation::Audit); }, "undeclared protobuf field");
    rejected([&] { schema.check(body, Validation::Audit); }, "undeclared protobuf field");
  }
  schema.check(cordis::Message(std::string{42, 9, 10, 7, 10, 1, 'k', 18, 2, 8, 0}), Validation::Audit);
}
int main() {
  raw_business_audit();
  using namespace cordis::contract;
  auto a = value_interface(true), b = a;
  a["capabilities"] = {"clock", "log"};
  b["capabilities"] = {"log", "clock"};
  compare(a, b);
  auto counter = example::resources::counter_interface(), changed = counter;
  changed["resources"]["value"]["counter"]["methods"]["value"]["add"]["args"]
         ["message"] = "google.protobuf.StringValue";
  rejected([&] { compare(counter, changed); }, "add.args");
  rejected([&] { normalize(nullptr); }, "interface");
  b["capabilities"] = {"clock", "clock"};
  rejected([&] { normalize(b); }, "duplicate");
  changed = counter;
  changed["resources"]["value"]["counter"]["release"] = "never";
  rejected([&] { normalize(changed); }, "lifecycle");
  const cordis::Value integer = {{"message", "google.protobuf.Int64Value"},
                                 {"descriptor", runtime_schema}};
  check_value(integer, "9223372036854775807", "args");
  rejected([&] { check_value(integer, "9223372036854775808", "args"); },
           "args");
  rejected([&] { check_value(integer, 1.5, "args"); }, "args");
}
