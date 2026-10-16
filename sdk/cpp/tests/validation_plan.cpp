#include <cassert>
#include <cordis/core/protobuf.hpp>
#include <profile.pb.h>
#include <thread>

template <class F> void rejects(F action) {
  bool rejected = false;
  try {
    action();
  } catch (const std::exception &) {
    rejected = true;
  }
  assert(rejected);
}
std::string schema(int minimum, int maximum) {
  google::protobuf::FileDescriptorSet files;
  auto *file = files.add_file();
  file->set_name("limits.proto");
  file->set_package("limits");
  file->set_syntax("proto3");
  file->add_dependency("cordis/options.proto");
  auto *record = file->add_message_type();
  record->set_name("Record");
  auto *field = record->add_field();
  field->set_name("value");
  field->set_number(1);
  field->set_type(google::protobuf::FieldDescriptorProto::TYPE_INT64);
  field->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
  field->mutable_options()->SetExtension(cordis::schema::minimum, minimum);
  field->mutable_options()->SetExtension(cordis::schema::maximum, maximum);
  return absl::Base64Escape(files.SerializeAsString());
}
int main() {
  using cordis::Validation;
  // Concurrent first use includes a mutually recursive schema graph.
  std::vector<std::thread> threads;
  for (int i = 0; i < 4; ++i)
    threads.emplace_back([] {
      profile::LinkA value;
      value.mutable_next()->mutable_next();
      for (int n = 0; n < 100; ++n)
        cordis::protobuf::validate(value, Validation::Checked);
    });
  for (auto &thread : threads)
    thread.join();
  profile::Config config;
  config.set_limit(42);
  cordis::protobuf::validate(config, Validation::Checked);
  config.set_limit(0);
  rejects([&] { cordis::protobuf::validate(config, Validation::Checked); });
  config.clear_limit();
  rejects([&] { cordis::protobuf::validate(config, Validation::Audit); });
  cordis::protobuf::validate(config, Validation::Contract);
  // Same name, different constraints, destroyed/rebuilt owners. No pointer or
  // type-name cache may preserve one dynamic pool's plan for another pool.
  for (int n = 0; n < 8; ++n) {
    cordis::protobuf::Schema low("limits.Record", schema(0, 10));
    cordis::protobuf::Schema high("limits.Record", schema(20, 30));
    cordis::Message five(std::string("\x08\x05", 2)),
        twenty_five(std::string("\x08\x19", 2));
    low.check(five);
    high.check(twenty_five);
    rejects([&] { low.check(twenty_five); });
    rejects([&] { high.check(five); });
  }
  profile::Data data;
  data.set_int32_value(0);
  data.mutable_option()->set_none(cordis::data::NULL_VALUE);
  data.set_binary(std::string(cordis::protobuf::max_bytes, '\0'));
  cordis::protobuf::validate(data, Validation::Checked);
  data.set_text("x");
  rejects([&] { cordis::protobuf::validate(data, Validation::Checked); });
  data.clear_binary();
  data.clear_text();
  data.mutable_flags()->Resize(static_cast<int>(cordis::protobuf::max_nodes),
                               false);
  rejects([&] { cordis::protobuf::validate(data, Validation::Checked); });
  data.clear_flags();
  data.set_status(static_cast<profile::Status>(99));
  rejects([&] { cordis::protobuf::validate(data, Validation::Checked); });
  data.set_status(profile::STATUS_ACTIVE);
  auto *tree = data.mutable_tree();
  for (int i = 0; i < 64; ++i)
    tree = tree->add_children();
  rejects([&] { cordis::protobuf::validate(data, Validation::Checked); });
}
