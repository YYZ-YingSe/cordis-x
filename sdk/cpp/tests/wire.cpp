#include <cordis/adapters/process/wire.hpp>
#include <sstream>
using namespace cordis::session;
static void check(bool ok) {
  if (!ok)
    throw std::runtime_error("wire regression");
}
template <class F> static void rejected(F run) {
  try {
    run();
  } catch (const std::exception &) {
    return;
  }
  throw std::runtime_error("invalid wire accepted");
}
int main() {
  // Parsed strings transfer ownership; retaining them does not retain or borrow
  // the soon-to-be-destroyed protobuf carrier.
  cordis::transport::Payload parsed;
  parsed.mutable_control()->set_null_value(google::protobuf::NULL_VALUE);
  parsed.add_messages(std::string(256 * 1024, 'x'));
  const auto *original = parsed.messages(0).data();
  auto moved = payload_from_proto(std::move(parsed));
  check(moved.one().bytes().data() == original);
  parsed.Clear();
  check(moved.one().bytes().size() == 256 * 1024);

  std::string raw;
  for (unsigned n = 0; n < 256; ++n)
    raw += static_cast<char>(n);
  Payload payload(cordis::Value{{"method", "echo"}}, cordis::Message(raw));
  const Request request{Header{std::string(32, 'a'), {"1", "2", "3"}, "4"},
                        "call", payload};
  auto encoded = encode(Frame(request));
  auto decoded = std::get<Request>(decode(encoded));
  check(decoded.payload.one().bytes() == raw);
  check(decoded.payload.control == payload.control);
  encoded.assign(encoded.size(), 255);
  check(decoded.payload.one().bytes() == raw);
  rejected([&] { decode(encoded); });
  rejected([] { decode({}); });
  auto unknown = encode(Frame(request));
  unknown.insert(unknown.end(), {0x78, 1});
  (void)decode(unknown, cordis::Validation::Contract);
  (void)decode(unknown, cordis::Validation::Checked);
  rejected([&] { decode(unknown, cordis::Validation::Audit); });
  rejected(
      [] { encode(Payload(cordis::Message(std::string(max_message, 'x')))); });
  cordis::Value nested = nullptr;
  for (unsigned n = 0; n < 16; ++n)
    nested = cordis::Value::array({nested});
  validate_control(nested);
  rejected([&] { validate_control(cordis::Value::array({nested})); });
  rejected([] { validate_control(cordis::Value(9007199254740992ULL)); });
  rejected([] {
    validate_control(cordis::Value(std::string(1, static_cast<char>(255))));
  });
  cordis::Value object = nullptr;
  for (unsigned n = 0; n < 16; ++n)
    object = cordis::Value{{"nested", object}};
  cordis::Value wide = cordis::Value::object();
  for (unsigned n = 0; n < 8191; ++n)
    wide[std::to_string(n)] = nullptr;
  for (const auto &control : {object, wide}) {
    Payload maximum(control);
    maximum.messages.assign(4096, cordis::Message(""));
    auto bounded = request;
    bounded.payload = maximum;
    for (auto policy : {cordis::Validation::Contract, cordis::Validation::Checked, cordis::Validation::Audit}) {
      auto result = std::get<Request>(decode(encode(Frame(bounded)), policy));
      check(result.payload.control == control && result.payload.messages.size() == 4096);
    }
    maximum.messages.emplace_back("");
    rejected([&] { encode(maximum); });
  }
  std::stringstream stream;
  auto bytes = encode(Frame(request));
  cordis::transports::write_frame(stream, bytes);
  cordis::transports::write_frame(stream, bytes);
  Frame frame;
  check(cordis::transports::read_frame(stream, frame));
  check(cordis::transports::read_frame(stream, frame));
  check(!cordis::transports::read_frame(stream, frame));
  auto complete = stream.str();
  for (std::size_t n = 1; n < 4 + bytes.size(); ++n) {
    std::stringstream truncated(complete.substr(0, n));
    rejected([&] { cordis::transports::read_frame(truncated, frame); });
  }
}
