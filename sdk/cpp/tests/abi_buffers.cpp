#include <cordis/adapters/abi/abi.hpp>

static void check(bool valid) {
  if (!valid)
    throw std::runtime_error("ABI buffer regression");
}
template <class Call> static void rejected(Call call) {
  try {
    call();
  } catch (const std::exception &) {
    return;
  }
  throw std::runtime_error("invalid ABI buffer accepted");
}

int main() {
  namespace detail = cordis::abi::detail;
  const cordis::session::Payload expected(cordis::Message(std::string(3 * 1024 * 1024, 'x')));
  for (auto validation : {cordis::Validation::Contract, cordis::Validation::Checked, cordis::Validation::Audit}) {
    detail::Output input;
    const auto raw = input.build(expected);
    const auto copied = detail::read(&raw, validation);
    input.bytes.assign(input.bytes.size(), 0);
    check(copied.one().bytes() == expected.one().bytes());

    auto output = detail::result(expected);
    const auto result = detail::read(&output.value, validation);
    detail::release(&output);
    check(output.owner == nullptr && output.value.len == 0);
    check(result.one().bytes() == expected.one().bytes());
  }
  const std::string message(2 * 1024 * 1024, 'x');
  auto output = detail::protect([&]() -> CordisAbiResult {
    throw cordis::Refusal("failed", message);
  });
  check(output.status == CORDIS_ABI_REFUSED);
  check(detail::text(output.message) == message);
  detail::release(&output);
  check(output.owner == nullptr);

  rejected([] { detail::read(static_cast<const CordisAbiBuffer *>(nullptr)); });
  rejected([] { detail::read(CordisAbiBuffer{nullptr, 1}); });
  const uint8_t byte = 0;
  rejected([&] { detail::read(CordisAbiBuffer{&byte, static_cast<std::size_t>(std::numeric_limits<int>::max()) + 1}); });
  rejected([&] { detail::text(CordisAbiSlice{&byte, static_cast<std::size_t>(std::numeric_limits<std::ptrdiff_t>::max()) + 1}); });
  check(detail::text(CordisAbiSlice{nullptr, 0}).empty());
}
