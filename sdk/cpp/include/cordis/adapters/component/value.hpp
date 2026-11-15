#pragma once
#include <cordis/cordis.hpp>
#include <cordis/protocols/session/binary.hpp>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <managed_plugin.h>
namespace cordis::component::detail {
using RawPayload = cordis_plugin_types_payload_t;
using Payload = cordis::session::Payload;
struct Unknown : std::runtime_error {
  using std::runtime_error::runtime_error;
};
inline std::string text(managed_plugin_string_t s) {
  return s.len ? std::string(reinterpret_cast<const char *>(s.ptr), s.len)
               : std::string{};
}
inline managed_plugin_string_t borrowed(const std::string &s) {
  return {reinterpret_cast<uint8_t *>(const_cast<char *>(s.data())), s.size()};
}
template <class T> T *allocate(std::size_t count) {
  if (!count)
    return nullptr;
  if (count > static_cast<std::size_t>(std::numeric_limits<std::ptrdiff_t>::max()) / sizeof(T))
    throw std::runtime_error("allocation size is not representable");
  auto *p = static_cast<T *>(std::malloc(count * sizeof(T)));
  if (!p)
    throw std::bad_alloc();
  return p;
}
inline managed_plugin_string_t owned(const std::string &s) {
  auto *p = allocate<uint8_t>(s.size());
  if (!s.empty())
    std::memcpy(p, s.data(), s.size());
  return {p, s.size()};
}
class Buffer {
  std::vector<uint8_t> bytes_;

public:
  explicit Buffer(const Payload &value) : bytes_(session::encode(value)) {}
  RawPayload view() { return {bytes_.data(), bytes_.size()}; }
  RawPayload release() {
    auto *p = allocate<uint8_t>(bytes_.size());
    if (!bytes_.empty())
      std::memcpy(p, bytes_.data(), bytes_.size());
    return {p, bytes_.size()};
  }
};
inline Payload decode(const RawPayload &raw, Validation validation = Validation::Checked) {
  if (raw.len && !raw.ptr)
    throw Refusal("invalid_payload", "invalid byte list");
  try {
    return session::decode_payload(raw.ptr, raw.len, validation);
  } catch (const std::exception &e) {
    throw Refusal("invalid_payload", e.what());
  }
}
inline cordis_plugin_types_failure_t failure(std::exception_ptr e) {
  bool uncertain = false, refused = false;
  std::string code = "component", message = "unknown exception";
  try {
    std::rethrow_exception(e);
  } catch (const Refusal &e) {
    refused = true;
    code = e.code;
    message = e.what();
  } catch (const RpcError &e) {
    code = e.code;
    message = e.what();
  } catch (const std::exception &e) {
    uncertain = true;
    message = e.what();
  } catch (...) {
    uncertain = true;
  }
  cordis_plugin_types_failure_t f{};
  f.tag = uncertain ? 1 : refused ? 2 : 0;
  const auto raw_code = owned(code);
  try {
    f.val.rejected = {raw_code, owned(message)};
  } catch (...) {
    std::free(raw_code.ptr);
    throw;
  }
  return f;
}
inline void raise(cordis_plugin_types_failure_t &failure) {
  auto code = text(failure.val.rejected.code),
       message = text(failure.val.rejected.message);
  bool refused = failure.tag == 2;
  bool uncertain = failure.tag != 0 && !refused;
  cordis_plugin_types_failure_free(&failure);
  if (uncertain)
    throw Unknown(code + ": " + message);
  if (refused)
    throw Refusal(code, message);
  throw RpcError(code, message);
}
} // namespace cordis::component::detail
