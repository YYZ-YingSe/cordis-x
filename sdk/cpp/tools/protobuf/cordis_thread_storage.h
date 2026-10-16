// Cordis macOS portability support for the pinned protobuf runtime.
// This file is installed next to protobuf headers; it is not a wire API.
#pragma once
#include <cstddef>
#include <type_traits>

namespace google::protobuf::internal {
using CordisThreadDelete = void (*)(void *);
std::size_t CordisRegisterThreadSlot(CordisThreadDelete destroy);
void *CordisThreadValue(std::size_t slot, void *(*create)(const void *),
                        const void *initial);
// Requires quiescence. Called after protobuf's ordinary shutdown callbacks.
void CordisShutdownThreadStorage();

// No thread-exit callback points into an unloadable plugin. Values belong to
// its protobuf runtime until ShutdownProtobufLibrary(), even when the foreign
// thread has already exited. Only trivially copyable cache/scalar types belong
// here; business objects must use their ordinary explicit lifecycle.
template <class T> class CordisThreadLocal {
  static_assert(std::is_trivially_copyable_v<T>);
  static_assert(std::is_trivially_destructible_v<T>);
  const T initial_;
  const std::size_t slot_;

public:
  explicit CordisThreadLocal(T initial = {})
      : initial_(initial), slot_(CordisRegisterThreadSlot(
                               [](void *p) { delete static_cast<T *>(p); })) {}
  T &get() const {
    return *static_cast<T *>(CordisThreadValue(
        slot_,
        [](const void *p) -> void * {
          return new T(*static_cast<const T *>(p));
        },
        &initial_));
  }
};
} // namespace google::protobuf::internal
