#include <google/protobuf/stubs/common.h>

// The ABI contains values and function tables, never protobuf C++ objects.
// Each plugin statically links a private protobuf runtime. Reclaim its registry
// and module-owned thread cells when the drained plugin is unloaded, after
// ordinary C++ static objects. macOS Abseil is a shared process-lifetime
// dependency: its foreign-thread destructors never point into this plugin.
namespace {
struct ProtobufLifetime {
  ~ProtobufLifetime() { google::protobuf::ShutdownProtobufLibrary(); }
};
#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4073)
#pragma init_seg(lib)
ProtobufLifetime protobuf_lifetime;
#pragma warning(pop)
#else
__attribute__((init_priority(101))) ProtobufLifetime protobuf_lifetime;
#endif
} // namespace
