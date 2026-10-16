cmake_minimum_required(VERSION 3.20)
project(cordis_setup_protobuf LANGUAGES C CXX)

# Only the explicit setup command uses this wrapper. SDK CMake never patches or
# installs external dependencies. Keep full/lite runtimes consistent on macOS.
add_subdirectory("${CORDIS_PROTOBUF_SOURCE}" protobuf)
set(adaptation "${CMAKE_CURRENT_LIST_DIR}/../../../sdk/cpp/tools/protobuf")
foreach(runtime libprotobuf libprotobuf-lite)
  target_sources(${runtime} PRIVATE "${adaptation}/cordis_thread_storage.cc")
endforeach()
install(FILES "${adaptation}/cordis_thread_storage.h"
        DESTINATION include/google/protobuf)
