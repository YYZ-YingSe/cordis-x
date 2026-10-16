cmake_minimum_required(VERSION 3.20)
project(cordis_sanitizer LANGUAGES C CXX)

# Protobuf annotates its containers under ASan. Instrument its mutation paths
# as well as generated code; mixing instrumented headers with an uninstrumented
# runtime leaves container annotations inconsistent.
add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
add_link_options(-fsanitize=address,undefined)
# Supply an externally prepared Protobuf/Abseil prefix built with the same
# sanitizer flags through CMAKE_PREFIX_PATH. Never rebuild dependencies here.
set(CORDIS_BUILD_EXAMPLES OFF CACHE BOOL "")
set(CORDIS_BUILD_ABI OFF CACHE BOOL "")
set(CORDIS_BUILD_PROCESS OFF CACHE BOOL "")
set(BUILD_TESTING ON CACHE BOOL "")
add_subdirectory(../.. sdk)
