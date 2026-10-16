cmake_minimum_required(VERSION 3.20)
project(cordis_component_consumer LANGUAGES C CXX)
add_subdirectory("${CORDIS_SDK_SOURCE}" cordis-sdk)
set(core)
if(CORDIS_CORE)
  set(core CORE)
endif()
cordis_add_component(plugin ${core}
  OUTPUT "${CORDIS_OUTPUT}" SOURCES "${CORDIS_SOURCE}"
  PROTOS ${CORDIS_PROTOS} PROTO_PATHS ${CORDIS_PROTO_PATHS}
  PROTOC "${CORDIS_PROTOC}" WIT_BINDGEN "${CORDIS_WIT_BINDGEN}" PACK "${CORDIS_COMPONENT_PACK}")
target_include_directories(plugin PRIVATE ${CORDIS_INCLUDE_DIRS})
