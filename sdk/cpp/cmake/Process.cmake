add_executable(cordis_calculator examples/calculator.cpp)
target_link_libraries(cordis_calculator PRIVATE cordis Threads::Threads)
target_compile_options(cordis_calculator PRIVATE -Wall -Wextra -Wpedantic -Werror)

add_executable(cordis_calculator_v2 examples/calculator.cpp)
target_link_libraries(cordis_calculator_v2 PRIVATE cordis Threads::Threads)
target_compile_definitions(cordis_calculator_v2 PRIVATE CORDIS_PROCESS_CODE_VERSION="v2")
target_compile_options(cordis_calculator_v2 PRIVATE -Wall -Wextra -Wpedantic -Werror)

add_executable(cordis_adder examples/adder.cpp)
target_link_libraries(cordis_adder PRIVATE cordis Threads::Threads)
target_compile_options(cordis_adder PRIVATE -Wall -Wextra -Wpedantic -Werror)

add_executable(cordis_pricing examples/pricing.cpp)
target_link_libraries(cordis_pricing PRIVATE cordis Threads::Threads)
target_compile_options(cordis_pricing PRIVATE -Wall -Wextra -Wpedantic -Werror)

add_executable(cordis_subscriptions examples/subscriptions.cpp)
target_link_libraries(cordis_subscriptions PRIVATE cordis Threads::Threads)
target_compile_options(cordis_subscriptions PRIVATE -Wall -Wextra -Wpedantic -Werror)

add_executable(cordis_sequence examples/sequence.cpp)
target_link_libraries(cordis_sequence PRIVATE cordis Threads::Threads)
target_compile_options(cordis_sequence PRIVATE -Wall -Wextra -Wpedantic -Werror)

add_executable(cordis_scopes examples/scopes.cpp)
target_link_libraries(cordis_scopes PRIVATE cordis Threads::Threads)
target_compile_options(cordis_scopes PRIVATE -Wall -Wextra -Wpedantic -Werror)

add_executable(cordis_tasks examples/tasks.cpp)
target_link_libraries(cordis_tasks PRIVATE cordis Threads::Threads)
target_compile_options(cordis_tasks PRIVATE -Wall -Wextra -Wpedantic -Werror)

add_executable(cordis_events examples/events.cpp)
target_link_libraries(cordis_events PRIVATE cordis Threads::Threads)
target_compile_options(cordis_events PRIVATE -Wall -Wextra -Wpedantic -Werror)

add_executable(cordis_middleware examples/middleware.cpp)
target_link_libraries(cordis_middleware PRIVATE cordis Threads::Threads)
target_compile_options(cordis_middleware PRIVATE -Wall -Wextra -Wpedantic -Werror)

add_executable(cordis_composition examples/composition.cpp)
target_link_libraries(cordis_composition PRIVATE cordis Threads::Threads)
target_compile_options(cordis_composition PRIVATE -Wall -Wextra -Wpedantic -Werror)

if(BUILD_TESTING)
  add_executable(cordis_wire_test tests/wire.cpp)
  target_link_libraries(cordis_wire_test PRIVATE cordis Threads::Threads)
  target_compile_options(cordis_wire_test PRIVATE -Wall -Wextra -Wpedantic -Werror)
  add_test(NAME cordis_wire COMMAND cordis_wire_test)
  add_executable(cordis_middleware_test tests/middleware.cpp)
  target_link_libraries(cordis_middleware_test PRIVATE cordis Threads::Threads)
  target_compile_options(cordis_middleware_test PRIVATE -Wall -Wextra -Wpedantic -Werror)
  add_test(NAME cordis_middleware COMMAND cordis_middleware_test)
  add_executable(cordis_context_test tests/context.cpp)
  target_link_libraries(cordis_context_test PRIVATE cordis Threads::Threads)
  target_compile_options(cordis_context_test PRIVATE -Wall -Wextra -Wpedantic -Werror)
  add_test(NAME cordis_context COMMAND cordis_context_test)
  add_executable(cordis_effects_test tests/effects.cpp)
  target_link_libraries(cordis_effects_test PRIVATE cordis Threads::Threads)
  target_compile_options(cordis_effects_test PRIVATE -Wall -Wextra -Wpedantic -Werror)
  add_test(NAME cordis_effects COMMAND cordis_effects_test)
endif()

include(${CMAKE_CURRENT_LIST_DIR}/Package.cmake)
foreach(generation v1 v2)
  add_library(cordis_process_leaf_${generation} SHARED tests/package/helper.cpp)
  target_compile_definitions(cordis_process_leaf_${generation} PRIVATE
    CORDIS_BUILD_HELPER=1 CORDIS_HELPER_SYMBOL=cordis_process_leaf_${generation}
    CORDIS_HELPER_VERSION="${generation}")
  set_target_properties(cordis_process_leaf_${generation} PROPERTIES CXX_VISIBILITY_PRESET hidden)
  add_library(cordis_process_helper_${generation} SHARED tests/package/helper.cpp)
  target_compile_definitions(cordis_process_helper_${generation} PRIVATE
    CORDIS_BUILD_HELPER=1 CORDIS_HELPER_SYMBOL=cordis_process_helper_${generation}
    CORDIS_HELPER_VERSION="${generation}")
  target_compile_definitions(cordis_process_helper_${generation} PRIVATE CORDIS_HELPER_CHILD=cordis_process_leaf_${generation})
  target_link_libraries(cordis_process_helper_${generation} PRIVATE cordis_process_leaf_${generation})
  set_target_properties(cordis_process_helper_${generation} PROPERTIES CXX_VISIBILITY_PRESET hidden)
  add_executable(cordis_package_process_${generation} examples/calculator.cpp)
  target_link_libraries(cordis_package_process_${generation} PRIVATE cordis::process)
  target_compile_definitions(cordis_package_process_${generation} PRIVATE
    CORDIS_PACKAGE_HELPER=1 CORDIS_HELPER_SYMBOL=cordis_process_helper_${generation})
  cordis_package(cordis_package_process_${generation} GENERATION ${generation}
    PRIVATE_LIBRARIES cordis_process_helper_${generation} cordis_process_leaf_${generation})
endforeach()
