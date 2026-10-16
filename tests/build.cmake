# This build owns the concrete generated locations for tests.
set(CORDIS_TEST_GENERATED "${CMAKE_CURRENT_LIST_DIR}/generated")
include_directories("${CORDIS_TEST_GENERATED}/cpp")
