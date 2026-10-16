# Explicit private targets form the package boundary. CMake links and installs
# them; cordis-package inspects actual imports and writes the content manifest.
# GENERATION must change whenever private code changes. It is a build identity,
# not an API/protocol version. No runtime rewriting of accepted bytes is needed.
function(cordis_package target)
  cmake_parse_arguments(PKG "" "GENERATION" "PRIVATE_LIBRARIES" ${ARGN})
  if(NOT PKG_GENERATION MATCHES "^[A-Za-z0-9_]+$")
    message(FATAL_ERROR "cordis_package requires an alphanumeric GENERATION")
  endif()
  foreach(lib IN LISTS PKG_PRIVATE_LIBRARIES)
    set_target_properties(${lib} PROPERTIES OUTPUT_NAME "${lib}_${PKG_GENERATION}")
    if(APPLE)
      set_target_properties(${lib} PROPERTIES MACOSX_RPATH OFF
        INSTALL_NAME_DIR "@loader_path" BUILD_WITH_INSTALL_NAME_DIR YES)
    endif()
    if(UNIX AND NOT APPLE)
      target_link_options(${lib} PRIVATE "LINKER:-Bsymbolic")
      set_target_properties(${lib} PROPERTIES BUILD_RPATH "$ORIGIN" INSTALL_RPATH "$ORIGIN")
    endif()
  endforeach()
  target_link_libraries(${target} PRIVATE ${PKG_PRIVATE_LIBRARIES})
  if(UNIX AND NOT APPLE)
    set_target_properties(${target} PROPERTIES BUILD_RPATH "$ORIGIN" INSTALL_RPATH "$ORIGIN")
  endif()
  # Host shared runtimes retain their explicit dependency RPATH in the plugin package.
  set_target_properties(${target} PROPERTIES INSTALL_RPATH_USE_LINK_PATH YES)
  install(TARGETS ${target} ${PKG_PRIVATE_LIBRARIES}
    RUNTIME DESTINATION . COMPONENT "cordis-${target}"
    LIBRARY DESTINATION . COMPONENT "cordis-${target}")
endfunction()
