cmake_minimum_required(VERSION 3.18 FATAL_ERROR)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

# Parse the library name, project name and project version out of Cargo's TOML file.
set(CARGO_LIB_SECTION OFF)

set(LIBRARY_NAME "")

set(CARGO_PKG_SECTION OFF)

set(CARGO_PKG_NAME "")

set(CARGO_PKG_VERSION "")

file(READ Cargo.toml TOML_STRING)

string(REPLACE ";" "\\\\;" TOML_STRING "${TOML_STRING}")

string(REPLACE "\n" ";" TOML_LINES "${TOML_STRING}")

foreach(TOML_LINE IN ITEMS ${TOML_LINES})
    string(REGEX MATCH "^\\[(lib|package)\\]$" _ ${TOML_LINE})

    if(CMAKE_MATCH_1 STREQUAL "lib")
        set(CARGO_LIB_SECTION ON)

        set(CARGO_PKG_SECTION OFF)
    elseif(CMAKE_MATCH_1 STREQUAL "package")
        set(CARGO_LIB_SECTION OFF)

        set(CARGO_PKG_SECTION ON)
    endif()

    string(REGEX MATCH "^name += +\"([^\"]+)\"$" _ ${TOML_LINE})

    if(CMAKE_MATCH_1 AND (CARGO_LIB_SECTION AND NOT CARGO_PKG_SECTION))
        set(LIBRARY_NAME "${CMAKE_MATCH_1}")
    elseif(CMAKE_MATCH_1 AND (NOT CARGO_LIB_SECTION AND CARGO_PKG_SECTION))
        set(CARGO_PKG_NAME "${CMAKE_MATCH_1}")
    endif()

    string(REGEX MATCH "^version += +\"([^\"]+)\"$" _ ${TOML_LINE})

    if(CMAKE_MATCH_1 AND CARGO_PKG_SECTION)
        set(CARGO_PKG_VERSION "${CMAKE_MATCH_1}")
    endif()

    if(LIBRARY_NAME AND (CARGO_PKG_NAME AND CARGO_PKG_VERSION))
        break()
    endif()
endforeach()

project(${CARGO_PKG_NAME} VERSION 0.0.1 LANGUAGES C DESCRIPTION "C bindings for the Automerge Rust backend.")

include(CTest)

option(BUILD_SHARED_LIBS "Enable the choice of a shared or static library.")

include(CMakePackageConfigHelpers)

include(GNUInstallDirs)

string(MAKE_C_IDENTIFIER ${PROJECT_NAME} SYMBOL_PREFIX)

string(TOUPPER ${SYMBOL_PREFIX} SYMBOL_PREFIX)

set(CARGO_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/Cargo/target")

set(CBINDGEN_INCLUDEDIR "${CARGO_TARGET_DIR}/${CMAKE_INSTALL_INCLUDEDIR}")

set(CBINDGEN_TARGET_DIR "${CBINDGEN_INCLUDEDIR}/${PROJECT_NAME}")

add_subdirectory(src)

# Generate and install the configuration header.
math(EXPR INTEGER_PROJECT_VERSION_MAJOR "${PROJECT_VERSION_MAJOR} * 100000")

math(EXPR INTEGER_PROJECT_VERSION_MINOR "${PROJECT_VERSION_MINOR} * 100")

math(EXPR INTEGER_PROJECT_VERSION_PATCH "${PROJECT_VERSION_PATCH}")

math(EXPR INTEGER_PROJECT_VERSION "${INTEGER_PROJECT_VERSION_MAJOR} + ${INTEGER_PROJECT_VERSION_MINOR} + ${INTEGER_PROJECT_VERSION_PATCH}")

configure_file(
    ${CMAKE_MODULE_PATH}/config.h.in
    config.h
    @ONLY
    NEWLINE_STYLE LF
)

install(
    FILES ${CMAKE_BINARY_DIR}/config.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME}
)

if(BUILD_TESTING)
    add_subdirectory(test EXCLUDE_FROM_ALL)

    enable_testing()
endif()

add_subdirectory(examples EXCLUDE_FROM_ALL)

# Generate and install .cmake files
set(PROJECT_CONFIG_NAME "${PROJECT_NAME}-config")

set(PROJECT_CONFIG_VERSION_NAME "${PROJECT_CONFIG_NAME}-version")

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_CONFIG_VERSION_NAME}.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY ExactVersion
)

# The namespace label starts with the title-cased library name.
string(SUBSTRING ${LIBRARY_NAME} 0 1 NS_FIRST)

string(SUBSTRING ${LIBRARY_NAME} 1 -1 NS_REST)

string(TOUPPER ${NS_FIRST} NS_FIRST)

string(TOLOWER ${NS_REST} NS_REST)

string(CONCAT NAMESPACE ${NS_FIRST} ${NS_REST} "::")

# \note CMake doesn't automate the exporting of an imported library's targets
#       so the package configuration script must do it.
configure_package_config_file(
    ${CMAKE_MODULE_PATH}/${PROJECT_CONFIG_NAME}.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_CONFIG_NAME}.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}
)

install(
    FILES
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_CONFIG_NAME}.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_CONFIG_VERSION_NAME}.cmake
    DESTINATION
        ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}
)
