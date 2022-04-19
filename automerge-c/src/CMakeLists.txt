cmake_minimum_required(VERSION 3.18 FATAL_ERROR)

find_program (
    CARGO_CMD
    "cargo"
    PATHS "$ENV{CARGO_HOME}/bin"
    DOC "The Cargo command"
)

if(NOT CARGO_CMD)
    message(FATAL_ERROR "Cargo (Rust package manager) not found! Install it and/or set the CARGO_HOME environment variable.")
endif()

string(TOLOWER "${CMAKE_BUILD_TYPE}" BUILD_TYPE_LOWER)

if(BUILD_TYPE_LOWER STREQUAL debug)
    set(CARGO_BUILD_TYPE "debug")

    set(CARGO_FLAG "")
else()
    set(CARGO_BUILD_TYPE "release")

    set(CARGO_FLAG "--release")
endif()

set(CARGO_CURRENT_BINARY_DIR "${CARGO_TARGET_DIR}/${CARGO_BUILD_TYPE}")

set(
    CARGO_OUTPUT
        # \note cbindgen won't regenerate a missing header so it can't be cleaned.
        #${CARGO_TARGET_DIR}/${LIBRARY_NAME}.h
        ${CARGO_CURRENT_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}${LIBRARY_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${CARGO_CURRENT_BINARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${LIBRARY_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}
)

if(WIN32)
    # \note The basename of an import library output by Cargo is the filename
    #       of its corresponding shared library.
    list(APPEND CARGO_OUTPUT ${CARGO_CURRENT_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}${LIBRARY_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()

add_custom_command(
    OUTPUT ${CARGO_OUTPUT}
    COMMAND
        ${CMAKE_COMMAND} -E env CARGO_TARGET_DIR=${CARGO_TARGET_DIR} ${CARGO_CMD} build ${CARGO_FLAG}
    MAIN_DEPENDENCY
        lib.rs
    DEPENDS
        ${CMAKE_SOURCE_DIR}/build.rs
        ${CMAKE_SOURCE_DIR}/Cargo.toml
        ${CMAKE_SOURCE_DIR}/cbindgen.toml
    WORKING_DIRECTORY
        ${CMAKE_SOURCE_DIR}
    COMMENT
        "Producing the library artifacts with Cargo..."
    VERBATIM
)

# \note This target is only necessary because cbindgen won't allow the
#       generated header to be listed in the Cargo command's output, being
#       another target's source file would've been enough otherwise.
add_custom_target(
    ${LIBRARY_NAME}_artifacts
    DEPENDS ${CARGO_OUTPUT}
)

if(BUILD_SHARED_LIBS)
    if(WIN32)
        set(LIBRARY_DESTINATION "${CMAKE_INSTALL_BINDIR}")
    else()
        set(LIBRARY_DESTINATION "${CMAKE_INSTALL_LIBDIR}")
    endif()

    set(LIBRARY_DEFINE_SYMBOL "${SYMBOL_PREFIX}_EXPORTS")

    # \note The basename of an import library output by Cargo is the filename
    #       of its corresponding shared library.
    set(LIBRARY_IMPLIB "${CARGO_CURRENT_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}${LIBRARY_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}")

    set(LIBRARY_LOCATION "${CARGO_CURRENT_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}${LIBRARY_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}")

    set(LIBRARY_NO_SONAME "${WIN32}")

    set(LIBRARY_SONAME "${CMAKE_SHARED_LIBRARY_PREFIX}${LIBRARY_NAME}${CMAKE_${CMAKE_BUILD_TYPE}_POSTFIX}${CMAKE_SHARED_LIBRARY_SUFFIX}")

    set(LIBRARY_TYPE "SHARED")
else()
    set(LIBRARY_DEFINE_SYMBOL "")

    set(LIBRARY_DESTINATION "${CMAKE_INSTALL_LIBDIR}")

    set(LIBRARY_IMPLIB "")

    set(LIBRARY_LOCATION "${CARGO_CURRENT_BINARY_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}${LIBRARY_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")

    set(LIBRARY_NO_SONAME "TRUE")

    set(LIBRARY_SONAME "")

    set(LIBRARY_TYPE "STATIC")
endif()

add_library(${LIBRARY_NAME} ${LIBRARY_TYPE} IMPORTED GLOBAL)

add_dependencies(${LIBRARY_NAME} ${LIBRARY_NAME}_artifacts)

set_target_properties(
    ${LIBRARY_NAME}
    PROPERTIES
        # \note Cargo writes a debug build into a nested directory instead of
        #       decorating its name.
        DEBUG_POSTFIX ""
        DEFINE_SYMBOL "${LIBRARY_DEFINE_SYMBOL}"
        IMPORTED_IMPLIB "${LIBRARY_IMPLIB}"
        IMPORTED_LOCATION "${LIBRARY_LOCATION}"
        IMPORTED_NO_SONAME "${LIBRARY_NO_SONAME}"
        IMPORTED_SONAME "${LIBRARY_SONAME}"
        LINKER_LANGUAGE C
        PUBLIC_HEADER "${CARGO_TARGET_DIR}/${LIBRARY_NAME}.h"
        SOVERSION "${PROJECT_VERSION_MAJOR}"
        VERSION "${PROJECT_VERSION}"
        # \note Cargo exports all of the symbols automatically.
        WINDOWS_EXPORT_ALL_SYMBOLS "TRUE"
)

target_compile_definitions(${LIBRARY_NAME} INTERFACE $<TARGET_PROPERTY:${LIBRARY_NAME},DEFINE_SYMBOL>)

target_include_directories(
    ${LIBRARY_NAME}
    INTERFACE
        "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME}>"
)

set(CMAKE_THREAD_PREFER_PTHREAD TRUE)

set(THREADS_PREFER_PTHREAD_FLAG TRUE)

find_package(Threads REQUIRED)

set(LIBRARY_DEPENDENCIES Threads::Threads ${CMAKE_DL_LIBS})

if(WIN32)
    list(APPEND LIBRARY_DEPENDENCIES Bcrypt userenv ws2_32)
else()
    list(APPEND LIBRARY_DEPENDENCIES m)
endif()

target_link_libraries(${LIBRARY_NAME} INTERFACE ${LIBRARY_DEPENDENCIES})

install(
    FILES $<TARGET_PROPERTY:${LIBRARY_NAME},IMPORTED_IMPLIB>
    TYPE LIB
    # \note The basename of an import library output by Cargo is the filename
    #       of its corresponding shared library.
    RENAME "${CMAKE_STATIC_LIBRARY_PREFIX}${LIBRARY_NAME}${CMAKE_${CMAKE_BUILD_TYPE}_POSTFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}"
    OPTIONAL
)

set(LIBRARY_FILE_NAME "${CMAKE_${LIBRARY_TYPE}_LIBRARY_PREFIX}${LIBRARY_NAME}${CMAKE_${CMAKE_BUILD_TYPE}_POSTFIX}${CMAKE_${LIBRARY_TYPE}_LIBRARY_SUFFIX}")

install(
    FILES $<TARGET_PROPERTY:${LIBRARY_NAME},IMPORTED_LOCATION>
    RENAME "${LIBRARY_FILE_NAME}"
    DESTINATION ${LIBRARY_DESTINATION}
)

install(
    FILES $<TARGET_PROPERTY:${LIBRARY_NAME},PUBLIC_HEADER>
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME}
)

find_package(Doxygen OPTIONAL_COMPONENTS dot)

if(DOXYGEN_FOUND)
    set(DOXYGEN_GENERATE_LATEX YES)

    set(DOXYGEN_PDF_HYPERLINKS YES)

    set(DOXYGEN_PROJECT_LOGO "${CMAKE_SOURCE_DIR}/img/brandmark.png")

    set(DOXYGEN_SORT_BRIEF_DOCS YES)

    set(DOXYGEN_USE_MDFILE_AS_MAINPAGE "${CMAKE_SOURCE_DIR}/README.md")

    doxygen_add_docs(
        ${LIBRARY_NAME}_docs
        "${CARGO_TARGET_DIR}/${LIBRARY_NAME}.h"
        "${CMAKE_SOURCE_DIR}/README.md"
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Producing documentation with Doxygen..."
    )

    add_dependencies(${LIBRARY_NAME}_docs ${LIBRARY_NAME}_artifacts)
endif()
