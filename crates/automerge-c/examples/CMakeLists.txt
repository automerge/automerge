cmake_minimum_required(VERSION 3.18 FATAL_ERROR)

add_executable(
    example_quickstart
        quickstart.c
)

set_target_properties(example_quickstart PROPERTIES LINKER_LANGUAGE C)

# \note An imported library's INTERFACE_INCLUDE_DIRECTORIES property can't
#       contain a non-existent path so its build-time include directory
#       must be specified for all of its dependent targets instead.
target_include_directories(
    example_quickstart
    PRIVATE "$<BUILD_INTERFACE:${CBINDGEN_INCLUDEDIR}>"
)

target_link_libraries(example_quickstart PRIVATE ${LIBRARY_NAME})

add_dependencies(example_quickstart ${LIBRARY_NAME}_artifacts)

if(BUILD_SHARED_LIBS AND WIN32)
    add_custom_command(
        TARGET example_quickstart
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                ${CARGO_CURRENT_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}${LIBRARY_NAME}${CMAKE_${CMAKE_BUILD_TYPE}_POSTFIX}${CMAKE_SHARED_LIBRARY_SUFFIX}
                ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Copying the DLL built by Cargo into the examples directory..."
        VERBATIM
    )
endif()

add_custom_command(
    TARGET example_quickstart
    POST_BUILD
    COMMAND
        example_quickstart
    COMMENT
        "Running the example quickstart..."
    VERBATIM
)
