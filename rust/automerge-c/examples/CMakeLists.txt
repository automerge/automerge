cmake_minimum_required(VERSION 3.25 FATAL_ERROR)

add_executable(
    ${LIBRARY_NAME}_quickstart
        quickstart.c
)

set_target_properties(${LIBRARY_NAME}_quickstart PROPERTIES LINKER_LANGUAGE C)

# \note An imported library's INTERFACE_INCLUDE_DIRECTORIES property can't
#       contain a non-existent path so its build-time include directory
#       must be specified for all of its dependent targets instead.
target_include_directories(
    ${LIBRARY_NAME}_quickstart
    PRIVATE "$<BUILD_INTERFACE:${CBINDGEN_INCLUDEDIR}>"
)

target_link_libraries(${LIBRARY_NAME}_quickstart PRIVATE ${LIBRARY_NAME})

add_dependencies(${LIBRARY_NAME}_quickstart ${BINDINGS_NAME}_artifacts)

if(BUILD_SHARED_LIBS AND WIN32)
    add_custom_command(
        TARGET ${LIBRARY_NAME}_quickstart
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                ${CARGO_CURRENT_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}${LIBRARY_NAME}${CMAKE_${CMAKE_BUILD_TYPE}_POSTFIX}${CMAKE_SHARED_LIBRARY_SUFFIX}
                ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Copying the DLL built by Cargo into the examples directory..."
        VERBATIM
    )
endif()

add_custom_command(
    TARGET ${LIBRARY_NAME}_quickstart
    POST_BUILD
    COMMAND
        ${LIBRARY_NAME}_quickstart
    COMMENT
        "Running the example quickstart..."
    VERBATIM
)
