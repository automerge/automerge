cmake_minimum_required(VERSION 3.18 FATAL_ERROR)

find_package(cmocka REQUIRED)

add_executable(
    test_${LIBRARY_NAME}
        actor_id_tests.c
        doc_tests.c
        group_state.c
        list_tests.c
        macro_utils.c
        main.c
        map_tests.c
        str_utils.c
        sync_tests.c
)

set_target_properties(test_${LIBRARY_NAME} PROPERTIES LINKER_LANGUAGE C)

# \note An imported library's INTERFACE_INCLUDE_DIRECTORIES property can't
#       contain a non-existent path so its build-time include directory
#       must be specified for all of its dependent targets instead.
target_include_directories(
    test_${LIBRARY_NAME}
    PRIVATE "$<BUILD_INTERFACE:${CARGO_TARGET_DIR}>"
)

target_link_libraries(test_${LIBRARY_NAME} PRIVATE cmocka ${LIBRARY_NAME})

add_dependencies(test_${LIBRARY_NAME} ${LIBRARY_NAME}_artifacts)

if(BUILD_SHARED_LIBS AND WIN32)
    add_custom_command(
        TARGET test_${LIBRARY_NAME}
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                ${CARGO_CURRENT_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}${LIBRARY_NAME}${CMAKE_${CMAKE_BUILD_TYPE}_POSTFIX}${CMAKE_SHARED_LIBRARY_SUFFIX}
                ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Copying the DLL built by Cargo into the test directory..."
        VERBATIM
    )
endif()

add_test(NAME test_${LIBRARY_NAME} COMMAND test_${LIBRARY_NAME})

add_custom_command(
    TARGET test_${LIBRARY_NAME}
    POST_BUILD
    COMMAND
        ${CMAKE_CTEST_COMMAND} --config $<CONFIG> --output-on-failure
    COMMENT
        "Running the test(s)..."
    VERBATIM
)
