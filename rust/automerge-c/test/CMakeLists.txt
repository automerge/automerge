cmake_minimum_required(VERSION 3.25 FATAL_ERROR)

find_package(cmocka CONFIG REQUIRED)

add_executable(
    ${LIBRARY_NAME}_test
        actor_id_tests.c
        base_state.c
        byte_span_tests.c
        cmocka_utils.c
        cursor_tests.c
        enum_string_tests.c
        doc_state.c
        doc_tests.c
        item_tests.c
        list_tests.c
        macro_utils.c
        main.c
        map_tests.c
        mark_tests.c
        str_utils.c
        ported_wasm/basic_tests.c
        ported_wasm/cursor_tests.c
        ported_wasm/suite.c
        ported_wasm/sync_tests.c
)

set_target_properties(${LIBRARY_NAME}_test PROPERTIES LINKER_LANGUAGE C)

if(WIN32)
    set(CMOCKA "cmocka::cmocka")
else()
    set(CMOCKA "cmocka")
endif()

target_link_libraries(${LIBRARY_NAME}_test PRIVATE ${CMOCKA} ${LIBRARY_NAME})

add_dependencies(${LIBRARY_NAME}_test ${BINDINGS_NAME}_artifacts)

add_custom_command(
    TARGET ${LIBRARY_NAME}_test
    POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory_if_different ${CMAKE_CURRENT_SOURCE_DIR}/files ${CMAKE_CURRENT_BINARY_DIR}/files
    COMMENT "Copying the test input files into the tests directory..."
)

if(BUILD_SHARED_LIBS AND WIN32)
    add_custom_command(
        TARGET ${LIBRARY_NAME}_test
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:${LIBRARY_NAME}> $<TARGET_FILE_DIR:${LIBRARY_NAME}_test>
        COMMENT "Copying the DLL into the tests directory..."
        VERBATIM
    )
endif()

add_test(NAME ${LIBRARY_NAME}_test COMMAND ${LIBRARY_NAME}_test)

add_custom_command(
    TARGET ${LIBRARY_NAME}_test
    POST_BUILD
    COMMAND
        ${CMAKE_CTEST_COMMAND} --build-config $<CONFIG> --output-on-failure
    COMMENT
        "Running the test(s)..."
    VERBATIM
)
