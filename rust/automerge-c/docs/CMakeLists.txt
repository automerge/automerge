cmake_minimum_required(VERSION 3.25 FATAL_ERROR)

find_package(Doxygen OPTIONAL_COMPONENTS dot)

if(DOXYGEN_FOUND)
    set(DOXYGEN_ALIASES "installed_headerfile=\\headerfile ${LIBRARY_NAME}.h <${PROJECT_NAME}/${LIBRARY_NAME}.h>")

    set(DOXYGEN_GENERATE_LATEX YES)

    set(DOXYGEN_PDF_HYPERLINKS YES)

    set(DOXYGEN_PREDEFINED "DOXYGEN")

    set(DOXYGEN_PROJECT_LOGO "${CMAKE_CURRENT_SOURCE_DIR}/img/brandmark.png")

    set(DOXYGEN_SORT_BRIEF_DOCS YES)

    set(DOXYGEN_USE_MDFILE_AS_MAINPAGE "${PROJECT_SOURCE_DIR}/README.md")

    doxygen_add_docs(
        ${LIBRARY_NAME}_docs
        "${CBINDGEN_TARGET_DIR}/${LIBRARY_NAME}.h"
        "${CBINDGEN_TARGET_DIR}/config.h"
        "${CBINDGEN_TARGET_DIR}/${UTILS_SUBDIR}/enum_string.h"
        "${PROJECT_SOURCE_DIR}/${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME}/${UTILS_SUBDIR}/result.h"
        "${PROJECT_SOURCE_DIR}/${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME}/${UTILS_SUBDIR}/stack_callback_data.h"
        "${PROJECT_SOURCE_DIR}/${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME}/${UTILS_SUBDIR}/stack.h"
        "${PROJECT_SOURCE_DIR}/${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME}/${UTILS_SUBDIR}/string.h"
        "${PROJECT_SOURCE_DIR}/README.md"
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        COMMENT "Producing documentation with Doxygen..."
    )

    # \note A Doxygen input file isn't a file-level dependency so the Doxygen
    #       command must instead depend upon a target that either outputs the
    #       file or depends upon it also or it will just output an error message
    #       when it can't be found.
    add_dependencies(${LIBRARY_NAME}_docs ${BINDINGS_NAME}_artifacts ${LIBRARY_NAME}_utilities)
endif()
