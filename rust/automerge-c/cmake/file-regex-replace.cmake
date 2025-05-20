# This CMake script is used to perform string substitutions within a generated
# file.
cmake_minimum_required(VERSION 3.25 FATAL_ERROR)

if(NOT DEFINED MATCH_REGEX)
    message(FATAL_ERROR "Variable \"MATCH_REGEX\" is not defined.")
elseif(NOT DEFINED REPLACE_EXPR)
    message(FATAL_ERROR "Variable \"REPLACE_EXPR\" is not defined.")
elseif(${CMAKE_ARGC} LESS 7)
    message(FATAL_ERROR "Too few arguments.")
elseif(${CMAKE_ARGC} GREATER 8)
    message(FATAL_ERROR "Too many arguments.")
elseif(NOT EXISTS ${CMAKE_ARGV6})
    message(FATAL_ERROR "Input file \"${CMAKE_ARGV6}\" not found.")
endif()

message(STATUS "Replacing \"${MATCH_REGEX}\" with \"${REPLACE_EXPR}\" in \"${CMAKE_ARGV6}\"...")

file(READ ${CMAKE_ARGV6} INPUT_STRING)

string(REGEX REPLACE "${MATCH_REGEX}" "${REPLACE_EXPR}" OUTPUT_STRING "${INPUT_STRING}")

if(DEFINED CMAKE_ARGV7)
    set(OUTPUT_FILE "${CMAKE_ARGV7}")
else()
    set(OUTPUT_FILE "${CMAKE_ARGV6}")
endif()

if(NOT "${OUTPUT_STRING}" STREQUAL "${INPUT_STRING}")
    file(WRITE ${OUTPUT_FILE} "${OUTPUT_STRING}")

    message(STATUS "Created/updated \"${OUTPUT_FILE}\".")
endif()
