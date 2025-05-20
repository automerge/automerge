# This CMake script is used to force Cargo to regenerate the header file for the
# core bindings after the out-of-source build directory has been cleaned.
cmake_minimum_required(VERSION 3.25 FATAL_ERROR)

if(NOT DEFINED CONDITION)
    message(FATAL_ERROR "Variable \"CONDITION\" is not defined.")
elseif(${CMAKE_ARGC} LESS 7)
    message(FATAL_ERROR "Too few arguments.")
elseif(${CMAKE_ARGC} GREATER 7)
    message(FATAL_ERROR "Too many arguments.")
elseif(NOT EXISTS ${CMAKE_ARGV6})
    message(FATAL_ERROR "File \"${CMAKE_ARGV6}\" not found.")
elseif(IS_DIRECTORY "${CMAKE_ARG6}")
    message(FATAL_ERROR "Directory \"${CMAKE_ARG6}\" can't be touched.")
endif()

message(STATUS "Touching \"${CMAKE_ARGV6}\" if ${CONDITION} \"${CMAKE_ARGV5}\"...")

if(CONDITION STREQUAL "EXISTS")
    if(EXISTS "${CMAKE_ARGV5}")
        set(DO_IT TRUE)
    endif()
elseif((CONDITION STREQUAL "NOT_EXISTS") OR (CONDITION STREQUAL "!EXISTS"))
    if(NOT EXISTS "${CMAKE_ARGV5}")
        set(DO_IT TRUE)
    endif()
else()
    message(FATAL_ERROR "Unexpected condition \"${CONDITION}\".")
endif()

if(DO_IT)
    file(TOUCH_NOCREATE "${CMAKE_ARGV6}")

    message(STATUS "Touched \"${CMAKE_ARGV6}\".")
endif()
