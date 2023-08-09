cmake_minimum_required(VERSION 2.8)

project(protocol C)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
set(CMAKE_INCLUDE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_INCLUDE_PATH})

include(utils)

# It's not really a target dir, it's `target/debug` or something...
# TODO: rename this variable to PICODATA_BUILD_DIR
if (NOT PICODATA_TARGET_DIR)
    message(FATAL_ERROR "PICODATA_TARGET_DIR not set")
endif()

set(TARANTOOL_PREFIX "${PICODATA_TARGET_DIR}/build/tarantool-sys/tarantool-prefix")
set(TARANTOOL_INCLUDE_DIRS "${TARANTOOL_PREFIX}/include/tarantool")

# Set search hints and find the MsgPuck library
list(APPEND MSGPUCK_ROOT_DIR "${PICODATA_TARGET_DIR}/../../tarantool-sys/src/lib/msgpuck")
list(APPEND MSGPUCK_ROOT_DIR "${TARANTOOL_PREFIX}/src/tarantool-build/src/lib/msgpuck")
find_package(MsgPuck REQUIRED)

# All the necessary prerequisites should be available if we've found the executable
set(PICODATA_EXECUTABLE "${PICODATA_TARGET_DIR}/picodata")
get_filename_component(PICODATA_EXECUTABLE ${PICODATA_EXECUTABLE} REALPATH)
if (NOT EXISTS "${PICODATA_EXECUTABLE}")
    message(FATAL_ERROR "Can't find picodata executable at ${PICODATA_EXECUTABLE}")
endif()

include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${TARANTOOL_INCLUDE_DIRS})
include_directories(${MSGPUCK_INCLUDE_DIRS})

add_subdirectory(src)
add_subdirectory(test)
