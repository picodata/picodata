cmake_minimum_required(VERSION 2.8)

project(protocol C)

include(cmake/utils.cmake)

if (NOT PICODATA_TARGET_DIR)
    message(FATAL_ERROR
        "Specify the picodata target directory via PICODATA_TARGET_DIR.")
endif()

include(cmake/FindMsgPuck.cmake)
if (NOT MSGPUCK_FOUND)
    message(FATAL_ERROR
        "MsgPuck were not found.")
endif()

set(TARANTOOL_INCLUDE_DIRS
    "${PICODATA_TARGET_DIR}/build/tarantool-sys/tarantool-prefix/include/tarantool")
set(PICODATA_EXECUTABLE
    "${PICODATA_TARGET_DIR}/picodata")

# EXISTS works only with real pathes
get_filename_component(PICODATA_EXECUTABLE ${PICODATA_EXECUTABLE} REALPATH)
get_filename_component(TARANTOOL_INCLUDE_DIRS ${TARANTOOL_INCLUDE_DIRS} REALPATH)

if (NOT EXISTS "${PICODATA_EXECUTABLE}" OR NOT EXISTS "${TARANTOOL_INCLUDE_DIRS}/module.h")
    message(FATAL_ERROR
        "Can't find picodata executable or tarantool headers. Rebuild can fix this issue.")
endif()

include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${TARANTOOL_INCLUDE_DIRS})
include_directories(${MSGPUCK_INCLUDE_DIRS})

add_subdirectory(src)
add_subdirectory(test)
