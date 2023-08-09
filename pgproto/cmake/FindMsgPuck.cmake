# - Find libmsgpuck header-only library
# The module defines the following variables:
#
#  MSGPUCK_FOUND - true if MsgPuck was found
#  MSGPUCK_INCLUDE_DIRS - the directory of the MsgPuck headers
#  MSGPUCK_LIB - the MsgPuck static library needed for linking

set(_MSGPUCK_ROOT_HINTS
    HINTS ${MSGPUCK_ROOT_DIR})

find_path(MSGPUCK_INCLUDE_DIRS
    NAMES msgpuck.h
    PATH_SUFFIXES msgpuck
    ${_MSGPUCK_ROOT_HINTS})

find_library(MSGPUCK_LIB
    NAMES libmsgpuck.a
    ${_MSGPUCK_ROOT_HINTS})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(MsgPuck
    REQUIRED_VARS MSGPUCK_INCLUDE_DIRS MSGPUCK_LIB)

set(MSGPUCK_FOUND TRUE)
mark_as_advanced(MSGPUCK_INCLUDE_DIRS MSGPUCK_LIB)
