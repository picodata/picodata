set(TARANTOOL_MODULE_SOURCES
    sio.c
    evio.c
)

add_library(tarantool STATIC ${TARANTOOL_MODULE_SOURCES})
set_property(TARGET tarantool PROPERTY POSITION_INDEPENDENT_CODE ON)
