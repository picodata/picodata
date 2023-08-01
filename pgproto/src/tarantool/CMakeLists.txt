set(TARANTOOL_MODULE_SOURCES
    sio.c
    evio.c)

build_module(tarantool "${TARANTOOL_MODULE_SOURCES}")
