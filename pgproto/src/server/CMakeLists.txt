set(SERVER_SOURCES
    server.c
)

build_module(pgproto ${SERVER_SOURCES})
target_link_libraries(pgproto
    PRIVATE tarantool postgres ${MSGPUCK_LIB})
