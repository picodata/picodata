set(SERVER_SOURCES
    server.c)

build_module(tcpserver "${SERVER_SOURCES}")
target_link_libraries(tcpserver PRIVATE
                      tarantool postgres
                      ${MSGPUCK_LIBRARIES})
