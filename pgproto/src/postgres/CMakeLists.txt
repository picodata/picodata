set(POSTGRES_SOURCES
    report.c
    port.c
    startup.c
    auth.c
    postgres.c
    attributes.c)


build_module(postgres "${POSTGRES_SOURCES}")
message("MSGPUCK_LIBRARIES: ${MSGPUCK_LIBRARIES}")
target_link_libraries(postgres PRIVATE ${MSGPUCK_LIBRARIES})

target_compile_options(postgres PRIVATE "-Wno-format-security")
