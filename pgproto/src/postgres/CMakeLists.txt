set(POSTGRES_SOURCES
    attributes.c
    auth.c
    messages.c
    port.c
    postgres.c
    report.c
    startup.c
)


build_module(postgres "${POSTGRES_SOURCES}")
message("MSGPUCK_LIBRARIES: ${MSGPUCK_LIBRARIES}")
target_link_libraries(postgres PRIVATE ${MSGPUCK_LIBRARIES})

target_compile_options(postgres PRIVATE "-Wno-format-security")
