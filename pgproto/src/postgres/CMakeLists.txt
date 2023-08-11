set(POSTGRES_SOURCES
    attributes.c
    auth.c
    messages.c
    port.c
    postgres.c
    report.c
    startup.c
)

add_library(postgres STATIC ${POSTGRES_SOURCES})
set_property(TARGET postgres PROPERTY POSITION_INDEPENDENT_CODE ON)
target_compile_options(postgres PRIVATE "-Wno-format-security")
