#!/bin/head -n 27
--
-- Usage:
--
--   rm -rf /tmp/picospec; picodata run --data-dir /tmp/picospec --script tools/dump_spec.lua | tee system_tables.spec
--
-- Dump system tables schema to the stdout. The output format self-defined by the script.
--
-- Example output:
--
-- ### _pico_table
--
-- Поля:
--
-- * `id`: (_unsigned_)
-- * `name`: (_string_)
-- * `distribution`: (_array_)
-- * `format`: (_array_)
-- * `schema_version`: (_unsigned_)
-- * `operable`: (_boolean_)
-- * `engine`: (_string_)
-- * `owner`: (_unsigned_)
--
-- Индексы:
--
-- * `id` (unique), parts: `[id]`
-- * `name` (unique), parts: `[name]`

local fiber = require('fiber')

local function smt_seq(t)
    return setmetatable(t, {__serialize = 'seq'})
end

local tables = {
    '_pico_table',
    '_pico_index',
    '_pico_routine',
    '_pico_property',
    '_pico_peer_address',
    '_pico_instance',
    '_pico_replicaset',
    '_pico_tier',
    '_pico_user',
    '_pico_privilege',
    '_pico_plugin',
    '_pico_service',
    '_pico_service_route',
    '_pico_plugin_migration',
    '_pico_plugin',
    '_pico_service',
    '_pico_service_route'
}

local function printf(fmt, ...)
    print(string.format(fmt, ...))
end

local function main()
    local res = {}

    printf(
        "Описание соответствует версии Picodata `%s`.",
        pico.PICODATA_VERSION
    )

    for i, t in ipairs(tables) do
        local tbl = box.space['_pico_table'].index[1]:get(t)
        printf("\n### %s\n", tbl.name)

        printf("Поля:\n")
        for _, field in ipairs(tbl.format) do
            local name = field.name
            local type = field.field_type
            local nullable = field.is_nullable
            printf("* `%s`: (_%s_)", name, type)
        end
        printf("")

        printf("Индексы:\n")
        for _, idx in pairs(box.space['_pico_index']:select({tbl.id})) do
            local parts = {}
            for _, v in ipairs(idx.parts) do
                table.insert(parts, v[1])
            end
            printf(
                "* `%s` (%s), parts: `[%s]`",
                idx.name,
                idx.opts[1]['unique'] and "unique" or "non-unique",
                table.concat(parts, ", ")
            )
        end

        local format = setmetatable(tbl.format, nil)
        local index = {}
    end
end

fiber.new(function()
    while not pcall(box.schema.func.call, '.proc_read_index', 1) do
        fiber.sleep(0.1)
    end

    local ok, err = xpcall(main, debug.traceback)
    if not ok then
        print(err)
        os.exit(1)
    end

    os.exit(0)
end)
