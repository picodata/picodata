For those who will eventually come up with a patch to satisfy the static analyzer(svace).

If patch affects `/src/box/sql/expr.c` or `/src/box/sql/printf.c`
please look at https://git.picodata.io/picodata/picodata/picodata/-/merge_requests/1398.
These patches were removed because they break some tarantool tests:
`tarantool-sys_printf.patch` -> `test/sql-tap/printf2.test.lua`,
`tarantool-sys_expr.patch`   -> `test/sql-tap/identifier-characters.test.lua`, `test/sql-tap/sql-errors.test.lua`.

