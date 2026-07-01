## feat

- Switched the `binary_data_decoding` compat option to `'new'`, so MessagePack
  `BIN` values now decode in Lua as the `varbinary` cdata type instead of a
  plain Lua string, keeping `BIN` and `STR` distinguishable.
