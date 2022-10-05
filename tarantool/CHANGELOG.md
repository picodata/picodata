# Change Log

# [0.6.4] Oct ?? 2022

### Added
- `tarantool::space::UpdateOps` helper struct for use with `update` & `upsert`
    methods of `Space` & `Index`.
- `impl ToTupleBuffer for TupleBuffer`
- serde_bytes::[Des|S]erialize implementations for `TupleBuffer` & `RawByteBuf`
- `#[derive(Clone, PartialEq, Eq)]` for `TupleBuffer` & `RawByteBuf`
- `Space` & `Index` now have `update_raw` & `upsert_raw` methods that accept
    serialized arguments.
- `space::FieldType::Varbinary`, `space::FieldType::Datetime`,
    `space::FieldType::Interval`, `space::FieldType::Map`.
- `tarantool::Space::Field::varbinary`, `tarantool::Space::Field::datetime`,
    `tarantool::Space::Field::interval`, `tarantool::Space::Field::map`.
- `index::FieldType::Datetime`.
- `impl Debug for Index`.
- `space::Field` now implements `From<(S, space::FieldType)>` &
    `From<(S, space::FieldType, IsNullable)>` where `S: Into<String>`, which can
    be used in the `space::Builder::field` and `space::Builder::format` methods.
- `space::IsNullable` helper enum.
- `space::Builder::into_parts` & `index::Builder::into_parts`  for accessing
    inner structs.
- `tlua::LuaRead`, `tlua::Push` & `tlua::PushInto` derive macros now support
    new-type style tuple structs: they are treated as the inner type.
- `impl tlua::PushInto for Tuple`.
- `net_box::promise::TryGet::into_res` and `From<TryGet<_, _>> for Result<_, _>`.
- `impl [tlua::LuaRead|tlua::Push|tlua::PushOne] for tlua::Object`.
- Doc-comments here and there.
- `tests/test.sh` now supports `TARANTOOL_MODULE_BUILD_MODE` environment
    variable to select which build mode is tested (debug, release, etc.)

### Fixed
- `TupleBuffer` no longer copies data into tarantool's transaction memory pool
    in `TupleBuffer::from_vec_unchecked`, which previously would result in a use
    after free in some cases.
- `tests/run_benchmarks.lua` now works again.

### Changed
- `TarantoolError::error_code` now returns a `u32` instead of `TarantoolErrorCode`.
- `TarantoolError`'s `Display` implementation will lookup the error code in lua
  in case it's not found in `TarantoolErrorCode` enum.
- `TarantoolErrorCode::NoSuchFieldName` is renamed
  `TarantoolErrorCode::NoSuchFieldNameInSpace`.
- `TarantoolErrorCode::BootstrapReadonly`'s value changed from 201 to 203.
- `update!` & `upsert!` macros are now more efficient due to the use of
    `update_raw` & `upsert_raw`.
- `SpaceCreateOptions::default` now sets `is_local` & `is_temporary` to `false`.
- `space::SpaceFieldType` is renamed `space::FieldType`.
    And `space::SpaceFieldType` is now a deprecated type alias.
- `index::IndexFieldType` is renamed `index::FieldType`.
    And `index::IndexFieldType` is now a deprecated type alias.
- enums `SpaceEngineType`, `space::FieldType`, `IndexType`, `index::FieldType` &
    `RtreeIndexDistanceType` now all
  * implement `Display`,
  * implement `std::convert::AsRef<str>` & `std::convert::Into<String>`,
  * implement `std::str::FromStr`,
  * implement `tlua::Push`, `tlua::PushInto`, `tlua::LuaRead`.
  * have a `const fn as_str`.
- `space::Builder::field` now accepts `impl Into<Field>`.
- `space::Builder::format` now accepts `impl IntoIterator<Item = impl Into<Field>>`.
- `index::Builder::parts` now accepts `impl IntoIterator<Item = impl Into<Part>>`.
- `space::Field` constructors accept `impl Into<String>`.

### Deprecated
- `update_ops` & `upsert_ops` methods of `Space` & `Index` are deprecated in
    favour of `update_raw` & `upsert_raw`.

# [0.6.3] Aug 08 2022

### Added
- Tuples can now be used as parameters to functions like `Space::get`,
    `Index::get`, etc. (`impl ToTupleBuffer for Tuple`)
- Tuple fields can now be read as raw bytes (without deserializing) using
    `&tarantool::tuple::RawBytes` (borrowed) or `tarantool::tuple::RawByteBuf`
    (owned)
- Tuples can now be efficiently returned from stored procedures defined with
    `#[proc]` macro attribute. (`impl Return for Tuple`)
- Raw bytes can now be returned from stored procedures defined with `#[proc]`
    macro attribute using `RawBytes` or `RawByteBuf`.
- Stored procedures defined with `#[proc]` macro attribute can now accept
   borrowed arguments. For example `#[proc] fn strlen(s: &str) -> usize
   { s.len() }` now compiles.
- `FunctionArgs::decode` method for efficient decoding of the stored procedure
    arguments.
- `tlua::Lua::eval_with` & `tlua::Lua::exec_with` method for passing parameters
    in place of `...` when evaluating lua code.
- `tlua::Strict` wrapper for reading lua numbers without implicit conversions.
- `tlua::CData` wrapper for reading/writing values as luajit cdata. Can be used
    work with primitve cdata types like numbers and pointers and also user
    defined structs.
- `tlua::AsCData` trait for user defined types which can represented as luajit
    cdata.
- `tlua::CDataOnStack` for working with luajit cdata efficiently within the lua
    stack. Can be used to read the raw cdata bytes or for passing cdata values
    into lua functions.
- Added support for reading/writing `isize` & `usize` in `tlua`.
- `Tuple::new` function for creating tuples from anything that can be converted
    to one.
- `Tuple::decode` method for converting tuple into something that implements
    `DecodeOwned`.
- `tarantool::space::clear_cache` function for clearing the cache in case it was
    invalidated.
- `tarantool::space::Space::meta` method for getting space metadata.
- `tarantool::net_box::Conn::execute` for executing remote sql queries.
- `tarantool::trigger::on_shutdown` function for setting a tarantool on_shutdown
    trigger.
- `tlua::LuaRead`, `tlua::Push` & `tlua::PushInto` derive macros now support
    generic structs & enums.
- `picodata` cfg feature for compatibility with [picodata's tarantool fork].
- `impl From<TupleBuffer> for Vec<u8>`
- `impl From<(u32, FieldType)> for KeyDefItem`

### Fixed
- Load type failure on tarantool 2.9 and later related to missing access to some
    internal symbols
- Rust callbacks of `Option<T>` failing when called from lua without arguments.
- `tlua::LuaRead` implementation for `HashMap<K, V>` no longer ignores
    conversion errors.
- Memory leak in `tarantool::set_error!` macro.
- `LuaRead` failing for some derived types related to enums with optional
    variants.
- `test/tests.sh` now supports custom cargo target directories.

### Changed
- Most tuple accessor methods changed their bounds (used to require
    `serde::Deserialize` now require `tuple::Decode`) e.g. `Tuple::get`,
    `TupleIterator::next`, etc. This coincidentally means that you can read a
    tuple's field into a `Tuple`, but you probably don't want that.
- `tarantool::decimal::Decimal` is now implemented using the
    [dec](https://crates.io/crates/dec) crate instead of tarantool built-in
    decimals (which are based on a patched version of the same decNumber library
    as the dec crate). This means there are minor changes in decimal's behavior
    (e.g. they're printed with scientific notation now, etc.) but nothing major.
- `Decimal::log10` & `Decimal::ln` now return `None` in case of invalid values
    instead of panicking.
- `KeyDef::new` now accepts a generic `impl IntoIterator<Item=impl Into<KeyDefItem>>`

### Deprecated
- `AsTuple` trait is now deprecated. User defined types should instead
    implement the new `tarantool::tuple::Encode` trait. And most of the api
    functions now require the parameters to implement
    `tarantool::tuple::ToTupleBuffer` (implemented for `Encode` types by default).
- `Tuple::from_struct` is deprecated. Use `Tuple::new` instead.
- `Tuple::as_struct` & `Tuple::into_struct` are deprecated. Use `Tuple::decode`
    instead.


# [0.6.2] Jun 09 2022

### Added
- `Conn::call_async`& `Conn::eval_async` functions for non-yielding network
    operations
- `Space::find_cached`& `Space::index_cached` functions better performance when
    accessing spaces and indexes
- `injected` & `custom_ret` arguments for `tarantool::proc` attribute macro
- builtin trait implementations for a number of types (`Hash` for `Decimal`,
    `Decirialize`, `Clone`, `Debug` for multiple space and index related structs
    inside)

### Fixed

- `decimal!` macro can now be used
- fixed memory corruption in `Decimal::to_string`
- fixed `is_sync` space option not working
- add a blanket impl `AsTuple` for `&T`
- README typos and other mistakes
- doc tests now pass
- fixed "unused unsafe" warning in `error!` macro

# [0.6.1] Apr 08 2022

### Added
- `upsert!` macro for operations of different types
- `tlua::AsTable` wrapper for pushing/reading rust tuples as lua tables
- `#[tarantool::proc]` attribute macro for easy stored procedure definitions

### Fixed

- `c_char` related compile errors on systems where `c_char` is `unsigned`
- `Display` not being implemented for some error types


# [0.6.0] Mar 17 2022

Added

### Added
- `tlua::Lua::new_thread` & `tarantool::lua_state`
- ability to set a custom filter for tarantool logger (see
    `TarantoolLogger::with_mapping`)
- `AsTuple` implementation for longer tuples
- `CString` & `CStr` support in **tlua**
- `update_space` macro for operations of different types
- `tlua::TableFromIter`
- `FunctionCtx::as_struct` for streamlined conversions
- `std::fmt::Debug` implementations for `Tuple`, `TupleBuffer` & others
- `is_nullable` setting for space field formats
- `tlua::error` macro for throwing a lua error from a rust callback
- `LuaError::WrongType` is returned if a rust callback receives incorrect
    arguments
- `LuaRead` implementation for `[T; N]`
- space/index creation builder api
- specifying index parts by json path
- `fiber::Mutex`
- `Indexable`, `IndexableRW` & `Callable` types for working with generic (not
    just builtin ones) indexable & callable lua values
- `AsLua::pcall` for calling rust functions in protected mode capturing any
  lua exceptions

### Changed
- join handles returned by `fiber::`{`start`|`defer`}[`_proc`] now have a
    lifetime parameter, which allows non-static fiber functions
- conversions between `Tuple`, `TupleBuffer` and `Vec<u8>` have been reorganized
  and made safer
- reading `Vec<T>` from lua no longer ignores elements that failed to convert to
    `T`
- some `tarantool::tuple` operation signatures have been changed removing
    unnecessary `Result`s, whenever an error cannot actually happen
- `fiber::Channel` only accepts `'static` values now

### Fixed
- `tlua::push_userdata` no longer requires arguments implement `Send`
- partially pushed tuples poluting the stack
- assertion violation when trying to create a tuple with incorrect msgpack data
- build for Arm MacOS

# [0.5.1] Dec 24 2021

**TODO**


[picodata's tarantool fork]: https://git.picodata.io/picodata/tarantool
