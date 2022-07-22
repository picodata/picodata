# Change Log

# [0.6.3] Aug ?? 2022

### Added
- Tuples can now be used as parameters to functions like `Space::get`,
    `Index::get`, etc. (`impl ToTupleBuffer for Tuple`)
- Tuple fields can now be read as raw bytes (without deserializing) using
    `&tarantool::tuple::RawBytes` (borrowed) or `tarantool::tuple::RawByteBuf`
    (owned)
- Tuples can now be efficient returned from stored procedures defined with
    `#[tarantool::proc]` macro attribute. (`impl Return for Tuple`)
- `Tuple::new` function for creating tuples from anything that can be converted
    to one.
- `impl From<TupleBuffer> for Vec<u8>`
- `impl From<(u32, FieldType)> for KeyDefItem`

### Changed
- `AsTuple` trait is now deprecated. User defined types should instead
    implement the new `tarantool::tuple::Encode` trait. And most of the api
    functions now require the parameters to implement
    `tarantool::tuple::ToTupleBuffer` (implemented for `Encode` types by default).
- `Tuple::from_struct` is deprecated. Use `Tuple::new` instead.
- Most tuple accessor methods changed their bounds (used to require
    `serde::Deserialize` now require `tuple::Decode`) e.g. `Tuple::get`,
    `TupleIterator::next`, etc. This coincidentally means that you can read a
    tuple's field into a `Tuple`, but you probably don't want that.
- `KeyDef::new` now accepts a generic `impl IntoIterator<Item=impl Into<KeyDefItem>>`


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
