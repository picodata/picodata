# Change Log

# [?.?.?] Unreleased

### Added

### Changed

### Fixed

### Deprecated

### Breaking changes
- Remove erroneously added `clap::ArgEnum` implementation for `AuthMethod`.
- Removed deprecated trait `AsTuple`
- Removed deprecated `Tuple` methods `from_struct`, `as_struct` & `into_struct`
- Removed deprecated `FunctionArgs` method `as_struct`
- enums `TarantoolErrorCode`, `IteratorType`, `SayLevel` & `SystemSpace` no
  longer implement trait `ToPrimitive`, just use `as i32` instead.

### Added (picodata)
- struct `read_view::ReadView` for opening read views on selected spaces.
- Expose `box_access_check_space` to be able to run access checks on spaces externally

### Fixed (picodata)
- A race condition causing undefined behaviour due to fiber_cond_delete being called outside tx sometimes
- A race condition causing unbounded channel receiver too block the thread forever


# [3.0.1] Sep 28 2023

### Fixed
- Change tarantool-proc version to fix backwards compatibility


# [3.0.0] Sep 26 2023

### Added
- With the new `async-std` feature flag `network::client::tcp::TcpStream` implements
  async `Read` and `Write` traits from `async-std` crate instead of `futures`.
- `box.session.su` API is now supported. When building for picodata with `picodata` feature enabled this API will use C-API directly. When used with vanilla tarantool lua polyfill is used.
- New `tarantool::session::user_id_by_name` API is available when running `picodata`.
- New `AuthData` generates authentication data when running `picodata` (a wrapper over `box_auth_data_prepare` symbol from the C API).
- Authentication methods enum (`AuthMethod`) was added to the module.
- Method `space::Builder::space_type`, field `SpaceCreateOptions::space_type` and
  enum `SpaceType` which is now the primary way of specify the type of space.
- Fully-temporary spaces can now be created, destroyed and indexes can be created
  for them as well (see `SpaceType::Temporary`). This feature requires your
  tarantool executable to support some low level APIs, you can use the newly
  added `ffi::has_fully_temporary_spaces` function to check if the required
  APIs are supported.
- `SystemSpace::as_space` method for easier conversion to `Space`.
- `schema::space::space_metadata` function
- `space::SPACE_ID_MAX` constant with a value of the maximum possible space id.
- `ViaMsgpack` wrapper type for passing values to/from lua by converting them
  to msgpack first.
- `msgpack::ValueIter::len` method which returns array length if it's known.

### Fixed
- `tarantool::log::say` used to do undefined behaviour when the message
  contained formatting sequences of %n sort.
- performance issues when sending large amounts of data via network::client.

### Changed
- `network::client::tcp::TcpStream` will now always try IPv4 addresses first when connecting.
- `tarantool::session::uid` and `tarantool::session::euid` when running `picodata` now use native C-API instead of calling into Lua.
- Updated some doc comments for Space and Index methods.

### Deprecated
- Methods `temporary`, `is_local` & `is_sync` of struct `space::Builder` are
  deprecated in favour of new method `space_type`.
- `schema::space::SpaceMetadata` is now a deprecated alias of `space::Metadata`.
- network::client::tcp::Error variants ResolveAddress & Connect now contain the
  address for which the corresponding operation has failed.

### Breaking changes
- `tarantool::session::uid` and `tarantool::session::euid` now return `UserId` type which is an alias for `u32`. Previously `isize` was used.
- `SpaceCreateOptions` fields `is_temporary`, `is_local` & `is_sync` are
    removed in favour of new field `space_type`.


# [2.0.0] Aug 28 2023

### Added
- `tarantool::log::TarantoolLogger::convert_level` method for converting
    log::Level to SayLevel taking the mapping function into account.
- `tlua::Push` and `tlua::LuaRead` implementations for `SayLevel`.
- `examples/luaopen` example of how to implement native lua modules.
- `tarantool::cbus` module for communication between any arbitrary thread and 
tarantool thread via syncronization primitives (channels) and low-level cbus api.
- `tarantool::time::Instant` a custom implementation of std-like `Instant` with more saturating operations
and support of the `fiber_clock` API.
- `r#async::sleep` - an async friendly analog of `fiber::sleep`.
- `SpaceEngineType` variants `SysView`, `Blackhole` and `Service` which cannot
  be created by users, but can be deserialized from contents of _space system space.
- `fiber::Builder::defer_ffi` & `fiber::Builder::defer_lua` low-level functions.
  Users should use `fiber::Builder::defer` instead in most cases.

### Changed
- `fiber::defer` will now use a more efficient implementation on newer versions
  of tarantool.

### Fixed
- `log::Log::enabled` implementation for TarantoolLogger no longer ignores the
  mapping provided at construction.
- A copy of fiber name used to leak in `Fiber::new` and `Fiber::new_with_attr`.
- `tarantool::decimal` api is now thread safe, which allows it to be used in concurrent threads.

### Deprecated
- `fiber::start_proc`, `fiber::defer_proc`, `fiber::Builder::proc`, `fiber::Builder::proc_async`:
  in favor of `fiber::start`, `fiber::defer`, etc. These used to be implemented with optimizations,
  but now their internals are simplified and unified, so there's now no difference between the
  "proc" and "non-proc" variants. This may result in minor performance degradation in debug builds,
  but in release builds there shouldn't be any difference.
- `fiber::UnitJoinHandle`, `fiber::LuaJoinHandle` & `fiber::LuaUnitJoinHandle`
    are now just aliases to `fiber::JoinHandle` and are deprecated.

### Breaking Changes
- `transaction::start_transaction` has a more flexible error handling,
  and is renamed to `transaction::transaction`
- `fiber::clock` now returns `tarantool::time::Instant`
- `fiber::time` and `fiber::time64` returning non-monotonic time removed. If
    calendar time is needed, use `std::time::SystemTime`.
- `tlua::PushIterError::TooManyValues` now stores how many values were attempted
    to be pushed.
- `<tlua::AsTable as Push>::Err` changed from `PushIterError` to
    `AsTablePushError`.
- `fiber::clock64` removed in favor of a new `Instant` based `fiber::clock` API
- `Error::Decode` now contains expected rust type and actual incorrect msgpack
    contents.
- `sql::Statement::execute` now returns `Error::DecodeRmpValue`.
- Removed a lot of helper structs and traits from `fiber` module, including
  `fiber::LuaFiber`, `fiber::FiberFunc`, etc. Users
  should not have been using those anyway, because `fiber::start`, `fiber::defer`,
  `fiber::Builder::start`, `fiber::Builder::defer` exist.
- Changed return types of function which spawn fibers.
  Now a single `fiber::JoinHandle` type is used everywhere.


# [1.1.0] Jun 16 2023

### Added
- `vclock::Vclock` data structure representing Tarantool vector clock
  (`box.info.vclock` Lua interface).
- `vclock::Lsn` type alias for `u64` representing Tarantool log sequence number.
- `ToTupleBuffer::tuple_data` method which returns `Option<&[u8]>`. It's only
  implemented for wrapper types (`TupleBuffer`, `RawBytes`, `RawByteBuf`)
  as an optimization to avoid extra copies.
- `impl ToOwned<Owned = RawByteBuf> for RawBytes`
- `impl Borrow<RawBytes> for RawByteBuf`

### Fixed
- Doc comments are no longer lost for functions marked with `#[proc]` attribute.
- Error when compiling with --no-default-features.
- `cargo test` link failure when a `#[::test]` is defined in the same mod with a
  `#[tarantool::proc]` on MacOS.

### Changed
- Marked trivial functions with `#[inline]` attributes in mods tuple, index, space.


# [1.0.0] May 29 2023

### Added
- `fiber::Builder::func_async` and `fiber::Builder::proc_async` - methods for
    easier construction with `Builder` of fibers executing `Future`
- `tlua::CFunction` wrapper struct to push `C` functions as values into lua.
- `#[tarantool::test]` macro attribute for defining test functions and adding
  them into a global list of test cases. Requires `--features=test`.
- `test::test_cases` & `test::collect_tester` functions for accessing the global
  list of test cases. This can be used to implement a custom testing harness.
  Requires `--features=test`.
- `test::TestCase` struct which is used internally in `#[tarantool::test]` and
  is returned by `test::test_cases`. Requires `--features=test`.
- `ffi::tarantool::Proc` type alias for a tarntool stored C function.
- `proc::all_procs` helper function which returns a global slice of `proc::Proc`
  \- descriptions for stored procedures defined with `#[tarantool::proc]`.
- `proc::module_path` helper function for getting a path to the dynamically
  linked object file in which the given symbol is defined.
- `msgpack::ArrayWriter` helper struct for generating msgpack arrays from
    arbitrary serializable data.
- `msgpack::ValueIter` helper struct for iterating over msgpack values.
- `tarantool::network::client` alternative async network client.
- `tarantool::network::client::reconnect` reconnecting async network client based on `network::client`.
- `tarantool::network::protocol` sans-io (without transport layer) implementation of Tarantool Binary Protocol.
  Serves as a base for `network::client`, but can be also used independently by other client implementations.
- `r#async::timeout::Error` enum with `Expired` and `Failed` vairants.
- `r#async::timeout::Result<T, E>` type alias for
    `std::result::Result<T, r#async::timeout::Error<E>>`
- `Space::from_id_unchecked` unsafe function, for creating a space struct from a space id.
- `Index::from_ids_unchecked` unsafe function, for creating a index struct from space and index ids.
- `examples/tokio-hyper` example of using tarantool with tokio + hyper
- `std::ops::Deref<Target = str>` implementation for enums defined with
    `tarantool::define_str_enum`.
- `Into<&'static str>` implementation for enums defined with
    `tarantool::define_str_enum`.
- `WrongType::[info|when|actual*|expected*|subtype*]` constructor methods to be
    used in impl LuaRead for user defined types.
- `impl LuaRead for TupleBuffer`.
- `LuaTable::try_get` method for checking which error happened.
- `fiber::r#async::Mutex` an async Mutex, with guard that can be held across await points.
- `TarantoolError::message()` method for getting just the error message.
- `T::as_cstr` method returning std::ffi::CStr is now implemented for
    enums defined with `tarantool::define_str_enum`.
- `T::values` method returning a static slice of static str variant names
    is now defined for enums defined with `tarantool::define_str_enum`.
- `index::Metadata` struct representing tuples stored in `_index` system space.
- `Index::meta` method for getting index metadata from `_index` system space.
- `Index::id` & `Index::space_id` accessor methods for getting ids.
- `tuple::KeyDefPart` helper struct for constructing `tuple::KeyDef`, it also
    has `try_from_index_part` constructor method which accepts `index::Part`.
- `IndexMetadata::to_key_def` method for creating a `tuple::KeyDef` instance
    from index metadata. Can be used to compare tuples with a key.
- `IndexMetadata::to_key_def_for_key` method for creating `tuple::KeyDef`
    similar to `to_key_def` but is used for comparing just the keys themselves.
- `IndexId` and `SpaceId` type aliases

### Changed
- `r#async::timeout::Timeout` can now only be wrapped around a future which
    resolves into a `std::result::Result<T, E>` and timeout itself now resolves
    into `r#async::timeout::Result`.
- `LuaRead` methods now return `WrongType` error in case of failure.
- `LuaRead` for `Tuple` now accepts arbitrary lua tables, not only tuples.
- `KeyDef::new` now accepts iterator over references to `KeyDefPart` and returns
    a result.
- All functions which take `t: &T` where `T: ToTupleBuffer`, now allow `T` to be
   unsized (`?Sized`), e.g. `tuple::RawBytes`

### Fixed
- Performance issue with `fiber::csw()` and `fiber::check_yield()`
  that caused tests failure.

### Removed
- `r#async::timeout::Expired` in favor of `r#async::timeout::Error`
- `tuple::KeyDefItem` in favor of `tuple::KeyDefPart`.
- `feature = "schema"`. Now the functionality is supported by default

# [0.6.5] Apr 5 2023

### Changed
- `TarantoolError`'s `Display` implementation will no longer lookup the error
    code in lua in case it's not found in `TarantoolErrorCode` enum.

### Fixed
- Link errors when `Display::fmt` is called for `tarantool::error::Error`
    from rust unit tests
- Used to have wrong crate versions for internal dependencies
      (tlua, tarantool-proc, etc.)


# [0.6.4] Dec 15 2022

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
- `fiber::Mutex`'s methods `lock` & `try_lock` now will log the location of
    last successful lock when built with `debug_assertions`.
- `#[track_caller]` added to tlua functions that can panic.
- A clarification in `tarantool::proc` documentation about the safety of using
    borrowed arguments.
- `impl LuaRead for StaticLua`: this is mainly useful for capturing the lua
    context passed to rust-callbacks for example for use with `tlua::error!`.
    See test `error` in `tests/src/tlua/functions_write.rs` for examples.
- Add `tlua::Throw` wrapper type for throwing lua errors from an error returned
    by rust callback.
- Doc-comments here and there.
- `fiber::r#yield` function for yielding fibers likewise tarantool LUA api.
- `#[derive(Copy)]` for a bunch of light enums including `TarantoolErrorCode`,
    `SayLevel`, `SystemSpace`, `FieldType`.
- `define_str_enum` macro suitable for public use.
- `fiber::csw` function for tracking fiber context switches.
- `fiber::check_yield` function for easier testing.
- `fiber::r#async` module with a simple fiber based async/await runtime.
- `fiber::block_on` function for executing a Future on a fiber based async/await
    runtime.
- `fiber::r#async::timeout::{timeout, IntoTimeout}` utilities for constraining
    futures with a timeout (only works with the fiber based async/await runtime!).
- `fiber::r#async::oneshot` an async/await oneshot channel (inspired by
    `tokio::sync::oneshot`).
- `fiber::r#async::watch` an async/await watch channel (inspired by
    `tokio::sync::watch`).
- `fiber::{start_async, defer_async}` for executing a future in a separate fiber.

### Removed

- `raft` cfg feature that wasn't finished and will never be.
- `tests/test.sh` script for running tests (`cargo test` can now be used).

### Fixed
- `TupleBuffer` no longer copies data into tarantool's transaction memory pool
    in `TupleBuffer::from_vec_unchecked`, which previously would result in a use
    after free in some cases.
- `impl<_> From<tlua::PushIterError<_>> for tlua::Void` is now more general
    which allows more types to be used in contexts like `tlua::Lua::set`, etc.
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
- `Space`, `Index`, `RemoteSpace` & `RemoteIndex` mutating methods now don't
    require `self` to be borrowed mutably. This is safe, because the only
    mutation those methods do is confined in the tarantool api, which is robust
    with respect to what rust mutability rules are supposed to protect from
    (except for thread safety, which is not supported by any of tarantool apis).
    Relaxing the `&mut self` requirement greatly increases the api's ease of use
    with the only downside of added compile warning of "variable does not need
    to be mutable" which is a small price to pay.
- In `tlua` if a lua error happens during code evaluation the location in the
    rust program where the code was created is now displayed in the error, i.e.
    the location of a call to `Lua::eval`, `Lua::exec`, etc. will be displayed.
- `tlua::Lua::set` function now has 2 generic parameters instead of 3 (not
    including lifetime parameters).

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
