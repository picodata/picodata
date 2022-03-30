//! Tarantool C API bindings for Rust.
//! This library contains the following Tarantool API's:
//!
//! - Box: [spaces](space), [indexes](index), [sequences](sequence)
//! - [Fibers: fiber attributes, conditional variables, latches](fiber)
//! - [CoIO](coio)
//! - [Transactions](transaction)
//! - [Schema management](schema)
//! - [Protocol implementation](net_box) (`net.box`): CRUD, stored procedure call, triggers
//! - [Tuple utils](mod@tuple)
//! - [Decimal numbers](mod@decimal)
//! - [Logging](log) (see <https://docs.rs/log/>)
//! - [Error handling](error)
//!
//! > **Caution!** The library is currently under development.
//! > API may be unstable until version 1.0 will be released.
//!
//! ### Features
//!
//! - `net_box` - Enables protocol implementation (enabled by default)
//! - `schema` - Enables schema manipulation utils (WIP for now)
//!
//! ### Prerequisites
//!
//! - rustc 1.48 or newer
//! - tarantool 2.2
//!
//! ### Stored procedures
//!
//! Tarantool can call Rust code via a plugin, from Lua using FFI, or as a stored procedure.
//! This tutorial only is about the third
//! option, Rust stored procedures. In fact Rust routines are always "C
//! functions" to Tarantool but the phrase "stored procedure" is commonly used
//! for historical reasons.
//!
//! This tutorial contains the following simple steps:
//! 1. `examples/easy` - prints "hello world";
//! 1. `examples/harder` - decodes a passed parameter value;
//! 1. `examples/hardest` - uses this library to do a DBMS insert;
//! 1. `examples/read` - uses this library to do a DBMS select;
//! 1. `examples/write` - uses this library to do a DBMS replace.
//!
//! By following the instructions and seeing that the results users should
//! become confident in writing their own stored procedures.
//!
//! #### Example
//!
//! Check that these items exist on the computer:
//! - Tarantool 2.2
//! - A rustc compiler + cargo builder. Any modern version should work
//!
//! Create cargo project:
//! ```shell script
//! $ cargo init --lib
//! ```
//!
//! Add the following lines to `Cargo.toml`:
//! ```toml
//! [package]
//! name = "easy"
//! version = "0.1.0"
//! edition = "2018"
//! # author, license, etc
//!
//! [dependencies]
//! tarantool = "0.5.0" # (1)
//! serde = "1.0" # (2)
//!
//! [lib]
//! crate-type = ["cdylib"] # (3)
//! ```
//!
//! 1. add to dependencies `tarantool` library;
//! 1. add to dependencies [Serde](https://!github.com/serde-rs/serde), this is optional and required if you want to use rust
//! structures as a tuple values (see [this example](#harder));
//! 1. you need to compile dynamic library.
//!
//! Requests will be done using Tarantool as a client. Start Tarantool, and enter these requests:
//! ```lua
//! box.cfg{listen=3306}
//! box.schema.space.create('capi_test')
//! box.space.capi_test:create_index('primary')
//! net_box = require('net.box')
//! capi_connection = net_box:new(3306)
//! ```
//!
//! In plain language: create a space named `capi_test`, and make a connection to self named `capi_connection`.
//!
//! Leave the client running. It will be used to enter more requests later.
//!
//! Edit `lib.rs` file and add the following lines:
//! ```rust
//! use std::os::raw::c_int;
//! use tarantool::tuple::{FunctionArgs, FunctionCtx};
//!
//! #[no_mangle]
//! pub extern "C" fn easy(_: FunctionCtx, _: FunctionArgs) -> c_int {
//!     println!("hello world");
//!     0
//! }
//!
//! #[no_mangle]
//! pub extern "C" fn easy2(_: FunctionCtx, _: FunctionArgs) -> c_int {
//!     println!("hello world -- easy2");
//!     0
//! }
//! ```
//!
//! Compile the program:
//! ```shell script
//! $ cargo build
//! ```
//!
//! Start another shell. Change directory (`cd`) so that it is the same as the directory that the client is running in.
//! Copy the compiled library (it is located in subfolder `target/debug` at you
//! project sources folder) to the current folder and rename it to `easy.so`
//!
//! Now go back to the client and execute these requests:
//! ```lua
//! box.schema.func.create('easy', {language = 'C'})
//! box.schema.user.grant('guest', 'execute', 'function', 'easy')
//! capi_connection:call('easy')
//! ```
//!
//! If these requests appear unfamiliar, read the descriptions of
//! [box.schema.func.create()](https://!www.tarantool.io/en/doc/2.2/reference/reference_lua/box_schema/#box-schema-func-create),
//! [box.schema.user.grant()](https://!www.tarantool.io/en/doc/2.2/reference/reference_lua/box_schema/#box-schema-user-grant)
//! and [conn:call()](https://!www.tarantool.io/en/doc/2.2/reference/reference_lua/net_box/#net-box-call).
//!
//! The function that matters is `capi_connection:call('easy')`.
//!
//! Its first job is to find the 'easy' function, which should be easy because by default Tarantool looks on the current directory
//! for a file named `easy.so`.
//!
//! Its second job is to call the 'easy' function. Since the `easy()` function in `lib.rs` begins with `println!("hello world")`,
//! the words "hello world" will appear on the screen.
//!
//! Its third job is to check that the call was successful. Since the `easy()` function in `lib.rs` ends with return 0, there
//! is no error message to display and the request is over.
//!
//! The result should look like this:
//! ```text
//! tarantool> capi_connection:call('easy')
//! hello world
//! ---
//! - []
//! ...
//! ```
//!
//! Now let's call the other function in lib.rs - `easy2()`. This is almost the same as the `easy()` function, but there's a
//! detail: when the file name is not the same as the function name, then we have to specify _{file-name}_._{function-name}_
//! ```lua
//! box.schema.func.create('easy.easy2', {language = 'C'})
//! box.schema.user.grant('guest', 'execute', 'function', 'easy.easy2')
//! capi_connection:call('easy.easy2')
//! ```
//!
//! ... and this time the result will be `hello world -- easy2`.
//!
//! Conclusion: calling a Rust function is easy.
pub mod clock;
pub mod coio;
pub mod decimal;
pub mod error;
#[doc(hidden)]
pub mod ffi;
pub mod fiber;
pub mod index;
pub mod log;
pub mod net_box;
pub mod proc;
pub mod raft;
pub mod schema;
pub mod sequence;
pub mod session;
pub mod space;
pub mod transaction;
pub mod tuple;
pub mod util;
pub mod uuid;
#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
#[doc(hidden)]
mod va_list;

pub use tlua;
pub use tarantool_proc::stored_proc as proc;

/// Return a global tarantool lua state.
///
/// **WARNING:** using global lua state is error prone, especially when writing
/// code that will be executed in multiple fibers. Consider using [`lua_thread`]
/// instead. Use with caution if necessary.
fn global_lua() -> tlua::StaticLua {
    unsafe {
        tlua::Lua::from_static(ffi::tarantool::luaT_state())
    }
}

/// Create a new lua state with an isolated stack. The new state has access to
/// all the global and tarantool data (Lua variables, tables, modules, etc.).
pub fn lua_state() -> tlua::LuaThread {
    global_lua().new_thread()
}

pub use error::Result;
pub type StdResult<T, E> = std::result::Result<T, E>;
