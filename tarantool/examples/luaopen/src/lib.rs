#![allow(clippy::missing_safety_doc)]
//! This example describes how you can define a simple lua module in rust.
//!
//! To test this module do the following:
//!
//! 0. Compile the example
//! ```text
//! cargo build -p luaopen
//! ```
//!
//! 1. Setup the LUA_CPATH environment variable, e.g. on linux do
//! ```text
//! export LUA_CPATH=target/debug/lib?.so
//! ```
//!
//! 2. Start tarantool and run the following commands:
//! ```text
//! $ tarantool
//! Tarantool 2.11.1
//! type 'help' for interactive help
//!
//! tarantool> m = require 'luaopen'
//! you have called `require('luaopen')`, congrats!
//! ---
//! ...
//!
//! tarantool> m.say_hello_to('Bob')
//! ---
//! - Hello, Bob!
//! ...
//!
//! tarantool> m.foo
//! ---
//! - bar
//! ...
//! ```

use tarantool::tlua::{self, AsLua};

// This function is called, when the lua module is imported via the `require` function.
//
// It is marked `no_mangle` because by default rust modifies function names,
// but lua is looking for exactly the name `luaopen_<module-name>`. In our case
// the module is called `luaopen`.
//
// Note that there's multiple ways to call native code from tarantool:
// 1. Defining a lua module like this example shows
// 2. Loading the library directly using luajit's ffi module.
// 3. Defining a tarantool stored procedure, e.g. using the `#[proc]` attribute
// (see examples/easy)
//
// Tarantool's stored procedures are implemented via a separate system so if you
// use method 1. and 3. in the same library, than this library will be loaded
// twice. This also means that global data like rust static variables cannot
// be shared between native lua modules (or native ffi functions) and tarantool
// stored procedures. Lua modules with ffi functions don't share this limitation
// however.
#[no_mangle]
#[rustfmt::skip]
pub unsafe extern "C" fn luaopen_luaopen(lua: tlua::LuaState) -> i32 {
    println!("you have called `require('luaopen')`, congrats!");

    // Construct a Lua api object, so we can do stuff with it.
    let lua = tlua::Lua::from_static(lua);

    // Push a table onto the lua stack getting a guard value back.
    let guard = (&lua).push(tlua::AsTable((
        ("say_hello_to", tlua::Function::new(|name: String| -> String {
            format!("Hello, {name}!")
        })),
        ("foo", "bar"),
    )));

    // Normally the guard pops the values off the stack when drop is called on
    // it, but in this case we want the values returned from this function, so
    // we call `forget`, which leaves the value on the stack and returns the
    // number of values the guard was guarding, which lua interprets as the
    // number of return values.
    guard.forget()
}
