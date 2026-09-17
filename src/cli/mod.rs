pub mod admin;
pub mod args;
pub mod console;
pub mod default_config;
#[cfg(feature = "demo")]
pub mod demo;
pub mod expel;
// The `picodata export` command and reading the catalog over pgproto come
// separately; until then only the dump rendering is here, with no caller.
#[expect(dead_code)]
pub mod export;
pub mod plugin;
pub mod restore;
pub mod run;
pub mod status;
pub mod tarantool;
pub mod test;
pub mod util;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
