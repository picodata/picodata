pub mod admin;
pub mod args;
pub mod console;
pub mod default_config;
#[cfg(feature = "demo")]
pub mod demo;
pub mod expel;
pub mod plugin;
pub mod restore;
pub mod run;
pub mod status;
pub mod tarantool;
pub mod test;
pub mod util;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
