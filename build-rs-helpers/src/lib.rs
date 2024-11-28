//! A toolkit for writing rust's build scripts (`build.rs`).
//!
//! See also:
//! * <https://doc.rust-lang.org/cargo/reference/build-scripts.html>
//! * <https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-build-scripts>

pub mod cargo;
pub mod cmake;
pub mod exports;
pub mod rustc;

use std::{ffi::OsStr, panic::Location, path::PathBuf, process::Command};

pub trait CommandExt {
    /// Configure the command to use make jobserver to limit (unwanted) concurrency.
    fn set_make_jobserver(&mut self, jsc: Option<&cargo::MakeJobserverClient>) -> &mut Self;

    /// Run the command.
    fn run(&mut self);
}

impl CommandExt for Command {
    fn set_make_jobserver(&mut self, jsc: Option<&cargo::MakeJobserverClient>) -> &mut Self {
        if let Some(jsc) = jsc {
            jsc.configure_make(self);
        }

        self
    }

    #[track_caller]
    fn run(&mut self) {
        let loc = Location::caller();
        println!("[{}:{}] running [{:?}]", loc.file(), loc.line(), self);

        // Redirect stderr to stdout. This is needed to preserve the order of
        // error messages in the output, because otherwise cargo separates the
        // streams into 2 chunks destroying any hope of understanding when the
        // errors happened.
        self.stderr(std::io::stdout());

        let prog = self.get_program().to_str().unwrap().to_owned();
        match self.status() {
            Ok(status) if status.success() => (),
            Ok(status) => panic!("{prog} failed: {status}"),
            Err(e) => panic!("failed running `{prog}`: {e}"),
        }
    }
}

/// Print all env variables for debug purposes.
pub fn debug_print_env() {
    #[allow(clippy::print_literal)]
    for (var, value) in std::env::vars() {
        println!("[{}:{}] {var}={value}", file!(), line!());
    }
}
