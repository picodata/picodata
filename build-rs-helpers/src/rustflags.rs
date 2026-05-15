pub use ::rustflags::*;

use std::path::PathBuf;

pub fn have_cfg(key: &str) -> bool {
    for flag in rustflags::from_env() {
        if let rustflags::Flag::Cfg { name, value: None } = flag {
            if name == key {
                return true;
            }
        }
    }

    false
}

pub fn have_codegen(key: &str) -> (bool, Option<String>) {
    for flag in rustflags::from_env() {
        if let rustflags::Flag::Codegen { opt, value } = flag {
            if opt == key {
                return (true, value);
            }
        }
    }

    (false, None)
}

/// Check if `RUSTFLAGS` have `--remap-path-prefix`. If so, return its value.
pub fn have_remap_path_prefix() -> Option<(PathBuf, PathBuf)> {
    for flag in rustflags::from_env() {
        if let rustflags::Flag::RemapPathPrefix { from, to } = flag {
            return Some((from, to));
        }
    }

    None
}

/// Check if code coverage is enabled by looking for `-C instrument-coverage` in `RUSTFLAGS`.
pub fn have_code_coverage() -> bool {
    have_codegen("instrument-coverage").0
}

/// Check if ASan is enabled by looking for `--cfg asan` in `RUSTFLAGS`.
pub fn have_asan() -> bool {
    have_cfg("asan")
}
