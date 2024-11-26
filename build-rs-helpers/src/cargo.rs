//! Various wrappers for cargo's facilities.

pub use jobserver::Client as MakeJobserverClient;

use super::*;

/// Cause `build.rs` to run again if an environment variable changes.
pub fn rerun_if_env_changed(name: impl AsRef<OsStr>) {
    println!(
        "cargo:rerun-if-env-changed={}",
        name.as_ref().to_str().unwrap()
    )
}

/// Cause `build.rs` to run again if a file (or a directory) changes.
pub fn rerun_if_changed(path: impl AsRef<OsStr>) {
    println!("cargo:rerun-if-changed={}", path.as_ref().to_str().unwrap())
}

/// Try connecting to a make jobserver, if any.
/// The main purpose is to limit concurrency at build time.
pub fn setup_make_jobserver() -> Option<MakeJobserverClient> {
    unsafe { jobserver::Client::from_env() }
}

/// Get target platform architecture (e.g. `x86_64`).
pub fn get_target_arch() -> String {
    std::env::var("CARGO_CFG_TARGET_ARCH").unwrap()
}

/// Check if a cargo feature is set (e.g. `--features=dynamic_build`).
pub fn get_feature(name: &str) -> bool {
    std::env::var(format!("CARGO_FEATURE_{}", name.to_uppercase())).is_ok()
}

/// If the package has a build script, this is set to the
/// folder where the build script should place its output.
pub fn get_out_dir() -> PathBuf {
    PathBuf::from(std::env::var_os("OUT_DIR").unwrap())
}

/// Path to a directory containing all build artifacts
/// for the current build profile.
pub fn build_root_from_out_dir(out_dir: impl Into<PathBuf>) -> PathBuf {
    let out_dir = out_dir.into();
    out_dir.ancestors().nth(2).unwrap().to_path_buf()
}

/// Path to a directory containing all build artifacts
/// for the current build profile.
pub fn get_build_root() -> PathBuf {
    let out_dir = get_out_dir();
    build_root_from_out_dir(out_dir)
}

/// Get target build profile (e.g. `debug`, `release`, `asan`).
pub fn get_build_profile() -> String {
    // The profile name is always the 3rd last part of the path (with 1 based indexing).
    let out_dir = get_out_dir();
    let ancestor_dir = out_dir.ancestors().nth(3).unwrap();
    ancestor_dir
        .file_name()
        .expect("missing file name")
        .to_str()
        .expect("bad file name")
        .to_owned()
}
