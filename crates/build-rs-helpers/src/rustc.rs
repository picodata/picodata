//! See also:
//! <https://doc.rust-lang.org/cargo/reference/build-scripts.html#rustc-link-lib>
//! <https://doc.rust-lang.org/rustc/command-line-arguments.html#option-l-link-lib>

use super::*;

/// Set an environment variable for rustc.
pub fn env(name: impl AsRef<OsStr>, value: impl AsRef<OsStr>) {
    println!(
        "cargo:rustc-env={}={}",
        name.as_ref().to_str().unwrap(),
        value.as_ref().to_str().unwrap()
    );
}

/// Add a search path for native libraries (static or dynamic).
pub fn link_search(path: impl AsRef<OsStr>) {
    println!(
        "cargo:rustc-link-search=native={}",
        path.as_ref().to_str().unwrap()
    );
}

/// Link a static library, **including every object file** in the archive.
/// This is normally used to turn an archive file into a shared library.
pub fn link_lib_static_whole_archive(lib: impl AsRef<OsStr>) {
    println!(
        "cargo:rustc-link-lib=static:+whole-archive,-bundle={}",
        lib.as_ref().to_str().unwrap()
    );
}

/// Link a static library, **discarding all unused object files**.
/// This is the default mode for static library archives.
pub fn link_lib_static(lib: impl AsRef<OsStr>) {
    println!(
        "cargo:rustc-link-lib=static:-bundle={}",
        lib.as_ref().to_str().unwrap()
    );
}

/// Link a dynamic library.
/// For certain libraries (e.g. ssl) we always use this mode.
pub fn link_lib_dynamic(lib: impl AsRef<OsStr>) {
    println!(
        "cargo:rustc-link-lib=dylib={}",
        lib.as_ref().to_str().unwrap()
    );
}

/// [`link_arg`], but for the specific binary only (excludes tests etc).
pub fn link_arg_bin(bin: impl AsRef<str>, arg: impl AsRef<str>) {
    println!("cargo:rustc-link-arg-bin={}={}", bin.as_ref(), arg.as_ref());
}

/// Add a linker argument to **all targets**: binary, tests, etc.
pub fn link_arg(arg: impl AsRef<OsStr>) {
    println!("cargo:rustc-link-arg={}", arg.as_ref().to_str().unwrap());
}
