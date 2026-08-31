use crate::rustflags;
use std::path::Path;

pub const TOOLCHAIN_ENV: [(&str, &str); 2] = [("CC", "clang"), ("CXX", "clang++")];

/// Clang flags which enable LLVM's source-based coverage for the C/C++ parts
/// of the build, see `tools/coverage.py` for the whole picture.
///
/// The tricky part is making clang embed the very same source file paths that
/// `rustc` does, otherwise `llvm-cov` will silently drop all C/C++ files from
/// the report (it's given `-path-equivalence=.,$PWD` plus a `.` source filter,
/// which only ever matches paths relative to the project's root).
pub fn coverage_flags() -> Vec<String> {
    let mut flags = vec![
        "-fprofile-instr-generate".to_owned(),
        "-fcoverage-mapping".to_owned(),
    ];

    let Some((from, to)) = rustflags::have_remap_path_prefix() else {
        return flags;
    };

    // `rustc` implements `--remap-path-prefix=$PWD=` by joining the remainder
    // of the path onto the (empty) replacement, which yields a relative path
    // like `src/main.rs`. Clang, on the other hand, performs a plain string
    // substitution and keeps the leading separator, so the same mapping would
    // turn `$PWD/tarantool-sys/src/box/box.cc` into `/tarantool-sys/src/box/box.cc`
    // -- still an absolute path, only now a bogus one. Use `.` instead of an
    // empty replacement to get a genuinely relative path.
    let to = to.as_os_str();
    let to = if to.is_empty() { ".".as_ref() } else { to };
    flags.push(format!(
        "-fcoverage-prefix-map={from}={to}",
        from = from.display(),
        to = Path::new(to).display(),
    ));

    // Coverage mapping stores the compilation directory alongside the file
    // name, and `llvm-cov` joins the two whenever the latter is relative.
    // Cmake compiles our C/C++ sources from within the build directory
    // (`target/cov/.../tarantool-build`), which would be prepended to every
    // path. Pin it to `.` so that the paths stay relative to `$PWD`.
    flags.push("-fcoverage-compilation-dir=.".to_owned());

    flags
}
