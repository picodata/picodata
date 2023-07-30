use std::collections::HashSet;
use std::io::Write;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::panic::Location;
use std::path::Path;
use std::process::Command;
use std::process::Stdio;

// See also: https://doc.rust-lang.org/cargo/reference/build-scripts.html
fn main() {
    let jobserver = unsafe { jobserver::Client::from_env() };
    let jobserver = jobserver.as_ref();

    // The file structure roughly looks as follows:
    // .
    // ├── build.rs                         // you are here
    // ├── src/
    // ├── tarantool-sys
    // │   └── static-build
    // │       └── CMakeLists.txt
    // └── <target-dir>/<build-type>/build  // <- build_root
    //     ├── picodata-<smth>/out          // <- out_dir
    //     ├── tarantool-http
    //     └── tarantool-sys
    //         ├── ncurses-prefix
    //         ├── openssl-prefix
    //         ├── readline-prefix
    //         └── tarantool-prefix
    //
    let out_dir = std::env::var("OUT_DIR").unwrap();
    dbg!(&out_dir); // "<target-dir>/<build-type>/build/picodata-<smth>/out"

    // Running `cargo build` and `cargo clippy` produces 2 different
    // `out_dir` paths. This is stupid, we're not going to use them.
    let build_root = Path::new(&out_dir).parent().unwrap().parent().unwrap();
    dbg!(&build_root); // "<target-dir>/<build-type>/build"

    // See also:
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-build-scripts
    if let Some(ref makeflags) = std::env::var_os("CARGO_MAKEFLAGS") {
        std::env::set_var("MAKEFLAGS", makeflags);
    }

    #[allow(clippy::print_literal)]
    for (var, value) in std::env::vars() {
        println!("[{}:{}] {var}={value}", file!(), line!());
    }

    generate_export_stubs(&out_dir);
    build_tarantool(jobserver, build_root);
    build_http(jobserver, build_root);
    #[cfg(feature = "webui")]
    build_webui(build_root);

    println!("cargo:rerun-if-changed=tarantool-sys");
    println!("cargo:rerun-if-changed=http/http");
    #[cfg(feature = "webui")]
    rerun_if_webui_changed();
}

fn generate_export_stubs(out_dir: &str) {
    let mut symbols = HashSet::with_capacity(1024);
    let exports = std::fs::read_to_string("tarantool-sys/extra/exports").unwrap();
    read_symbols_into(&exports, &mut symbols);

    let exports = std::fs::read_to_string("tarantool-sys/extra/exports_libcurl").unwrap();
    read_symbols_into(&exports, &mut symbols);

    let exports = std::fs::read_to_string("src/sql/exports").unwrap();
    read_symbols_into(&exports, &mut symbols);

    let mut code = Vec::with_capacity(2048);
    writeln!(code, "pub fn export_symbols() {{").unwrap();
    writeln!(code, "    extern \"C\" {{").unwrap();
    for symbol in &symbols {
        writeln!(code, "        static {symbol}: *const ();").unwrap();
    }
    writeln!(code, "    }}").unwrap();
    // TODO: use std::hint::black_box, when we move to rust 1.66
    writeln!(code, "    fn black_box(_: *const ()) {{}}").unwrap();
    writeln!(code, "    unsafe {{").unwrap();
    for symbol in &symbols {
        writeln!(code, "        black_box({symbol});").unwrap();
    }
    writeln!(code, "    }}").unwrap();
    writeln!(code, "}}").unwrap();

    let gen_path = std::path::Path::new(out_dir).join("export_symbols.rs");
    std::fs::write(gen_path, code).unwrap();

    fn read_symbols_into<'a>(file_contents: &'a str, symbols: &mut HashSet<&'a str>) {
        for line in file_contents.lines() {
            let line = line.trim();
            if line.starts_with('#') {
                continue;
            }
            if line.is_empty() {
                continue;
            }
            symbols.insert(line);
        }
    }
}

#[cfg(feature = "webui")]
fn rerun_if_webui_changed() {
    use std::fs;

    let source_dir = std::env::current_dir().unwrap().join("picodata-webui");
    // Do not rerun for generated files changes
    let ignored_files = ["node_modules", ".husky"];
    for entry in fs::read_dir(&source_dir)
        .expect("failed to scan picodata-webui dir")
        .flatten()
    {
        if !ignored_files.contains(&entry.file_name().to_str().unwrap()) {
            println!(
                "cargo:rerun-if-changed=picodata-webui/{}",
                entry.file_name().to_string_lossy()
            );
        }
    }
}

#[cfg(feature = "webui")]
fn build_webui(build_root: &Path) {
    let source_dir = std::env::current_dir().unwrap().join("picodata-webui");
    let build_dir = build_root.join("picodata-webui");
    let build_dir_str = build_dir.display().to_string();

    Command::new("yarn")
        .arg("install")
        .arg("--prefer-offline")
        .arg("--frozen-lockfile")
        .arg("--no-progress")
        .arg("--non-interactive")
        .current_dir(&source_dir)
        .run();
    Command::new("yarn")
        .arg("vite")
        .arg("build")
        .args(["--outDir", &build_dir_str])
        .arg("--emptyOutDir")
        .current_dir(&source_dir)
        .run();
}

fn build_http(jsc: Option<&jobserver::Client>, build_root: &Path) {
    let build_dir = build_root.join("tarantool-http");
    let build_dir_str = build_dir.display().to_string();

    let tarantool_dir = build_root.join("tarantool-sys/tarantool-prefix");
    let tarantool_dir_str = tarantool_dir.display().to_string();

    Command::new("cmake")
        .args(["-S", "http"])
        .args(["-B", &build_dir_str])
        .arg(format!("-DTARANTOOL_DIR={tarantool_dir_str}"))
        .run();

    let mut cmd = Command::new("cmake");
    cmd.args(["--build", &build_dir_str]);
    if let Some(jsc) = jsc {
        jsc.configure(&mut cmd);
    }
    cmd.run();

    Command::new("ar")
        .arg("-rcs")
        .arg(build_dir.join("libhttpd.a"))
        .arg(build_dir.join("http/CMakeFiles/httpd.dir/lib.c.o"))
        .run();

    rustc::link_search(build_dir_str);
}

fn build_tarantool(jsc: Option<&jobserver::Client>, build_root: &Path) {
    let tarantool_sys = build_root.join("tarantool-sys");
    let tarantool_build = tarantool_sys.join("tarantool-prefix/src/tarantool-build");

    if !tarantool_build.exists() {
        // Build from scratch
        Command::new("cmake")
            .args(["-S", "tarantool-sys/static-build"])
            .arg("-B")
            .arg(&tarantool_sys)
            .arg(concat!(
                "-DCMAKE_TARANTOOL_ARGS=",
                "-DCMAKE_BUILD_TYPE=RelWithDebInfo;",
                "-DBUILD_TESTING=FALSE;",
                "-DBUILD_DOC=FALSE",
            ))
            .run();
        let mut cmd = Command::new("cmake");
        cmd.arg("--build").arg(&tarantool_sys);
        if let Some(jsc) = jsc {
            jsc.configure(&mut cmd);
        }
        cmd.run();
    } else {
        // static-build/CMakeFiles.txt builds tarantool via the ExternalProject
        // module, which doesn't rebuild subprojects if their contents changed,
        // therefore we dive into `tarantool-prefix/src/tarantool-build`
        // directly and try to rebuild it individually.
        let mut cmd = Command::new("cmake");
        cmd.arg("--build").arg(&tarantool_build);
        if let Some(jsc) = jsc {
            jsc.configure(&mut cmd);
        }
        cmd.run();
    }

    let tarantool_sys = tarantool_sys.display();
    let tarantool_build = tarantool_build.display();

    // Don't build a shared object in case it's the default for the compiler
    rustc::link_arg("-no-pie");

    for l in [
        "core",
        "small",
        "msgpuck",
        "vclock",
        "bit",
        "swim",
        "uri",
        "json",
        "http_parser",
        "mpstream",
        "raft",
        "csv",
        "bitset",
        "coll",
        "fakesys",
        "salad",
        "tzcode",
    ] {
        rustc::link_search(format!("{tarantool_build}/src/lib/{l}"));
        rustc::link_lib_static(l);
    }

    rustc::link_search(format!("{tarantool_build}/src/lib/crypto"));
    rustc::link_lib_static("tcrypto");

    rustc::link_search(format!("{tarantool_build}"));
    rustc::link_search(format!("{tarantool_build}/src"));
    rustc::link_search(format!("{tarantool_build}/src/box"));
    rustc::link_search(format!("{tarantool_build}/third_party/luajit/src"));
    rustc::link_search(format!("{tarantool_build}/third_party/libyaml"));
    rustc::link_search(format!("{tarantool_build}/third_party/c-dt/build"));
    rustc::link_search(format!("{tarantool_build}/build/nghttp2/dest/lib"));

    rustc::link_lib_static("tarantool");
    rustc::link_lib_static("ev");
    rustc::link_lib_static("coro");
    rustc::link_lib_static("cdt");
    rustc::link_lib_static("server");
    rustc::link_lib_static("misc");
    rustc::link_lib_static("nghttp2");
    rustc::link_lib_static("zstd");
    rustc::link_lib_static("decNumber");
    rustc::link_lib_static("eio");
    rustc::link_lib_static("box");
    rustc::link_lib_static("tuple");
    rustc::link_lib_static("xrow");
    rustc::link_lib_static("box_error");
    rustc::link_lib_static("xlog");
    rustc::link_lib_static("crc32");
    rustc::link_lib_static("stat");
    rustc::link_lib_static("shutdown");
    rustc::link_lib_static("swim_udp");
    rustc::link_lib_static("swim_ev");
    rustc::link_lib_static("symbols");
    rustc::link_lib_static("cpu_feature");
    rustc::link_lib_static("luajit");
    rustc::link_lib_static("yaml_static");
    rustc::link_lib_static("xxhash");

    // Add LDAP authentication support libraries.
    rustc::link_search(format!("{tarantool_build}/bundled-ldap-prefix/lib"));
    rustc::link_lib_static("ldap");
    rustc::link_lib_static("lber");
    rustc::link_search(format!("{tarantool_build}/bundled-sasl-prefix/lib"));
    rustc::link_lib_static("sasl2");

    if cfg!(target_os = "macos") {
        // Currently we link against 2 versions of `decNumber` library: one
        // comes with tarantool and ther other comes from the `dec` cargo crate.
        // On macos this seems to confuse the linker, which just chooses one of
        // the libraries and complains that it can't find symbols from the other.
        // So we add the second library explicitly via full path to the file.
        rustc::link_arg(format!("{tarantool_build}/libdecNumber.a"));

        // OpenMP and Libunwind are builtin to the compiler on macos
    } else {
        rustc::link_lib_static("gomp");

        // These two must be linked as positional arguments, because they define
        // duplicate symbols which is not allowed (by default) when linking with
        // via -l... option
        let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap();
        let lib_dir = format!("{tarantool_build}/third_party/libunwind/src/.libs");
        rustc::link_arg(format!("{lib_dir}/libunwind-{arch}.a"));
        rustc::link_arg(format!("{lib_dir}/libunwind.a"));
    }

    rustc::link_arg("-lc");

    rustc::link_search(format!("{tarantool_sys}/readline-prefix/lib"));
    rustc::link_lib_static("readline");

    rustc::link_search(format!("{tarantool_sys}/icu-prefix/lib"));
    rustc::link_lib_static("icudata");
    rustc::link_lib_static("icui18n");
    rustc::link_lib_static("icuio");
    rustc::link_lib_static("icutu");
    rustc::link_lib_static("icuuc");

    rustc::link_search(format!("{tarantool_sys}/zlib-prefix/lib"));
    rustc::link_lib_static("z");

    rustc::link_search(format!("{tarantool_build}/build/curl/dest/lib"));
    rustc::link_lib_static("curl");

    rustc::link_search(format!("{tarantool_build}/build/ares/dest/lib"));
    rustc::link_lib_static("cares");

    rustc::link_search(format!("{tarantool_sys}/openssl-prefix/lib"));
    rustc::link_lib_static("ssl");
    rustc::link_lib_static("crypto");

    rustc::link_search(format!("{tarantool_sys}/ncurses-prefix/lib"));
    rustc::link_lib_static("tinfo");

    rustc::link_search(format!("{tarantool_sys}/iconv-prefix/lib"));
    if cfg!(target_os = "macos") {
        // -lc++ instead of -lstdc++ on macos
        rustc::link_lib_dynamic("c++");

        // -lresolv on macos
        rustc::link_lib_dynamic("resolv");
    } else {
        // not supported on macos
        rustc::link_arg("-export-dynamic");
        rustc::link_lib_dynamic("stdc++");
    }
}

trait CommandExt {
    fn run(&mut self);
}

impl CommandExt for Command {
    #[track_caller]
    fn run(&mut self) {
        let loc = Location::caller();
        println!("[{}:{}] running [{:?}]", loc.file(), loc.line(), self);

        // Redirect stderr to stdout. This is needed to preserve the order of
        // error messages in the output, because otherwise cargo separates the
        // streams into 2 chunks destroying any hope of understanding when the
        // errors happened.
        let stdout_fd = std::io::stdout().as_raw_fd();
        let stdout_dup_fd = nix::unistd::dup(stdout_fd).expect("what could go wrong?");
        let stdout = unsafe { Stdio::from_raw_fd(stdout_dup_fd) };
        self.stderr(stdout);

        let prog = self.get_program().to_owned().into_string().unwrap();

        match self.status() {
            Ok(status) if status.success() => (),
            Ok(status) => panic!("{} failed: {}", prog, status),
            Err(e) => panic!("failed running `{}`: {}", prog, e),
        }
    }
}

mod rustc {
    pub fn link_search(path: impl AsRef<str>) {
        println!("cargo:rustc-link-search=native={}", path.as_ref());
    }

    pub fn link_lib_static(lib: impl AsRef<str>) {
        // See also:
        // https://doc.rust-lang.org/cargo/reference/build-scripts.html#cargorustc-link-liblib
        // https://doc.rust-lang.org/rustc/command-line-arguments.html#option-l-link-lib
        println!(
            "cargo:rustc-link-lib=static:+whole-archive,-bundle={}",
            lib.as_ref()
        );
    }

    pub fn link_lib_dynamic(lib: impl AsRef<str>) {
        println!("cargo:rustc-link-lib=dylib={}", lib.as_ref());
    }

    pub fn link_arg(arg: impl AsRef<str>) {
        println!("cargo:rustc-link-arg={}", arg.as_ref());
    }
}
