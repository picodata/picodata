use std::panic::Location;
use std::path::Path;
use std::process::Command;

fn main() {
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

    build_tarantool(build_root);
    build_http(build_root);

    println!("cargo:rerun-if-changed=tarantool-sys");
    println!("cargo:rerun-if-changed=http/http");
}

fn build_http(build_root: &Path) {
    let build_dir = build_root.join("tarantool-http");
    let build_dir_str = build_dir.display().to_string();

    let tarantool_dir = build_root.join("tarantool-sys/tarantool-prefix");
    let tarantool_dir_str = tarantool_dir.display().to_string();

    Command::new("cmake")
        .args(["-S", "http"])
        .args(["-B", &build_dir_str])
        .arg(format!("-DTARANTOOL_DIR={tarantool_dir_str}"))
        .run();

    Command::new("cmake")
        .args(["--build", &build_dir_str])
        .arg("-j")
        .run();

    Command::new("ar")
        .arg("-rcs")
        .arg(build_dir.join("libhttpd.a"))
        .arg(build_dir.join("http/CMakeFiles/httpd.dir/lib.c.o"))
        .run();

    rustc::link_search(build_dir_str);
}

fn build_tarantool(build_root: &Path) {
    let build_dir = build_root.join("tarantool-sys");
    let build_dir_str = build_dir.display().to_string();

    let tarantool_prefix = "tarantool-prefix/src/tarantool-build";

    if !build_dir.join(tarantool_prefix).exists() {
        // Build from scratch
        Command::new("cmake")
            .args(["-S", "tarantool-sys/static-build"])
            .args(["-B", &build_dir_str])
            .arg(concat!(
                "-DCMAKE_TARANTOOL_ARGS=",
                "-DCMAKE_BUILD_TYPE=RelWithDebInfo;",
                "-DBUILD_TESTING=FALSE;",
                "-DBUILD_DOC=FALSE",
            ))
            .run();
        Command::new("cmake")
            .args(["--build", &build_dir_str])
            .arg("-j")
            .run();
    } else {
        // static-build/CMakeFiles.txt builds tarantool via the ExternalProject
        // module, which doesn't rebuild subprojects if their contents changed,
        // therefore we dive into `tarantool-prefix/src/tarantool-build`
        // directly and try to rebuild it individually.
        let build_dir = build_dir.join(tarantool_prefix);
        let build_dir_str = build_dir.display().to_string();
        Command::new("cmake")
            .args(["--build", &build_dir_str])
            .arg("-j")
            .run();
    }

    let b = build_dir_str; // rename for shortness

    // Don't build a shared object in case it's the default for the compiler
    rustc::link_arg("-no-pie");

    for l in [
        "core",
        "small",
        "msgpuck",
        "crypto",
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
        rustc::link_search(format!("{b}/{tarantool_prefix}/src/lib/{l}"));
        rustc::link_lib_static(l);
    }

    rustc::link_search(format!("{b}/{tarantool_prefix}"));
    rustc::link_search(format!("{b}/{tarantool_prefix}/src"));
    rustc::link_search(format!("{b}/{tarantool_prefix}/src/box"));
    rustc::link_search(format!("{b}/{tarantool_prefix}/third_party/luajit/src"));
    rustc::link_search(format!("{b}/{tarantool_prefix}/third_party/libyaml"));
    rustc::link_search(format!("{b}/{tarantool_prefix}/third_party/c-dt/build"));
    rustc::link_search(format!("{b}/{tarantool_prefix}/build/nghttp2/dest/lib"));

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
    rustc::link_lib_static("scramble");
    rustc::link_lib_static("stat");
    rustc::link_lib_static("shutdown");
    rustc::link_lib_static("swim_udp");
    rustc::link_lib_static("swim_ev");
    rustc::link_lib_static("cpu_feature");
    rustc::link_lib_static("luajit");
    rustc::link_lib_static("yaml_static");

    if cfg!(target_os = "macos") {
        // OpenMP and Libunwind are builtin to the compiler on macos
    } else {
        rustc::link_lib_static("gomp");

        // These two must be linked as positional arguments, because they define
        // duplicate symbols which is not allowed (by default) when linking with
        // via -l... option
        let lib_dir = format!("{b}/{tarantool_prefix}/build/libunwind/dest/lib");
        rustc::link_arg(format!("{lib_dir}/libunwind-x86_64.a"));
        rustc::link_arg(format!("{lib_dir}/libunwind.a"));
    }

    rustc::link_arg("-lc");

    rustc::link_search(format!("{b}/readline-prefix/lib"));
    rustc::link_lib_static("readline");

    rustc::link_search(format!("{b}/icu-prefix/lib"));
    rustc::link_lib_static("icudata");
    rustc::link_lib_static("icui18n");
    rustc::link_lib_static("icuio");
    rustc::link_lib_static("icutu");
    rustc::link_lib_static("icuuc");

    rustc::link_search(format!("{b}/zlib-prefix/lib"));
    rustc::link_lib_static("z");

    rustc::link_search(format!("{b}/{tarantool_prefix}/build/curl/dest/lib"));
    rustc::link_lib_static("curl");

    rustc::link_search(format!("{b}/{tarantool_prefix}/build/ares/dest/lib"));
    rustc::link_lib_static("cares");

    rustc::link_search(format!("{b}/openssl-prefix/lib"));
    rustc::link_lib_static("ssl");

    // This one must be linked as a positional argument, because -lcrypto
    // conflicts with `src/lib/crypto/libcrypto.a`
    // duplicate symbols which is not allowed (by default) when linking with via
    rustc::link_arg(format!("{b}/openssl-prefix/lib/libcrypto.a"));

    rustc::link_search(format!("{b}/ncurses-prefix/lib"));
    rustc::link_lib_static("tinfo");

    rustc::link_search(format!("{b}/iconv-prefix/lib"));
    if cfg!(target_os = "macos") {
        // -lc++ instead of -lstdc++ on macos
        rustc::link_lib_dynamic("c++");
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
        eprintln!("[{}:{}] running [{:?}]", loc.file(), loc.line(), self);

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
        println!(
            "cargo:rustc-link-lib=static:+whole-archive={}",
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
