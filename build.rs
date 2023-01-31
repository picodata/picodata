use std::panic::Location;
use std::path::Path;
use std::process::Command;

fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    dbg!(&out_dir); // "$PWD/target/<build-type>/build/picodata-<smth>/out"

    // Running `cargo build` and `cargo clippy` produces 2 different
    // `out_dir` paths. This is stupid, we're not going to use them.
    let build_root = Path::new(&out_dir).parent().unwrap().parent().unwrap();
    dbg!(&build_root); // "$PWD/target/<build-type>/build"

    build_tarantool(build_root);
    build_http(build_root);

    println!("cargo:rerun-if-changed=tarantool-sys");
    println!("cargo:rerun-if-changed=http/http");
}

fn build_http(build_root: &Path) {
    let build_dir = build_root.join("tarantool-http");
    let build_dir_str = build_dir.display().to_string();

    let tarantool_dir = build_root.join("tarantool-sys/build/tarantool-prefix");
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

    println!("cargo:rustc-link-search=native={build_dir_str}");
}

fn build_tarantool(build_root: &Path) {
    let build_dir = build_root.join("tarantool-sys/build");
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

    // A temporary alias to reduce the commit diff
    // TODO (rosik): refactor in the next commit
    let dst = build_dir.parent().unwrap(); // this is what cmake crate used to do

    let build_dir = dst.join("build/tarantool-prefix/src/tarantool-build");
    let build_disp = build_dir.display();

    // Don't build a shared object in case it's the default for the compiler
    println!("cargo:rustc-link-arg=-no-pie");

    let link_static_flag = "cargo:rustc-link-lib=static:+whole-archive";

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
        println!(
            "cargo:rustc-link-search=native={}/src/lib/{}",
            build_disp, l
        );
        println!("{}={}", link_static_flag, l);
    }

    println!("cargo:rustc-link-search=native={}", build_disp);
    println!("cargo:rustc-link-search=native={}/src", build_disp);
    println!("cargo:rustc-link-search=native={}/src/box", build_disp);
    println!(
        "cargo:rustc-link-search=native={}/third_party/luajit/src",
        build_disp
    );
    println!(
        "cargo:rustc-link-search=native={}/third_party/libyaml",
        build_disp
    );
    println!("cargo:rustc-link-search=native={build_disp}/third_party/c-dt/build");
    println!("cargo:rustc-link-search=native={build_disp}/build/nghttp2/dest/lib");
    for l in [
        "tarantool",
        "ev",
        "coro",
        "cdt",
        "server",
        "misc",
        "nghttp2",
        "zstd",
        "decNumber",
        "eio",
        "box",
        "tuple",
        "xrow",
        "box_error",
        "xlog",
        "crc32",
        "scramble",
        "stat",
        "shutdown",
        "swim_udp",
        "swim_ev",
        "cpu_feature",
        "luajit",
        "yaml_static",
    ] {
        println!("{}={}", link_static_flag, l);
    }

    if !cfg!(target_os = "macos") {
        // OpenMP is builtin to the compiler on macos
        println!("{}=gomp", link_static_flag);

        // Libunwind is builtin to the compiler on macos
        //
        // These two must be linked as positional arguments, because they define
        // duplicate symbols which is not allowed (by default) when linking with
        // via -l... option
        println!(
            "cargo:rustc-link-arg={}/build/libunwind/dest/lib/libunwind-x86_64.a",
            build_disp
        );
        println!(
            "cargo:rustc-link-arg={}/build/libunwind/dest/lib/libunwind.a",
            build_disp
        );
    }

    println!("cargo:rustc-link-arg=-lc");

    println!(
        "cargo:rustc-link-search=native={}/build/readline-prefix/lib",
        dst.display()
    );
    println!("{}=readline", link_static_flag);

    println!(
        "cargo:rustc-link-search=native={}/build/icu-prefix/lib",
        dst.display()
    );
    println!("{}=icudata", link_static_flag);
    println!("{}=icui18n", link_static_flag);
    println!("{}=icuio", link_static_flag);
    println!("{}=icutu", link_static_flag);
    println!("{}=icuuc", link_static_flag);

    println!(
        "cargo:rustc-link-search=native={}/build/zlib-prefix/lib",
        dst.display()
    );
    println!("{}=z", link_static_flag);

    println!(
        "cargo:rustc-link-search=native={}/build/curl/dest/lib",
        build_disp
    );
    println!("{}=curl", link_static_flag);
    println!(
        "cargo:rustc-link-search=native={}/build/ares/dest/lib",
        build_disp
    );
    println!("{}=cares", link_static_flag);

    println!(
        "cargo:rustc-link-search=native={}/build/openssl-prefix/lib",
        dst.display()
    );
    println!("{}=ssl", link_static_flag);
    // This one must be linked as a positional argument, because -lcrypto
    // conflicts with `src/lib/crypto/libcrypto.a`
    // duplicate symbols which is not allowed (by default) when linking with via
    println!(
        "cargo:rustc-link-arg={}/build/openssl-prefix/lib/libcrypto.a",
        dst.display()
    );

    println!(
        "cargo:rustc-link-search=native={}/build/ncurses-prefix/lib",
        dst.display()
    );
    println!("{}=tinfo", link_static_flag);

    println!(
        "cargo:rustc-link-search=native={}/build/iconv-prefix/lib",
        dst.display()
    );

    if cfg!(target_os = "macos") {
        // -lc++ instead of -lstdc++ on macos
        println!("cargo:rustc-link-lib=dylib=c++");
    } else {
        // not supported on macos
        println!("cargo:rustc-link-arg=-export-dynamic");
        println!("cargo:rustc-link-lib=dylib=stdc++");
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
