use std::path::Path;

fn main() {
    patch_tarantool();
    build_tarantool();
    println!("cargo:rerun-if-changed=tarantool-sys");
}

fn patch_tarantool() {
    let patch_check = Path::new("tarantool-sys/patches-applied");
    if patch_check.exists() {
        println!(
            "cargo:warning='{}' exists, so patching step is skipped",
            patch_check.display()
        );
        return;
    }

    let mut patches = std::fs::read_dir("tarantool-patches")
        .expect("failed reading tarantool-patches")
        .map(|de| de.unwrap_or_else(|e| panic!("Failed reading directory entry: {}", e)))
        .map(|de| de.path())
        .filter(|f| f.extension().map(|e| e == "patch").unwrap_or(false))
        .map(|f| Path::new("..").join(f))
        .collect::<Vec<_>>();
    patches.sort();

    for patch in &patches {
        dbg!(patch);
        let status = std::process::Command::new("patch")
            .current_dir("tarantool-sys")
            .arg("--forward")
            .arg("-p1")
            .arg("-i")
            .arg(patch)
            .status()
            .expect("`patch` couldn't be executed");

        if !status.success() {
            panic!("failed to apply tarantool patches")
        }
    }

    let _ = std::fs::File::create(&patch_check)
        .unwrap_or_else(|e| panic!("failed to create '{}': {}", patch_check.display(), e));
}

fn version() -> (u32, u32) {
    let version_bytes = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .unwrap()
        .stdout;
    let version_line = String::from_utf8(version_bytes).unwrap();
    let mut version_line_parts = version_line.split_whitespace();
    let rustc = version_line_parts.next().unwrap();
    assert_eq!(rustc, "rustc");
    let version = version_line_parts.next().unwrap();
    let mut version_parts = version.split('.');
    let major = version_parts.next().unwrap().parse().unwrap();
    let minor = version_parts.next().unwrap().parse().unwrap();
    (major, minor)
}

fn build_tarantool() {
    // $OUT_DIR = ".../target/<build-type>/build/picodata-<smth>/out"
    let out_dir = std::env::var("OUT_DIR").unwrap();
    // cargo creates 2 different output directories when running `cargo build`
    // and `cargo clippy`, which is stupid, so we're not going to use them
    //
    // build_dir = ".../target/<build-type>/build/tarantool-sys"
    let build_dir = Path::new(&out_dir)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("tarantool-sys");

    if build_dir.exists() {
        // static-build/CMakeFiles.txt builds tarantool via the ExternalProject
        // module, which doesn't rebuild subprojects if their contents changed,
        // therefore we do `cmake --build tarantool-prefix/src/tarantool-build`
        // directly, to try and rebuild tarantool-sys.
        let status = std::process::Command::new("cmake")
            .arg("--build")
            .arg(build_dir.join("build/tarantool-prefix/src/tarantool-build"))
            .arg("-j")
            .status()
            .expect("cmake couldn't be executed");

        if !status.success() {
            panic!("cmake failed")
        }
    }

    std::fs::create_dir_all(&build_dir).expect("failed creating build directory");
    let dst = cmake::Config::new("tarantool-sys/static-build")
        .build_target("tarantool")
        .out_dir(build_dir)
        .build();

    let build_dir = dst.join("build/tarantool-prefix/src/tarantool-build");
    let build_disp = build_dir.display();

    // Don't build a shared object in case it's the default for the compiler
    println!("cargo:rustc-link-arg=-no-pie");

    let link_static_flag = if version() < (1, 61) {
        "cargo:rustc-link-lib=static"
    } else {
        "cargo:rustc-link-lib=static:+whole-archive"
    };

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
