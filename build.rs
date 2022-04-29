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

    let status = std::process::Command::new("git")
        .current_dir("tarantool-sys")
        .arg("apply")
        .arg("--3way")
        .arg("--index")
        .args(patches)
        .status()
        .expect("git couldn't be executed");

    if !status.success() {
        panic!("failed to apply tarantool patches")
    }

    let _ = std::fs::File::create(&patch_check)
        .unwrap_or_else(|e| panic!("failed to create '{}': {}", patch_check.display(), e));
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
    std::fs::create_dir_all(&build_dir).expect("failed creating build directory");
    let dst = cmake::Config::new("tarantool-sys/static-build")
        .build_target("tarantool")
        .out_dir(build_dir)
        .build();

    let build_dir = dst.join("build/tarantool-prefix/src/tarantool-build");
    let build_disp = build_dir.display();

    // Don't build a shared object in case it's the default for the compiler
    println!("cargo:rustc-link-arg=-no-pie");

    for l in [
        "core",
        "small",
        "msgpuck",
        "crypto",
        "vclock",
        "uuid",
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
    ] {
        println!(
            "cargo:rustc-link-search=native={}/src/lib/{}",
            build_disp, l
        );
        println!("cargo:rustc-link-lib=static={}", l);
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
    for l in [
        "tarantool",
        "ev",
        "coro",
        "server",
        "misc",
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
        println!("cargo:rustc-link-lib=static={}", l);
    }

    if !cfg!(target_os = "macos") {
        // OpenMP is builtin to the compiler on macos
        println!("cargo:rustc-link-lib=static=gomp");

        // Libunwind is builtin to the compiler on macos
        //
        // These two must be linked as positional arguments, because they define
        // duplicate symbols which is not allowed (by default) when linking with
        // via -l... option
        println!(
            "cargo:rustc-link-arg={}/build/unwind-prefix/lib/libunwind-x86_64.a",
            dst.display()
        );
        println!(
            "cargo:rustc-link-arg={}/build/unwind-prefix/lib/libunwind.a",
            dst.display()
        );
    }

    println!("cargo:rustc-link-arg=-lc");

    println!(
        "cargo:rustc-link-search=native={}/build/readline-prefix/lib",
        dst.display()
    );
    println!("cargo:rustc-link-lib=static=readline");

    println!(
        "cargo:rustc-link-search=native={}/build/icu-prefix/lib",
        dst.display()
    );
    println!("cargo:rustc-link-lib=static=icudata");
    println!("cargo:rustc-link-lib=static=icui18n");
    println!("cargo:rustc-link-lib=static=icuio");
    println!("cargo:rustc-link-lib=static=icutu");
    println!("cargo:rustc-link-lib=static=icuuc");

    println!(
        "cargo:rustc-link-search=native={}/build/zlib-prefix/lib",
        dst.display()
    );
    println!("cargo:rustc-link-lib=static=z");

    println!(
        "cargo:rustc-link-search=native={}/build/curl/dest/lib",
        build_disp
    );
    println!("cargo:rustc-link-lib=static=curl");
    println!(
        "cargo:rustc-link-search=native={}/build/ares/dest/lib",
        build_disp
    );
    println!("cargo:rustc-link-lib=static=cares");

    println!(
        "cargo:rustc-link-search=native={}/build/openssl-prefix/lib",
        dst.display()
    );
    println!("cargo:rustc-link-lib=static=ssl");
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
    println!("cargo:rustc-link-lib=static=tinfo");

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
