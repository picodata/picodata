use std::path::Path;

fn main() {
    patch_tarantool();
    build_tarantool();
}

fn patch_tarantool() {
    let status = std::process::Command::new("git")
        .current_dir("tarantool-sys")
        .arg("apply")
        .arg("--3way")
        .arg("--index")
        .args(
            std::fs::read_dir("tarantool-patches")
                .expect("failed reading tarantool-patches")
                .map(|de| de.unwrap_or_else(|e| panic!("Failed reading directory entry: {}", e)))
                .map(|de| de.path())
                .filter(|f| f.extension().map(|e| e == "patch").unwrap_or(false))
                .map(|f| Path::new("..").join(f)),
        )
        .status()
        .expect("git couldn't be executed");

    if !status.success() {
        panic!("failed to apply tarantool patches")
    }
}

fn build_tarantool() {
    let dst = cmake::Config::new("tarantool-sys/static-build")
        .build_target("tarantool")
        .build();

    let build_dir = dst.join("build/tarantool-prefix/src/tarantool-build");
    let build_disp = build_dir.display();

    // static-build/CMakeFiles.txt uses builds tarantool via the ExternalProject
    // module, which doesn't rebuild tarantool if something is changed,
    // therefore we do `cmake --build tarantool-prefix/src/tarantool-build`
    // directly.
    // XXX: this should only be done if the above command did not rebuild
    // anything
    let status = std::process::Command::new("cmake")
        .arg("--build")
        .arg(build_disp.to_string())
        .arg("-j")
        .status()
        .expect("cmake couldn't be executed");

    if !status.success() {
        panic!("cmake failed")
    }

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
        "gomp",
    ] {
        println!("cargo:rustc-link-lib=static={}", l);
    }

    // These two must be linked as positional arguments, because they define
    // duplicate symbols which is not allowed (by default) when linking with via
    // -l... option
    println!(
        "cargo:rustc-link-arg={}/build/unwind-prefix/lib/libunwind-x86_64.a",
        dst.display()
    );
    println!(
        "cargo:rustc-link-arg={}/build/unwind-prefix/lib/libunwind.a",
        dst.display()
    );
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

    println!("cargo:rustc-link-lib=dylib=stdc++");

    println!("cargo:rustc-link-arg=-export-dynamic");
}
