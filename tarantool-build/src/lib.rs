use build_rs_helpers::{
    cargo,
    cmake::{self, CmakeVariables},
    pkg_config, rustc, CommandExt,
};
use std::{path::PathBuf, process::Command};

pub struct TarantoolBuildRootOptions {
    /// In static build mode we build not only Tarantool, but also many
    /// of its dependencies, e.g. readline, curl, unwind, etc.
    pub use_static_build: bool,
    /// Should we use Debug or RelWithDebInfo?
    pub use_debug_build: bool,
}

impl TarantoolBuildRootOptions {
    pub fn build_profile(&self) -> &'static str {
        match self.use_debug_build {
            false => "RelWithDebInfo",
            true => "Debug",
        }
    }

    pub fn linkage(&self) -> &'static str {
        match self.use_static_build {
            false => "dynamic",
            true => "static",
        }
    }
}

/// This struct represents Tarantool's build directory.
/// NOTE: please **do not** add irrelevant fields here.
pub struct TarantoolBuildRoot {
    /// Path to Tarantool's topmost build directory.
    root: PathBuf,
    options: TarantoolBuildRootOptions,
}

impl TarantoolBuildRoot {
    pub fn new(build_root: impl Into<PathBuf>, options: TarantoolBuildRootOptions) -> Self {
        // The distinction between static & dynamic build paths
        // lets us keep both builds intact when we toggle
        // the "use_static_build" option.
        //
        // The current variants are:
        // - static.debug
        // - static.release
        // - dynamic.debug
        // - dynamic.release
        let root = build_root
            .into()
            .join("tarantool-sys")
            .join(options.linkage())
            // We don't want to use otions.build_profile() here just yet.
            // That would cause unnecessary rebuilds due to path changes.
            .with_extension(match options.use_debug_build {
                false => "release",
                true => "debug",
            });

        Self { root, options }
    }

    pub fn options(&self) -> &TarantoolBuildRootOptions {
        &self.options
    }

    /// A prefix for Tarantool's http server.
    pub fn http_prefix_dir(&self) -> PathBuf {
        self.root.join("tarantool-http")
    }

    /// In static build mode, root contains multiple prefixes
    /// for the projects we're going to build:
    ///
    /// - **tarantool-prefix**
    /// - readline-prefix
    /// - zlib-prefix
    /// - ...
    pub fn tarantool_prefix_dir(&self) -> PathBuf {
        if self.options.use_static_build {
            self.root.join("tarantool-prefix")
        } else {
            self.root.clone()
        }
    }

    /// It static build mode, tarantool-prefix in turn
    /// contains the **actual** tarantool build directory.
    pub fn tarantool_build_dir(&self) -> PathBuf {
        let prefix = self.tarantool_prefix_dir();
        if self.options.use_static_build {
            prefix.join("src/tarantool-build")
        } else {
            prefix
        }
    }
}

/// Build steps.
impl TarantoolBuildRoot {
    /// Build Tarantool and other libraries we're going to use.
    pub fn build_libraries(&self, jsc: Option<&cargo::MakeJobserverClient>) -> &Self {
        self.build_tarantool(jsc).build_http(jsc)
    }

    /// Build Tarantool's http server.
    /// NOTE: this **does not** affect picodata's link flags.
    /// Refer to [`Self::link_libraries`] for more information.
    fn build_http(&self, jsc: Option<&cargo::MakeJobserverClient>) -> &Self {
        cargo::rerun_if_changed("http/http");

        let tarantool_prefix = self.tarantool_prefix_dir();
        let http_prefix = self.http_prefix_dir();

        let mut configure_cmd = Command::new("cmake");
        configure_cmd
            .env("CMAKE_POLICY_VERSION_MINIMUM", "3.5") // >= 3.5 is required since 4.0
            .args(["-S", "http"])
            .arg("-B")
            .arg(&http_prefix)
            .arg(format!("-DTARANTOOL_DIR={}", tarantool_prefix.display()));

        if let Ok(name) = std::env::var("COMPILER_LAUNCHER") {
            configure_cmd.args([
                format!("-DCMAKE_C_COMPILER_LAUNCHER={}", name),
                format!("-DCMAKE_CXX_COMPILER_LAUNCHER={}", name),
            ]);
            cargo::warning(format!("set '{name}' compiler launcher for 'build_http'"));
        }

        configure_cmd.run();

        Command::new("cmake")
            .set_make_jobserver(jsc)
            .arg("--build")
            .arg(&http_prefix)
            .run();

        Command::new("ar")
            .arg("-rcs")
            .arg(http_prefix.join("libhttpd.a"))
            .arg(http_prefix.join("http/CMakeFiles/httpd.dir/lib.c.o"))
            .run();

        self
    }

    /// Build Tarantool itself.
    /// NOTE: this **does not** affect picodata's link flags.
    /// Refer to [`Self::link_libraries`] for more information.
    fn build_tarantool(&self, jsc: Option<&cargo::MakeJobserverClient>) -> &Self {
        cargo::rerun_if_changed("tarantool-sys");

        let use_static_build = self.options.use_static_build;
        let tarantool_build = self.tarantool_build_dir();
        let tarantool_root = &self.root;

        if !tarantool_build.exists() {
            // Build from scratch
            let mut configure_cmd = Command::new("cmake");
            configure_cmd.arg("-B").arg(tarantool_root);

            let mut common_args = vec![
                format!("-DCMAKE_BUILD_TYPE={}", self.options.build_profile()),
                "-DBUILD_TESTING=FALSE".to_string(),
                "-DBUILD_DOC=FALSE".to_string(),
            ];

            // Tarantool won't let us use gcc for an asan build.
            let profile = cargo::get_build_profile();
            if profile.starts_with("asan") {
                cargo::warning("ASan has been enabled, this may affect the performance");
                configure_cmd.envs([("CC", "clang"), ("CXX", "clang++")]);
                common_args.push("-DENABLE_ASAN=ON".to_string());
            }

            if let Ok(name) = std::env::var("COMPILER_LAUNCHER") {
                common_args.extend([
                    format!("-DCMAKE_C_COMPILER_LAUNCHER={}", name),
                    format!("-DCMAKE_CXX_COMPILER_LAUNCHER={}", name),
                ]);
                cargo::warning(format!(
                    "set '{name}' compiler launcher for 'build_tarantool'"
                ));
            }

            if use_static_build {
                // In pgproto, we use openssl crate that links to openssl from the system, so we want
                // tarantool to link to the same library. Otherwise there will be conflicts between
                // different library versions.
                //
                // In theory, the crate can use openssl from tarantool. There is a variable
                // OPENSSL_DIR that points to the directory with the library. But to use it we need
                // to build the crate after tarantool, while the order is not determined.
                // Thus, in practice, openssl crate builds before tarantool and throws an error that it
                // can't find the library because it wasn't built yet.
                common_args.push("-DOPENSSL_USE_STATIC_LIBS=FALSE".to_string());

                // static build is a separate project that uses CMAKE_TARANTOOL_ARGS
                // to forward parameters to tarantool cmake project
                configure_cmd
                    .args(["-S", "tarantool-sys/static-build"])
                    .arg(format!("-DCMAKE_TARANTOOL_ARGS={}", common_args.join(";")));
            } else {
                // Note: we only consider system curl if it's fresh enough.
                // See https://git.picodata.io/core/picodata/-/issues/1299.
                let have_system_curl = pkg_config()
                    .atleast_version("8.4.0")
                    .probe("libcurl")
                    .is_ok();

                // for dynamic build we do not use most of the bundled dependencies
                configure_cmd
                    .args(["-S", "tarantool-sys"])
                    .args(common_args)
                    .arg(format!(
                        "-DENABLE_BUNDLED_LIBCURL={}",
                        cmake::print_bool(!have_system_curl),
                    ))
                    .args([
                        "-DENABLE_BUNDLED_LDAP=OFF",
                        "-DENABLE_BUNDLED_LIBUNWIND=OFF",
                        "-DENABLE_BUNDLED_LIBYAML=OFF",
                        "-DENABLE_BUNDLED_OPENSSL=OFF",
                        "-DENABLE_BUNDLED_ZSTD=OFF",
                    ])
                    // for dynamic build we'll also need to install the project, so configure the prefix
                    .arg(format!(
                        "-DCMAKE_INSTALL_PREFIX={}",
                        tarantool_root.display(),
                    ));
            }

            configure_cmd.env("CMAKE_POLICY_VERSION_MINIMUM", "3.5"); // >= 3.5 is required since 4.0
            configure_cmd.run();
        }

        let mut build_cmd = Command::new("cmake");
        build_cmd
            // CONFIG_SITE is a path to a systemwide autoconf script, which may modify its
            // parameters. On OpenSUSE, running this script breaks our build, because it
            // modifies the (project-local!) paths to compiled libraries in submodules.
            .env_remove("CONFIG_SITE")
            .env("CMAKE_POLICY_VERSION_MINIMUM", "3.5") // >= 3.5 is required since 4.0
            .set_make_jobserver(jsc)
            .arg("--build")
            .arg(tarantool_root);
        // Must be passed after all normal flags.
        if !use_static_build {
            build_cmd.args(["--", "install"]);
        }
        build_cmd.run();

        self
    }
}

impl TarantoolBuildRoot {
    fn tarantool_cmake_variables(&self) -> CmakeVariables {
        let tarantool_src = "tarantool-sys";
        let tarantool_build = self.tarantool_build_dir();
        CmakeVariables::gather(tarantool_src, tarantool_build)
    }
}

/// Link steps.
impl TarantoolBuildRoot {
    /// Emit magic prints to link all libraries.
    pub fn link_libraries(&self) {
        self.link_tarantool();
        self.link_http();
    }

    /// Refer to `build.rs` for httpd's build steps.
    fn link_http(&self) {
        let http_prefix = self.http_prefix_dir();

        rustc::link_search(http_prefix);
        rustc::link_lib_static_whole_archive("httpd");
    }

    /// Refer to `build.rs` for tarantool's build steps.
    fn link_tarantool(&self) {
        let use_static_build = self.options.use_static_build;
        let tarantool_build = self.tarantool_build_dir();
        let tarantool_root = &self.root;

        let cmake_variables = self.tarantool_cmake_variables();
        let enable_backtrace = cmake_variables
            .get_bool("ENABLE_BACKTRACE")
            .expect("ENABLE_BACKTRACE is not found in cmake build");
        let enable_bundled_libcurl = cmake_variables
            .get_bool("ENABLE_BUNDLED_LIBCURL")
            .expect("ENABLE_BUNDLED_LIBCURL is not found in cmake build");

        let tarantool_libs = [
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
        ];
        for l in tarantool_libs {
            rustc::link_search(tarantool_build.join("src/lib").join(l));
            rustc::link_lib_static_whole_archive(l);
        }

        // {tarantool_build}
        rustc::link_search(&tarantool_build);
        rustc::link_lib_static_whole_archive("coro");
        rustc::link_lib_static_whole_archive("decNumber");
        rustc::link_lib_static_whole_archive("eio");
        rustc::link_lib_static_whole_archive("misc");
        rustc::link_lib_static_whole_archive("swim_ev");
        rustc::link_lib_static_whole_archive("swim_udp");
        rustc::link_lib_static_whole_archive("xxhash");

        // {tarantool_build}/src
        rustc::link_search(tarantool_build.join("src"));
        rustc::link_lib_static_whole_archive("cpu_feature");
        rustc::link_lib_static_whole_archive("crc32");
        rustc::link_lib_static_whole_archive("server");
        rustc::link_lib_static_whole_archive("shutdown");
        rustc::link_lib_static_whole_archive("stat");
        rustc::link_lib_static_whole_archive("symbols");
        rustc::link_lib_static_whole_archive("tarantool");

        // {tarantool_build}/src/box
        rustc::link_search(tarantool_build.join("src/box"));
        rustc::link_lib_static_whole_archive("box");
        rustc::link_lib_static_whole_archive("box_error");
        rustc::link_lib_static_whole_archive("tuple");
        rustc::link_lib_static_whole_archive("xlog");
        rustc::link_lib_static_whole_archive("xrow");

        rustc::link_search(tarantool_build.join("src/lib/crypto"));
        rustc::link_lib_static_whole_archive("tcrypto");

        rustc::link_search(tarantool_build.join("third_party/c-dt/build"));
        rustc::link_lib_static_whole_archive("cdt");

        rustc::link_search(tarantool_build.join("third_party/luajit/src"));
        rustc::link_lib_static_whole_archive("luajit");

        // libev
        if use_static_build {
            rustc::link_lib_static_whole_archive("ev");
        } else {
            rustc::link_lib_dynamic("ev");
        }

        // libzstd
        if use_static_build {
            rustc::link_lib_static_whole_archive("zstd");
        } else {
            rustc::link_lib_dynamic("zstd");
        }

        // libyaml
        if use_static_build {
            rustc::link_search(tarantool_build.join("build/libyaml/lib"));
            rustc::link_lib_static_whole_archive("yaml_static");
        } else {
            rustc::link_lib_dynamic("yaml");
        }

        // libreadline
        if use_static_build {
            rustc::link_search(tarantool_root.join("readline-prefix/lib"));
            rustc::link_lib_static_whole_archive("readline");

            // Static readline depends on tinfo.
            rustc::link_search(tarantool_root.join("ncurses-prefix/lib"));
            rustc::link_lib_static_whole_archive("tinfo");
        } else {
            // On macos readline is keg-only, so we should add it to search path.
            if cfg!(target_os = "macos") {
                rustc::link_search("/usr/local/opt/readline/lib");
            }
            rustc::link_lib_dynamic("readline");
        }

        // libiconv (macos adds -liconv automatically; not needed on linux but needed on freebsd)
        rustc::link_search(tarantool_root.join("iconv-prefix/lib"));
        if cfg!(target_os = "freebsd") {
            rustc::link_lib_static_whole_archive("iconv");
        }

        // libicu
        if use_static_build {
            rustc::link_search(tarantool_root.join("icu-prefix/lib"));
            rustc::link_lib_static_whole_archive("icudata");
            rustc::link_lib_static_whole_archive("icui18n");
            rustc::link_lib_static_whole_archive("icuio");
            rustc::link_lib_static_whole_archive("icutu");
            rustc::link_lib_static_whole_archive("icuuc");
        } else {
            // On macos icu4c is keg-only, so we should add it to search path.
            if cfg!(target_os = "macos") {
                rustc::link_search("/usr/local/opt/icu4c/lib");
            }
            rustc::link_lib_dynamic("icudata");
            rustc::link_lib_dynamic("icui18n");
            rustc::link_lib_dynamic("icuio");
            rustc::link_lib_dynamic("icutu");
            rustc::link_lib_dynamic("icuuc");
        }

        // libcurl
        if use_static_build || enable_bundled_libcurl {
            rustc::link_search(tarantool_build.join("build/curl/dest/lib"));
            rustc::link_lib_static_whole_archive("curl");

            // Static curl depends on nghttp2 & cares.
            rustc::link_search(tarantool_build.join("build/nghttp2/dest/lib"));
            rustc::link_lib_static_whole_archive("nghttp2");
            rustc::link_search(tarantool_build.join("build/ares/dest/lib"));
            rustc::link_lib_dynamic("cares");
        } else {
            rustc::link_lib_dynamic("curl");
        }

        // libz
        if use_static_build {
            rustc::link_search(tarantool_root.join("zlib-prefix/lib"));
            rustc::link_lib_static_whole_archive("z");
        } else {
            rustc::link_lib_dynamic("z");
        }

        // Add LDAP authentication support libraries.
        if use_static_build {
            rustc::link_search(tarantool_build.join("bundled-ldap-prefix/lib"));
            rustc::link_lib_static("ldap");
            rustc::link_lib_static("lber");
            rustc::link_search(tarantool_build.join("bundled-sasl-prefix/lib"));
            rustc::link_lib_static("sasl2");
        } else {
            rustc::link_lib_dynamic("sasl2");
            rustc::link_lib_dynamic("ldap");
        }

        // Previously in static build mode we used to link both libssl
        // and libcrypto statically. However with the addition of libssl
        // support to pgproto we needed to use Rust ssl bindings which
        // need to link with libssl too. To unify tarantool and rust side
        // of things we decided to always link libssl dynamically. When
        // libssl is linked dynamically there is no need to link libcrypto
        // because it is a dependency of libssl and is already present in
        // libssl.so:
        // > ldd /usr/lib64/libssl.so
        //     libcrypto.so.3 => /lib64/libcrypto.so.3
        //     ...
        //
        // If needed this is the way to link libssl statically:
        // rustc::link_lib_static("ssl");
        // rustc::link_lib_static("crypto");
        rustc::link_lib_dynamic("ssl");

        // macos links libunwind automatically.
        if enable_backtrace && !cfg!(target_os = "macos") {
            let arch = cargo::get_target_arch();
            if use_static_build {
                rustc::link_search(tarantool_build.join("third_party/libunwind/src/.libs"));
                rustc::link_lib_static(format!("unwind-{arch}"));
                rustc::link_lib_static("unwind");
            } else {
                rustc::link_lib_dynamic(format!("unwind-{arch}"));
                rustc::link_lib_dynamic("unwind");
            }
        }

        if cfg!(target_os = "macos") {
            rustc::link_lib_dynamic("resolv");
            rustc::link_lib_dynamic("c++");
        } else if cfg!(target_os = "freebsd") {
            rustc::link_lib_dynamic("c++");
        } else {
            rustc::link_lib_dynamic("stdc++");
        }

        rustc::link_arg("-lc");
    }
}
