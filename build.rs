use build_rs_helpers::{cargo, rustc, CommandExt};
use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    path::{Path, PathBuf},
    process::Command,
};
use tarantool_build::{TarantoolBuildRoot, TarantoolBuildRootOptions};

fn main() {
    // Prevent linkage problems due to non-pie static archives.
    // TODO: drop this since it's less than ideal from a security standpoint.
    rustc::link_arg("-no-pie");

    // Initialize make jobserver to limit unwanted concurrency.
    let jobserver = cargo::setup_make_jobserver();
    let jobserver = jobserver.as_ref();

    let build_root = cargo::get_build_root();
    let use_static_build = !cargo::get_feature("dynamic_build");
    let use_debug_build = cargo::get_feature("debug_tarantool");

    let tarantool_build_root = TarantoolBuildRoot::new(
        &build_root,
        TarantoolBuildRootOptions {
            use_static_build,
            use_debug_build,
        },
    );
    // Build and link all the relevant tarantool libraries.
    // For more info, read the comments in tarantool-build.
    tarantool_build_root
        .build_libraries(jobserver)
        .link_libraries();

    generate_git_version();
    export_public_symbols();
    check_plugins_ffi();

    if cfg!(feature = "webui") {
        build_webui(&build_root);
    }

    // XXX: Currently, picodata's linkage is defined by tarantool.
    rustc::env("PICO_LINKAGE", tarantool_build_root.options().linkage());

    rustc::env("PICO_BUILD_PROFILE", std::env::var("PROFILE").unwrap());
    rustc::env(
        "TNT_BUILD_PROFILE",
        tarantool_build_root.options().build_profile(),
    );

    let rustflags_file = save_rustflags();
    rustc::env("RUSTFLAGS_FILE", rustflags_file);

    let cargo_cfg_file = save_cargo_cfg();
    rustc::env("CARGO_CFG_FILE", cargo_cfg_file);

    let cargo_feature_file = save_cargo_feature();
    rustc::env("CARGO_FEATURE_FILE", cargo_feature_file);
}

fn save_rustflags() -> PathBuf {
    let mut data = Vec::new();
    for flag in rustflags::from_env() {
        match flag {
            // E.g. `#[cfg(target_os = "macos")]`.
            rustflags::Flag::Cfg { name, value } => match value {
                Some(value) => data.push(format!("--cfg {name}={value}")),
                None => data.push(format!("--cfg {name}")),
            },
            // E.g. `-C opt-level=3`.
            rustflags::Flag::Codegen { opt, value } => match value {
                Some(value) => data.push(format!("-C {opt}={value}")),
                None => data.push(format!("-C {opt}")),
            },
            // E.g. `-Z sanitizer=address`.
            rustflags::Flag::Z(opt) => data.push(format!("-Z {opt}")),
            // Currently, we're not interested in other kinds of flags.
            // Consider taking a look at `rustflags::Flag` for more info.
            _ => {}
        };
    }

    let contents = serde_json::to_string(&data).unwrap();
    let file = cargo::get_out_dir().join("rustflags");
    std::fs::write(&file, contents).unwrap();
    file
}

fn save_env_matching(file: &str, prefix: &str) -> PathBuf {
    let mut data = HashMap::new();

    for (name, value) in std::env::vars_os() {
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(value) = value.to_str() else {
            continue;
        };

        if name.starts_with(prefix) {
            data.insert(name.to_owned(), value.to_owned());
        }
    }

    let contents = serde_json::to_string(&data).unwrap();
    let file = cargo::get_out_dir().join(file);
    std::fs::write(&file, contents).unwrap();
    file
}

fn save_cargo_cfg() -> PathBuf {
    save_env_matching("cargo_cfg", "CARGO_CFG_")
}

fn save_cargo_feature() -> PathBuf {
    save_env_matching("cargo_feature", "CARGO_FEATURE_")
}

fn generate_git_version() {
    rustc::env(
        "GIT_DESCRIBE",
        git_version::git_version!(
            args = [], // disable --always flag
            fallback = std::env::var("GIT_DESCRIBE")
                .expect("failed to get version from git and GIT_DESCRIBE env")
        ),
    );
}

fn export_public_symbols() {
    let exports = [
        "exports_picodata",
        "tarantool-sys/extra/exports",
        "tarantool-sys/extra/exports_libcurl",
    ];

    let mut symbols = HashSet::<String>::new();
    for f in exports {
        cargo::rerun_if_changed(f);
        build_rs_helpers::exports::read_file(f, &mut symbols).unwrap();
    }

    // Add extra symbols for ASan (tarantool/src/lua/utils.lua).
    let profile = cargo::get_build_profile();
    if profile.starts_with("asan") {
        symbols.extend([
            "__asan_unpoison_memory_region".into(),
            "__asan_poison_memory_region".into(),
            "__asan_address_is_poisoned".into(),
        ]);
    }

    // Sorted symbols file is much easier to navigate.
    let mut symbols: Vec<_> = symbols.into_iter().collect();
    symbols.sort();

    let exports_file = cargo::get_out_dir().join("combined-exports");
    build_rs_helpers::exports::write_file(&exports_file, symbols).unwrap();

    // Export symbols only from the main binary (i.e. not tests etc).
    let exports_file = exports_file.display();
    if cfg!(target_os = "macos") {
        rustc::link_arg_bin(
            "picodata",
            format!("-Wl,-exported_symbols_list,{exports_file}"),
        );
    } else {
        #[rustfmt::skip]
        rustc::link_arg_bin(
            "picodata",
            format!("-Wl,--dynamic-list,{exports_file}"),
        );
    }
}

fn check_plugins_ffi() {
    let definitions_filename = "src/plugin/ffi.rs";
    cargo::rerun_if_changed(definitions_filename);
    let declarations_filename = "picodata-plugin/src/internal/ffi.rs";
    cargo::rerun_if_changed(declarations_filename);

    if std::env::var("SKIP_FFI_CHECK").is_ok() {
        // Must exist only after the `rerun-if-changed` directives
        return;
    }

    let definitions_source = std::fs::read_to_string(definitions_filename).unwrap();
    let Ok(definitions_ast) = syn::parse_file(&definitions_source) else {
        // Parse errors will be reported by the compiler, just bail out ASAP
        return;
    };

    let declarations_source = std::fs::read_to_string(declarations_filename).unwrap();
    let Ok(declarations_ast) = syn::parse_file(&declarations_source) else {
        // Parse errors will be reported by the compiler, just bail out ASAP
        return;
    };

    let mut definitions = HashMap::new();
    for item in definitions_ast.items {
        let syn::Item::Fn(f) = item else {
            continue;
        };
        let Some(abi) = &f.sig.abi else {
            continue;
        };
        let Some(abi_name) = &abi.name else {
            continue;
        };
        if abi_name.value() != "C" {
            continue;
        }

        let name = f.sig.ident.to_string();
        definitions.insert(name, f);
    }

    let mut declarations = HashMap::new();
    for item in declarations_ast.items {
        let syn::Item::ForeignMod(m) = item else {
            continue;
        };
        for foreign_item in m.items {
            let syn::ForeignItem::Fn(f) = foreign_item else {
                continue;
            };
            let name = f.sig.ident.to_string();
            declarations.insert(name, f);
        }
    }

    for (name, decl) in &declarations {
        let Some(def) = definitions.get(name) else {
            let line = decl.sig.ident.span().start().line;
            cargo::warning(format!("{declarations_filename}:{line}: found a declaration for `fn {name}` which is not defined in file {definitions_filename}"));
            std::process::exit(1);
        };

        // Compare ignoring ABI because only definition specifies it
        let mut def_sig_no_abi = def.sig.clone();
        def_sig_no_abi.abi.take();

        if def_sig_no_abi != decl.sig {
            let def_sig = &def.sig;
            let def_line = def.sig.ident.span().start().line;
            let def_sig = quote::quote! { #def_sig }.to_string();

            let decl_sig = &decl.sig;
            let decl_line = decl.sig.ident.span().start().line;
            let decl_sig = quote::quote! { #decl_sig }.to_string();
            cargo::warning(format!("Signature mismatch for `fn {name}`"));
            println!(
                "
--> {definitions_filename}:{def_line}
|
| {def_sig}

--> {declarations_filename}:{decl_line}
|
| {decl_sig}
    "
            );
            std::process::exit(1);
        }
    }
}

pub fn build_webui(build_root: &Path) {
    let src_dir = PathBuf::from("webui");
    let out_dir = build_root.join("webui");

    cargo::rerun_if_env_changed("WEBUI_BUNDLE");
    for entry in std::fs::read_dir(&src_dir).unwrap() {
        // Do not rerun for generated files changes
        let entry = entry.unwrap();
        let ignored_files = ["node_modules", ".husky"].map(OsString::from);
        if !ignored_files.contains(&entry.file_name()) {
            cargo::rerun_if_changed(entry.path());
        }
    }

    if std::env::var_os("WEBUI_BUNDLE").is_some() {
        println!("building webui_bundle skipped");
        return;
    }

    println!("building webui_bundle ...");
    let webui_bundle = out_dir.join("bundle.json");
    rustc::env("WEBUI_BUNDLE", webui_bundle);

    Command::new("yarn")
        .arg("install")
        .arg("--prefer-offline")
        .arg("--frozen-lockfile")
        .arg("--no-progress")
        .arg("--non-interactive")
        .current_dir(&src_dir)
        .run();

    Command::new("yarn")
        .arg("vite")
        .arg("build")
        .arg("--outDir")
        .arg(&out_dir)
        .arg("--emptyOutDir")
        .current_dir(&src_dir)
        .run();
}
