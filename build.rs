use build_rs_helpers::{cargo, rustc};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
};
use tarantool_build::TarantoolBuildRoot;

fn main() {
    // Prevent linkage problems due to non-pie static archives.
    // TODO: drop this since it's less than ideal from a security standpoint.
    rustc::link_arg("-no-pie");

    // Initialize make jobserver to limit unwanted concurrency.
    let jobserver = cargo::setup_make_jobserver();
    let jobserver = jobserver.as_ref();

    let build_root = cargo::get_build_root();
    let use_static_build = !cargo::get_feature("dynamic_build");

    // Build and link all the relevant tarantool libraries.
    // For more info, read the comments in tarantool-build.
    TarantoolBuildRoot::new(&build_root, use_static_build)
        .build_libraries(jobserver)
        .link_libraries();

    generate_git_version();
    export_public_symbols();
    check_plugins_ffi();

    if cfg!(feature = "webui") {
        webui::build(&build_root, Path::new("webui"));
    }
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
            println!("cargo:warning={declarations_filename}:{line}: found a declaration for `fn {name}` which is not defined in file {definitions_filename}");
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
            println!("cargo:warning=Signature mismatch for `fn {name}`");
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

mod webui {
    use super::*;
    use build_rs_helpers::CommandExt;
    use std::{ffi::OsString, process::Command};

    fn rerun_if_changed(webui_dir: &Path) {
        let source_dir = std::env::current_dir().unwrap().join(webui_dir);
        std::fs::read_dir(source_dir)
            .expect("failed to scan webui dir")
            .flatten()
            .for_each(|entry| {
                // Do not rerun for generated files changes
                let file_name = entry.file_name();
                let ignored_files = ["node_modules", ".husky"].map(OsString::from);
                if !ignored_files.contains(&file_name) {
                    cargo::rerun_if_changed(webui_dir.join(file_name));
                }
            });
    }

    pub fn build(build_root: &Path, webui_dir: &Path) {
        cargo::rerun_if_env_changed("WEBUI_BUNDLE");
        self::rerun_if_changed(webui_dir);

        if std::env::var("WEBUI_BUNDLE").is_ok() {
            println!("building webui_bundle skipped");
            return;
        }

        println!("building webui_bundle ...");
        let src_dir = std::env::current_dir().unwrap().join(webui_dir);
        let out_dir = build_root.join(webui_dir);

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
}
