use build_rs_helpers::{cargo, rustc};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
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
    if !cfg!(target_os = "macos") {
        // not supported on macos
        // We use -rdynamic instead of -export-dynamic
        // because on fedora this likely triggers this bug
        // https://gcc.gnu.org/bugzilla/show_bug.cgi?id=47390
        rustc::link_arg("-rdynamic");
    }

    let tarantool_src = PathBuf::from("tarantool-sys");
    let mut symbols = HashSet::with_capacity(1024);

    let exports = std::fs::read_to_string(tarantool_src.join("extra/exports")).unwrap();
    read_symbols_into(&exports, &mut symbols);

    let exports = std::fs::read_to_string(tarantool_src.join("extra/exports_libcurl")).unwrap();
    read_symbols_into(&exports, &mut symbols);

    // Tell a linker to create an undefined symbol for each entry.
    // This will instruct the linker to keep a module containing the symbol.
    // Refer to `man ld` to learn more on this topic.
    for symbol in &symbols {
        if cfg!(target_os = "macos") {
            // Mach-O symbols start with the underscore.
            // Historical note: https://news.ycombinator.com/item?id=20143732.
            // Furthermore, macos uses `-u` & `--undefined error` (the default) for `--require-defined`.
            rustc::link_arg_bin("picodata", format!("-u_{symbol}"));
        } else {
            rustc::link_arg_bin("picodata", format!("-Wl,--require-defined={symbol}"));
        }
    }

    fn read_symbols_into<'a>(file_contents: &'a str, symbols: &mut HashSet<&'a str>) {
        for line in file_contents.lines() {
            let line = line.trim();
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            if line.is_empty() {
                continue;
            }
            symbols.insert(line);
        }
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
