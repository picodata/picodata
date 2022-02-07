use clap::App;
use std::collections::HashMap;
use std::ffi::CString;

#[macro_use]
extern crate clap;

mod execve;

macro_rules! CString {
    ($s:expr) => {
        CString::new($s).expect("CString::new failed")
    };
}

fn main() {
    let yml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yml).version(crate_version!()).get_matches();

    if matches.subcommand_name().is_none() {
        println!("{}", matches.usage());
        std::process::exit(0);
    }

    match matches.subcommand() {
        ("run", Some(subm)) => main_run(subm),
        _ => unreachable!(),
    }
}

fn main_run(matches: &clap::ArgMatches) {
    let tarantool_path: String = match execve::which("tarantool") {
        Some(v) => v
            .into_os_string()
            .into_string()
            .expect("OsString::into_string failed"),
        None => {
            eprintln!("tarantool: {}", errno::Errno(libc::ENOENT));
            std::process::exit(-1);
        }
    };

    let mut argv: Vec<&str> = vec![&tarantool_path];
    argv.push("-l");
    argv.push("picolib");

    if let Some(script) = matches.value_of("tarantool-exec") {
        argv.push("-e");
        argv.push(script);
    }

    let argv = argv.iter().map(|&s| CString!(s)).collect();

    let mut envp = HashMap::new();
    // Tarantool implicitly parses some environment variables.
    // We don't want them to affect the behavior and thus filter them out.
    for (k, v) in std::env::vars() {
        if !k.starts_with("TT_") && !k.starts_with("TARANTOOL_") {
            envp.insert(k, v);
        }
    }

    if let Some(peer) = matches.values_of("peer") {
        let append = |s: String, str| if s.is_empty() { s + str } else { s + "," + str };
        let peer = peer.fold(String::new(), append);
        envp.insert("PICODATA_PEER".to_owned(), peer);
    }

    envp.entry("PICODATA_LISTEN".to_owned())
        .or_insert_with(|| "3301".to_owned());
    envp.entry("PICODATA_DATA_DIR".to_owned())
        .or_insert_with(|| ".".to_owned());

    let bypass_vars = [
        "cluster-id",
        "data-dir",
        "instance-id",
        "listen",
        "replicaset-id",
    ];

    for var in bypass_vars {
        if let Some(v) = matches.value_of(var) {
            let k = format!("PICODATA_{}", var.to_uppercase().replace("-", "_"));
            envp.insert(k, v.to_owned());
        }
    }

    // In order to make libpicodata loaded we should provide
    // its path in the LUA_CPATH env variable.
    let cpath: String = {
        // The lib is located in the same dir as the executable
        let mut p = std::env::current_exe().unwrap();
        p.pop();

        #[cfg(target_os = "macos")]
        p.push("lib?.dylib");

        #[cfg(target_os = "linux")]
        p.push("lib?.so");

        p.into_os_string().into_string().unwrap()
    };

    envp.entry("LUA_CPATH".to_owned())
        .and_modify(|v| *v = format!("{};{}", cpath, v))
        .or_insert(cpath);

    let envp = envp
        .into_iter()
        .map(|(k, v)| CString!(format!("{}={}", k, v)))
        .collect();

    let e = execve::execve(argv, envp);
    eprintln!("{}: {}", tarantool_path, e);
    std::process::exit(-1);
}
