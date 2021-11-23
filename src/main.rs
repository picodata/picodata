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
    println!("{:?}", matches);

    if matches.subcommand_name().is_none() {
        println!("{}", matches.usage());
        std::process::exit(0);
    }

    if let Some(matches) = matches.subcommand_matches("run") {
        return main_run(matches);
    }

    unreachable!();
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

    let mut args: Vec<&str> = vec![&tarantool_path, "-l", "picodata"];
    if let Some(script) = matches.value_of("tarantool-exec") {
        args.push("-e");
        args.push(script);
    }
    let argv = args.iter().map(|&s| CString!(s)).collect();

    let mut envp = HashMap::new();
    for (k, v) in std::env::vars() {
        if !k.starts_with("TT_") && !k.starts_with("TARANTOOL_") {
            envp.insert(k, v);
        }
    }

    if let Some(peer) = matches.values_of("peer") {
        let append = |s: String, str| if s.is_empty() {s + str} else {s + "," + str};
        let peer = peer.fold(String::new(), append);
        envp.insert("PICODATA_PEER".to_owned(), peer);
    }

    for arg in ["listen", "instance-id", "replicaset-id", "cluster-id"] {
        if let Some(v) = matches.value_of(arg) {
            let k = format!("PICODATA_{}", arg.to_uppercase().replace("-", "_"));
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

    println!("Hello from picodata main");

    let e = execve::execve(argv, envp);
    eprintln!("{}: {}", tarantool_path, e);
    std::process::exit(-1);
}
