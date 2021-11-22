use std::collections::HashMap;
use std::ffi::CString;

mod execve;

macro_rules! CString {
    ($s:expr) => {
        CString::new($s).expect("CString::new failed")
    };
}

fn main() {
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

    let args = [&tarantool_path, "-l", "picodata"];
    let argv = std::iter::empty()
        .chain(args.iter().map(|&s| CString!(s)))
        .chain(std::env::args().skip(1).map(|s| CString!(s)))
        .collect();

    let mut envp = HashMap::new();
    for (k, v) in std::env::vars() {
        if !k.starts_with("TT_") && !k.starts_with("TARANTOOL_") {
            envp.insert(k, v);
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
