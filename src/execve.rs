use libc::c_char;
use std::path;
use std::ffi::CString;

#[must_use]
pub fn execve(argv: Vec<CString>, envp: Vec<CString>) -> errno::Errno {
    let mut argv_p: Vec<*const c_char> = Vec::new();
    for s in argv.iter() {
        argv_p.push(s.as_ptr());
    }
    argv_p.push(std::ptr::null());

    let mut envp_p: Vec<*const c_char> = Vec::new();
    for s in envp.iter() {
        envp_p.push(s.as_ptr());
    }
    envp_p.push(std::ptr::null());

    unsafe {
        libc::execve(argv[0].as_ptr(), argv_p.as_ptr(), envp_p.as_ptr());
    };

    // execve doesn't retun unless it fails
    errno::errno()
}

pub fn which(prog: &str) -> Option<path::PathBuf> {
    for p in std::env::var("PATH").unwrap().split(":") {
        let pb: path::PathBuf = [p, prog].iter().collect();
        if pb.as_path().exists() {
            return Some(pb);
        }
    }
    None
}
