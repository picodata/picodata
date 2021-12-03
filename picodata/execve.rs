use libc::c_char;
use std::ffi::CString;
use std::path;

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
    let path = std::env::var("PATH").unwrap_or_default();
    for p in path.split(":") {
        let pb: path::PathBuf = [p, prog].iter().collect();
        if pb.as_path().exists() {
            return Some(pb);
        }
    }
    None
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::File;
    use tempfile::tempdir;

    #[test]
    fn test_which() -> Result<(), std::io::Error> {
        let temp = vec![tempdir()?, tempdir()?];
        File::create(temp[0].path().join("b0"))?;
        File::create(temp[1].path().join("b1"))?;
        for p in &temp {
            File::create(p.path().join("bin"))?;
        }

        let _savepoint = PathSavepoint::new();

        // which can handle unset variable
        std::env::remove_var("PATH");
        assert_eq!(which("void"), None);

        // path order matters
        let path = format!("{}:{}", temp[0].path().display(), temp[1].path().display());
        std::env::set_var("PATH", path);

        assert_eq!(which("void"), None);
        assert_eq!(which("bin"), Some(temp[0].path().join("bin")));
        assert_eq!(which("b0"), Some(temp[0].path().join("b0")));
        assert_eq!(which("b1"), Some(temp[1].path().join("b1")));

        std::env::set_var("PATH", "ensure:savepoint:works");
        drop(_savepoint);
        assert_ne!(
            std::env::var("PATH").ok(),
            Some("ensure:savepoint:works".to_owned())
        );

        Ok(())
    }

    struct PathSavepoint(Option<String>);
    impl PathSavepoint {
        fn new() -> Self {
            Self(std::env::var("PATH").ok())
        }
    }
    impl Drop for PathSavepoint {
        fn drop(&mut self) {
            match &self.0 {
                Some(v) => std::env::set_var("PATH", v),
                None => std::env::remove_var("PATH"),
            }
        }
    }
}
