use std::io::Write as _;
use std::{collections::HashMap, ffi::OsStr, process::Command};

pub struct CmakeVariables(pub HashMap<String, String>);

/// Polyfill for `slice_split_once` on byte slices
///
/// Unstable as of Rust 1.99: <https://github.com/rust-lang/rust/issues/112811>
fn bytes_split_once(s: &[u8], pred: u8) -> Option<(&[u8], &[u8])> {
    let index = s.iter().position(|&x| x == pred)?;
    Some((&s[..index], &s[index + 1..]))
}

fn parse_variables(stdout: &[u8]) -> Result<HashMap<String, String>, String> {
    let mut result = HashMap::new();
    for line in stdout.split(|&x| x == b'\n') {
        // Skip comments, e.g. `-- Generating done (0.3s)`.
        if line.starts_with(b"--") {
            continue;
        }

        // E.g. `ENABLE_BACKTRACE:BOOL=ON`.
        let Some((name_type, value)) = bytes_split_once(line, b'=') else {
            continue;
        };

        // E.g. `BASH:FILEPATH`.
        let Some((name, _)) = bytes_split_once(name_type, b':') else {
            continue;
        };

        let name =
            std::str::from_utf8(name).map_err(|e| format!("variable name not UTF-8: {e}"))?;
        let value =
            std::str::from_utf8(value).map_err(|e| format!("variable value not UTF-8: {e}"))?;

        result.insert(name.to_owned(), value.to_owned());
    }

    Ok(result)
}

fn print_cmake_outputs(output: &std::process::Output) {
    eprintln!("cmake stderr:");
    std::io::stderr()
        .write_all(&output.stderr)
        .expect("failed to write to stderr");
    println!("cmake stdout:");
    std::io::stdout()
        .write_all(&output.stdout)
        .expect("failed to write to stdout");
}

impl CmakeVariables {
    pub fn gather(source_dir: impl AsRef<OsStr>, build_dir: impl AsRef<OsStr>) -> Self {
        let output = Command::new("cmake")
            .arg("-S")
            .arg(source_dir)
            .arg("-B")
            .arg(build_dir)
            .arg("-L")
            .output()
            .expect("failed to get cmake variables");

        if !output.status.success() {
            // Print CMake output for inspection...
            print_cmake_outputs(&output);

            // ...and then panic with the root cause error
            panic!(
                "failed to get cmake variables: cmake returned {:?}",
                output.status
            );
        }

        match parse_variables(&output.stdout) {
            Ok(result) => CmakeVariables(result),
            Err(e) => {
                // Print CMake output for inspection...
                print_cmake_outputs(&output);

                // ...and then panic with the root cause error
                panic!("{e}")
            }
        }
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        let value = self.0.get(key)?;
        try_parse_bool(value)
    }
}

pub fn try_parse_bool(s: &str) -> Option<bool> {
    let matches_any = |variants: &[&str]| variants.iter().any(|x| s.eq_ignore_ascii_case(x));

    if matches_any(&["off", "false"]) {
        return Some(false);
    }

    if matches_any(&["on", "true"]) {
        return Some(true);
    }

    None
}

pub fn print_bool(v: bool) -> &'static str {
    match v {
        false => "OFF",
        true => "ON",
    }
}
