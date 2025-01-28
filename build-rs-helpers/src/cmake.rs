use std::{collections::HashMap, ffi::OsStr, process::Command};

pub struct CmakeVariables(pub HashMap<String, String>);

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

        let stdout = String::from_utf8(output.stdout).expect("invalid utf-8");
        let stderr = String::from_utf8(output.stderr).expect("invalid utf-8");

        if !output.status.success() {
            panic!("failed to get cmake variables: {stderr}");
        }

        let mut result = HashMap::new();
        for line in stdout.lines() {
            // Skip comments, e.g. `-- Generating done (0.3s)`.
            if line.starts_with("--") {
                continue;
            }

            // E.g. `ENABLE_BACKTRACE:BOOL=ON`.
            let Some((name_type, value)) = line.split_once('=') else {
                continue;
            };

            // E.g. `BASH:FILEPATH`.
            let Some((name, _)) = name_type.split_once(':') else {
                continue;
            };

            result.insert(name.to_owned(), value.to_owned());
        }

        CmakeVariables(result)
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
