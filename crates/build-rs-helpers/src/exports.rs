use std::{collections::HashSet, fmt::Write, io, path::Path};

pub fn read_file(exports_file: impl AsRef<Path>, symbols: &mut HashSet<String>) -> io::Result<()> {
    let contents = std::fs::read_to_string(exports_file)?;

    for line in contents.lines() {
        let line = line.trim();
        if !line.is_empty() && !line.starts_with('#') {
            symbols.insert(line.to_owned());
        }
    }

    Ok(())
}

pub fn write_file(
    exports_file: impl AsRef<Path>,
    symbols: impl IntoIterator<Item = String>,
) -> io::Result<()> {
    let mut contents = String::new();

    if cfg!(target_os = "macos") {
        for symbol in symbols {
            // All macos symbols start with an underscore.
            contents.write_fmt(format_args!("_{symbol}\n")).unwrap();
        }
    } else {
        contents.write_str("{\n").unwrap();
        for symbol in symbols {
            contents.write_fmt(format_args!("{symbol};\n")).unwrap();
        }
        contents.write_str("};\n").unwrap();
    }

    std::fs::write(&exports_file, contents)
}
