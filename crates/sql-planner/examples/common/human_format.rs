/// Thousands-separated allocation count, e.g. `1,234,567`.
pub fn commas(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    let len = digits.len();
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (len - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

/// Human-readable byte size, e.g. `12.34 MiB`.
pub fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    if n < 1024 {
        return format!("{n} B");
    }
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1023.995 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.2} {}", UNITS[unit])
}

/// [`human_bytes`] with a sign, for the net figures that can come out negative.
pub fn human_signed(n: i64) -> String {
    if n < 0 {
        format!("-{}", human_bytes(n.unsigned_abs()))
    } else {
        human_bytes(n as u64)
    }
}
