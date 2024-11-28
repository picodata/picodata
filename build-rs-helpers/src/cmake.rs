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
