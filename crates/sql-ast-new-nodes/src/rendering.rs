//! Helpers shared by every stage's snapshot tests.
//!
//! They live here rather than in each stage because `#[cfg(test)]` does not
//! cross a crate boundary and the stages are separate crates.
//! The helper is only ever applied to `Display` output.

/// Collapses whitespace runs to a single space and trims the ends, so a snapshot
/// can be written on one line no matter how [`Display`](std::fmt::Display) breaks
/// the rendering.
///
/// Whitespace *inside* a single-quoted literal is content, not layout, and is
/// left alone: `SELECT 'a  b'` must not normalize to `SELECT 'a b'`.
///
/// The `''` escape needs no special handling.
pub fn normalize_whitespace(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_string = false;
    let mut pending_space = false;

    let mut last_quote: Option<char> = None;
    for c in s.chars() {
        if in_string {
            out.push(c);
            in_string = (last_quote.is_some_and(|ch| ch == '\'') && c != '\'')
                || (last_quote.is_some_and(|ch| ch == '"' && c != '"'));
            continue;
        }
        if c.is_ascii_whitespace() {
            // Emitted only once something follows it, which trims both ends.
            pending_space = !out.is_empty();
            continue;
        }
        if pending_space {
            out.push(' ');
            pending_space = false;
        }
        out.push(c);
        in_string = c == '\'' || c == '"';
        if in_string {
            last_quote = Some(c);
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::normalize_whitespace;

    /// Whitespace inside a string literal is data.
    #[test]
    fn normalize_whitespace_preserves_string_literals() {
        insta::assert_snapshot!(
            normalize_whitespace(r#"  SELECT\n   'a  b',\t'c\nd', "a  b"  FROM t  "#),
            @r#"SELECT\n 'a  b',\t'c\nd', "a  b" FROM t"#
        );
        insta::assert_snapshot!(
            normalize_whitespace("SELECT 'it''s  fine'  FROM  t"),
            @"SELECT 'it''s  fine' FROM t"
        );
        insta::assert_snapshot!(
            normalize_whitespace("SELECT ''  FROM  t"),
            @"SELECT '' FROM t"
        );
        insta::assert_snapshot!(
            normalize_whitespace(r#"SELECT 'a"  b'  FROM  t"#),
            @r#"SELECT 'a"  b' FROM t"#
        );
    }
}
