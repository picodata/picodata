/// picodata's SQL parser rejects comments altogether. They survive only because
/// `psql` drops the `--` lines preceding a statement, so never put one inside a
/// statement and never use `/* */`.
///
/// `psql` ends a `--` comment at a `\r` as well as at a `\n`, so `\r` splits
/// the line too: otherwise the text after it would run as SQL on restore.
pub(super) fn render_comment(text: &str) -> String {
    text.lines()
        .flat_map(|line| line.split('\r'))
        .map(|line| {
            if line.is_empty() {
                "--\n".to_owned()
            } else {
                format!("-- {line}\n")
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn comments_prefix_every_line() {
        assert_eq!(render_comment("one"), "-- one\n");
        assert_eq!(render_comment("one\ntwo"), "-- one\n-- two\n");
        assert_eq!(render_comment("one\n\ntwo"), "-- one\n--\n-- two\n");
        assert_eq!(render_comment("one\r\ntwo"), "-- one\n-- two\n");
    }

    #[test]
    fn a_lone_carriage_return_cannot_end_the_comment() {
        assert_eq!(
            render_comment("one\rDROP TABLE t;"),
            "-- one\n-- DROP TABLE t;\n"
        );
    }
}
