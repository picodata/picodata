use std::borrow::Cow;

use sql::frontend::sql::ast::is_reserved_keyword;

/// An unquoted identifier is lower-cased by the parser, so `MyTable` must be quoted.
fn needs_quoting(name: &str) -> bool {
    let mut characters = name.chars();
    let is_plain = characters
        .next()
        .is_some_and(|first| first.is_ascii_lowercase() || first == '_')
        && characters.all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        });

    !is_plain || is_reserved_keyword(name)
}

pub(super) fn quote_ident(name: &str) -> Cow<'_, str> {
    if needs_quoting(name) {
        Cow::Owned(format!("\"{}\"", name.replace('"', "\"\"")))
    } else {
        Cow::Borrowed(name)
    }
}

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
    fn plain_lower_case_names_are_left_alone() {
        for name in ["t", "t_sharded", "_hidden", "a1", "bucket_id"] {
            assert_eq!(quote_ident(name), name, "{name} should stay bare");
        }
    }

    #[test]
    fn anything_that_would_be_lower_cased_is_quoted() {
        assert_eq!(quote_ident("MyTable"), "\"MyTable\"");
        assert_eq!(quote_ident("Quoted Name"), "\"Quoted Name\"");
        assert_eq!(quote_ident("with-dash"), "\"with-dash\"");
        assert_eq!(quote_ident("привет"), "\"привет\"");
        assert_eq!(quote_ident("1025_pkey"), "\"1025_pkey\"");
    }

    #[test]
    fn keywords_are_quoted_even_though_they_look_plain() {
        assert_eq!(quote_ident("select"), "\"select\"");
        assert_eq!(quote_ident("table"), "\"table\"");
        assert_eq!(quote_ident("order"), "\"order\"");
        // Not a keyword
        assert_eq!(quote_ident("selection"), "selection");
    }

    #[test]
    fn a_quote_inside_a_name_is_doubled() {
        assert_eq!(quote_ident("it\"s"), "\"it\"\"s\"");
        // A single quote needs no escaping inside a delimited identifier.
        assert_eq!(quote_ident("it's"), "\"it's\"");
    }

    #[test]
    fn an_empty_name_is_quoted_rather_than_emitted_bare() {
        assert_eq!(quote_ident(""), "\"\"");
    }

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
