//! Corpus-based DQL.
//!
//! # `queries.sql`
//! Anonymized real-world DQL statements. The unit tests pin constructs one at a time.
//!
//! A `-- TEST: <name>` comment names the statements after it - the name a
//! test failure or a criterion benchmark id reports.
//! The file is kept formatted see `README.md` for details.

pub mod corpus_mock;
pub mod mock_catalog;
pub use corpus_mock::corpus_catalog;
pub use mock_catalog::{ColumnSpec, MockCatalog};

const CORPUS: &str = include_str!("queries.sql");

pub struct CorpusQuery {
    name: &'static str,
    sql: String,
}

impl CorpusQuery {
    pub fn into_parts(self) -> (&'static str, String) {
        (self.name, self.sql)
    }
}

/// Splits the corpus into statements. Full-line `--` comments are skipped,
/// `;` terminates a statement unless inside a single-quoted string.
pub fn corpus_queries() -> Vec<CorpusQuery> {
    split_statements(CORPUS)
        .into_iter()
        .map(|(name, sql)| CorpusQuery { name, sql })
        .collect()
}

/// The splitter proper, over an arbitrary corpus text so it can be unit-tested.
///
/// Names borrow from `corpus`.
///
/// A `--` outside a single-quoted string comments out the rest of the line — both
/// when it starts the line and when it trails code. Getting the latter wrong is not
/// cosmetic: an apostrophe in a trailing comment (`-- don't`) would flip the
/// in-string state and silently swallow every following `;`.
fn split_statements(corpus: &str) -> Vec<(&str, String)> {
    let mut queries = Vec::new();
    let mut name = "<unnamed>";
    let mut stmt = String::new();
    let mut in_string = false;

    let mut flush = |name, stmt: &mut String| {
        let sql = stmt.trim();
        if !sql.is_empty() {
            queries.push((name, sql.to_owned()));
        }
        stmt.clear();
    };

    for line in corpus.lines() {
        if !in_string {
            if let Some(comment) = line.trim_start().strip_prefix("--") {
                if let Some(n) = comment.trim_start().strip_prefix("TEST:") {
                    name = n.trim();
                }
                continue;
            }
        }
        let mut chars = line.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '\'' => {
                    in_string = !in_string;
                    stmt.push(c);
                }
                ';' if !in_string => flush(name, &mut stmt),
                // Trailing comment: drop the rest of the line.
                '-' if !in_string && chars.peek() == Some(&'-') => break,
                _ => stmt.push(c),
            }
        }
        stmt.push('\n');
    }
    flush(name, &mut stmt);
    queries
}

#[cfg(test)]
mod tests {
    use sql_ast_new_parser::parse;

    use super::{corpus_queries, split_statements, CORPUS};

    fn round_trip(sql: &str) -> Result<(), String> {
        let ast = parse(sql).map_err(|e| format!("parse error: {e}"))?;
        let rendered = ast.to_string();
        let reparsed = parse(&rendered)
            .map_err(|e| format!("rendered SQL does not re-parse: {e}\nrendered: {rendered}"))?;
        let re_rendered = reparsed.to_string();
        if re_rendered != rendered {
            return Err(format!(
                "rendering is not stable:\n first: {rendered}\nsecond: {re_rendered}"
            ));
        }
        Ok(())
    }

    /// A trailing `--` comment ends at the newline, and an apostrophe inside it must
    /// not be mistaken for a string delimiter (which would swallow every later `;`).
    #[test]
    fn splitter_ignores_trailing_line_comments() {
        let corpus = "\
-- TEST: q1
SELECT a FROM t; -- shouldn't merge with q2
-- TEST: q2
SELECT b FROM t;
";
        let queries = split_statements(corpus);

        assert_eq!(
            queries,
            vec![
                ("q1", "SELECT a FROM t".to_owned()),
                ("q2", "SELECT b FROM t".to_owned()),
            ]
        );
    }

    /// `;` and `--` inside a string literal are data, not syntax; `''` is an escaped
    /// quote and must leave the scanner inside the string.
    #[test]
    fn splitter_respects_string_literals() {
        let corpus = "\
-- TEST: q1
SELECT 'a;b -- c' FROM t;
-- TEST: q2
SELECT 'it''s; fine' FROM t;
";
        let queries = split_statements(corpus);

        assert_eq!(
            queries,
            vec![
                ("q1", "SELECT 'a;b -- c' FROM t".to_owned()),
                ("q2", "SELECT 'it''s; fine' FROM t".to_owned()),
            ]
        );
    }

    /// Validate pattern 'q<n>'
    #[test]
    fn validate_corpus_query_names() {
        let queries = split_statements(CORPUS);

        queries
            .into_iter()
            .enumerate()
            .for_each(|(idx, (name, _))| {
                let pos = idx + 1;
                // Assure that `name` is q<pos>
                let expected = format!("q{pos}");
                assert_eq!(name, &expected, "expected {expected}, found {name}");
            });
    }

    #[test]
    fn corpus_dql_round_trip() {
        let queries = corpus_queries();

        for query in &queries {
            // Not `.expect(...)`: the diagnostics are multi-line, and `expect` would
            // render them through `Debug` with the newlines escaped.
            if let Err(err) = round_trip(&query.sql) {
                panic!("query '{}' failed: {err}", query.name);
            }
        }
    }
}
