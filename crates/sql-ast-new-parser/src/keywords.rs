//! Keyword coverage for the whole grammar.
//!
//! # Why this module exists
//! `query.pest` is scannerless: there is no lexer to split the input into
//! tokens, so every keyword literal has to establish its own right-hand
//! boundary with `KwEnd` — the grammar's zero-width
//! `&IdentifierInapplicableSymbol` lookahead. A literal that forgets the guard
//! matches the *prefix* of a longer word, and because PEG never backtracks into
//! an alternative that already succeeded, the damage surfaces far from the
//! keyword: `SELECT distinct_x FROM t` used to fail with "expected EOI or
//! MultisetInner" because `DISTINCT` ate the first eight characters of a column
//! name.
//!
//! That failure mode is invisible to per-form tests — each one spells its
//! keyword correctly. It only shows up when the *whole keyword set* is swept, so
//! the sweep lives here rather than being scattered across the parser modules.
//!
//!
//! # What is checked
//! Two halves, mirroring the grammar's two mechanisms.
//!
//! * The `Keyword` list is the *reservation* mechanism: it is what
//!   `RegularIdentifier` consults to refuse a bare keyword as a column name.
//!   [`reserved_words_are_refused_bare_and_kept_when_suffixed`] drives every
//!   entry of that list through both directions, and needs no table — the cases
//!   are generated from the grammar text.
//! * The keyword literals inside the syntax rules are the *token* mechanism.
//!   Those need a hand-written position each, so [`SYNTAX_KEYWORDS`] pairs every
//!   one with a query that spells it and a query that glues word characters onto
//!   it.
//!
//! Both halves are gated on the grammar source by
//! [`syntax_keyword_table_covers_the_grammar`], so a keyword added to
//! `query.pest` fails this module until it is given a case.

use std::collections::BTreeSet;

use pest::Parser;
use sql_ast_new_grammar::{PairParser, Rule, GRAMMAR_SOURCE};
use sql_ast_new_nodes::rendering::normalize_whitespace;

use crate::parse;

/// Parse a whole query and render the tree back, so one comparison checks both
/// that the input was accepted and that it produced the tree it should have.
fn render(query: &str) -> Result<String, String> {
    match parse(query) {
        Ok(ast) => Ok(normalize_whitespace(&ast.root.to_string())),
        Err(err) => Err(err.to_string()),
    }
}

//////////////////////////////// Grammar scan ////////////////////////////////

/// Every `^"..."` literal of `query.pest`, split by the mechanism it serves:
/// `(reserved, syntax)` — the entries of the `Keyword` list, and the literals
/// spelled by the syntax rules.
///
/// Comment lines are skipped (the `Keyword` list documents its ordering rule
/// with `^"in"`/`^"insert"` examples), and so are the punctuation literals
/// `^"("`, `^")"` and `^","`, which are not keywords.
fn grammar_keywords() -> (BTreeSet<String>, BTreeSet<String>) {
    let (mut reserved, mut syntax) = (BTreeSet::new(), BTreeSet::new());
    let mut in_keyword_list = false;

    for line in GRAMMAR_SOURCE.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("//") {
            continue;
        }
        if trimmed.starts_with("Keyword =") {
            in_keyword_list = true;
        }

        let mut rest = line;
        while let Some(start) = rest.find("^\"") {
            let after = &rest[start + 2..];
            let Some(end) = after.find('"') else { break };
            let literal = &after[..end];
            rest = &after[end + 1..];

            let is_word = !literal.is_empty()
                && literal
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            if !is_word {
                continue;
            }
            if in_keyword_list {
                reserved.insert(literal.to_string());
            } else {
                syntax.insert(literal.to_string());
            }
        }

        if in_keyword_list && trimmed == "}" {
            in_keyword_list = false;
        }
    }

    (reserved, syntax)
}

#[test]
fn grammar_scan_finds_both_keyword_sets() {
    let (reserved, syntax) = grammar_keywords();

    // Guards against a silently empty sweep: if the scan ever stops matching
    // the grammar's shape, every other test here would vacuously pass.
    assert!(
        reserved.len() > 50,
        "the `Keyword` list scan collapsed, found only {reserved:?}"
    );
    assert!(
        syntax.len() > 80,
        "the syntax-rule scan collapsed, found only {syntax:?}"
    );
    // The two halves are distinguished, not merged: `select` is reserved *and*
    // spelled by a rule, while `preceding` is only ever spelled by a rule.
    assert!(reserved.contains("select") && syntax.contains("select"));
    assert!(!reserved.contains("preceding") && syntax.contains("preceding"));
    // Comment text and punctuation stay out.
    assert!(!reserved.contains("insert") && !syntax.contains("insert"));
    assert!(!syntax.contains("("));
}

//////////////////////////////// Reserved words ////////////////////////////////

/// Reserved words that are *also* complete expressions: they sit in the
/// `Keyword` list, so they cannot name a column, yet a rule spells each of them
/// as a value, so `SELECT <word> FROM t` is a legal query. Every other reserved
/// word must be refused in that position.
const RESERVED_WORDS_THAT_ARE_EXPRESSIONS: &[&str] = &[
    "true",
    "false",
    "null",
    "current_date",
    "current_time",
    "current_timestamp",
    "localtime",
    "localtimestamp",
];

/// The reservation mechanism, in both directions, for every entry of the
/// grammar's `Keyword` list.
///
/// *Bare*: the word alone is not a column name (except for the handful that are
/// expressions in their own right).
///
/// *Suffixed*: gluing word characters onto the keyword yields an ordinary
/// identifier. This is the direction that catches a missing `KwEnd`, and it
/// catches it for the entire keyword set at once.
#[test]
fn reserved_words_are_refused_bare_and_kept_when_suffixed() {
    let (reserved, _) = grammar_keywords();
    let mut failures = Vec::new();

    for word in &reserved {
        let bare = format!("SELECT {word} FROM t");
        let is_expression = RESERVED_WORDS_THAT_ARE_EXPRESSIONS.contains(&word.as_str());
        match (render(&bare), is_expression) {
            (Ok(_), true) | (Err(_), false) => {}
            (Ok(tree), false) => failures.push(format!(
                "`{word}` is reserved, but `{bare}` parsed as `{tree}`"
            )),
            (Err(err), true) => failures.push(format!(
                "`{word}` should be an expression, but `{bare}` failed: {err}"
            )),
        }

        for glued in [format!("{word}x"), format!("{word}_x")] {
            let query = format!("SELECT {glued} FROM t");
            let expected = format!("SELECT {glued} FROM t");
            match render(&query) {
                Ok(tree) if tree == expected => {}
                Ok(tree) => failures.push(format!(
                    "`{query}` should read `{glued}` as an identifier, parsed as `{tree}`"
                )),
                Err(err) => failures.push(format!(
                    "`{query}` should read `{glued}` as an identifier, failed: {err}"
                )),
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{} reserved-word case(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

//////////////////////////////// Syntax keywords ////////////////////////////////

/// What a spelling must do.
enum Parses {
    /// Accepted, rendering back as this.
    As(&'static str),
    /// Rejected. Usually by the grammar; for a form the grammar spells but the
    /// language does not support, by the parser behind it.
    Not,
}

/// One keyword as a syntax rule spells it.
struct SyntaxKeyword {
    /// The `query.pest` literals this row accounts for. A literal spelled by
    /// several rules (`as`, `by`, `from`, ...) is exercised once, in the
    /// position where gluing is most likely to be absorbed silently.
    covers: &'static [&'static str],
    /// A query using the keyword, and the tree it must produce.
    used: &'static str,
    renders: Parses,
    /// The same construct with word characters glued onto the keyword's tail.
    glued: &'static str,
    /// What the glued spelling must do: [`Parses::As`] where the position also
    /// admits an identifier, [`Parses::Not`] where nothing can absorb the word.
    ///
    /// Either way the contract is the same: the glued spelling is never read as
    /// the keyword.
    glued_reads_as: Parses,
}

/// Every keyword literal the syntax rules spell, in the position that spells it.
///
/// Ordered as the grammar is: statement shape first, then clauses, then
/// expressions.
const SYNTAX_KEYWORDS: &[SyntaxKeyword] = &[
    // ---- statement shape
    SyntaxKeyword {
        covers: &["select"],
        used: "SELECT a FROM t",
        renders: Parses::As("SELECT a FROM t"),
        glued: "SELECTx a FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["distinct"],
        used: "SELECT DISTINCT a FROM t",
        renders: Parses::As("SELECT DISTINCT a FROM t"),
        glued: "SELECT distinctx FROM t",
        glued_reads_as: Parses::As("SELECT distinctx FROM t"),
    },
    SyntaxKeyword {
        // The keyword and a column that merely starts with it, side by side.
        covers: &["distinct"],
        used: "SELECT DISTINCT distinct_x FROM t",
        renders: Parses::As("SELECT DISTINCT distinct_x FROM t"),
        glued: "SELECT distinct_x FROM t",
        glued_reads_as: Parses::As("SELECT distinct_x FROM t"),
    },
    SyntaxKeyword {
        // `FunctionArgs` spells the same keyword as `(Distinct ~ W)?`, with the
        // whitespace *inside* the optional group — the shape `SelectList` does
        // not have, and the reason the two positions are exercised separately.
        covers: &["distinct"],
        used: "SELECT count(DISTINCT a) FROM t",
        renders: Parses::As("SELECT count(DISTINCT a) FROM t"),
        glued: "SELECT count(distinct_x) FROM t",
        glued_reads_as: Parses::As("SELECT count(distinct_x) FROM t"),
    },
    SyntaxKeyword {
        covers: &["from"],
        used: "SELECT a FROM t",
        renders: Parses::As("SELECT a FROM t"),
        glued: "SELECT a FROMx t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["as"],
        used: "SELECT a AS b FROM t",
        renders: Parses::As("SELECT a AS b FROM t"),
        glued: "SELECT a ASx FROM t",
        glued_reads_as: Parses::As("SELECT a AS asx FROM t"),
    },
    SyntaxKeyword {
        covers: &["public"],
        used: "SELECT a FROM public.t",
        renders: Parses::As("SELECT a FROM t"),
        glued: "SELECT a FROM publicx.t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["with"],
        used: "WITH c AS (SELECT a FROM t) SELECT a FROM c",
        renders: Parses::As("WITH c AS (SELECT a FROM t) SELECT a FROM c"),
        glued: "WITHx c AS (SELECT a FROM t) SELECT a FROM c",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["values"],
        used: "VALUES (1)",
        renders: Parses::As("VALUES (1)"),
        glued: "VALUESx (1)",
        glued_reads_as: Parses::Not,
    },
    // ---- set operations
    SyntaxKeyword {
        covers: &["union"],
        used: "SELECT a FROM t UNION SELECT b FROM u",
        renders: Parses::As("(SELECT a FROM t) UNION (SELECT b FROM u)"),
        glued: "SELECT a FROM t UNIONx SELECT b FROM u",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["all"],
        used: "SELECT a FROM t UNION ALL SELECT b FROM u",
        renders: Parses::As("(SELECT a FROM t) UNION ALL (SELECT b FROM u)"),
        glued: "SELECT a FROM t UNION ALLx SELECT b FROM u",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["except"],
        used: "SELECT a FROM t EXCEPT SELECT b FROM u",
        renders: Parses::As("(SELECT a FROM t) EXCEPT (SELECT b FROM u)"),
        glued: "SELECT a FROM t EXCEPTx SELECT b FROM u",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["intersect"],
        used: "SELECT a FROM t INTERSECT SELECT b FROM u",
        renders: Parses::As("(SELECT a FROM t) INTERSECT (SELECT b FROM u)"),
        glued: "SELECT a FROM t INTERSECTx SELECT b FROM u",
        glued_reads_as: Parses::Not,
    },
    // ---- joins
    SyntaxKeyword {
        covers: &["join"],
        used: "SELECT a FROM t JOIN u ON a = b",
        renders: Parses::As("SELECT a FROM t INNER JOIN u ON a = b"),
        glued: "SELECT a FROM t JOINx u ON a = b",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["inner"],
        used: "SELECT a FROM t INNER JOIN u ON a = b",
        renders: Parses::As("SELECT a FROM t INNER JOIN u ON a = b"),
        glued: "SELECT a FROM t INNERx JOIN u ON a = b",
        glued_reads_as: Parses::As("SELECT a FROM t AS innerx INNER JOIN u ON a = b"),
    },
    SyntaxKeyword {
        covers: &["left"],
        used: "SELECT a FROM t LEFT JOIN u ON a = b",
        renders: Parses::As("SELECT a FROM t LEFT OUTER JOIN u ON a = b"),
        glued: "SELECT a FROM t LEFTx JOIN u ON a = b",
        glued_reads_as: Parses::As("SELECT a FROM t AS leftx INNER JOIN u ON a = b"),
    },
    SyntaxKeyword {
        covers: &["outer"],
        used: "SELECT a FROM t LEFT OUTER JOIN u ON a = b",
        renders: Parses::As("SELECT a FROM t LEFT OUTER JOIN u ON a = b"),
        glued: "SELECT a FROM t LEFT OUTERx JOIN u ON a = b",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["cross"],
        used: "SELECT a FROM t CROSS JOIN u",
        renders: Parses::As("SELECT a FROM t CROSS JOIN u"),
        glued: "SELECT a FROM t CROSSx JOIN u",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["on"],
        used: "SELECT a FROM t JOIN u ON a = b",
        renders: Parses::As("SELECT a FROM t INNER JOIN u ON a = b"),
        glued: "SELECT a FROM t JOIN u ONx a = b",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["using"],
        used: "SELECT a FROM t JOIN u USING (a)",
        renders: Parses::As("SELECT a FROM t INNER JOIN u USING (a)"),
        glued: "SELECT a FROM t JOIN u USINGx (a)",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["indexed"],
        used: "SELECT a FROM t INDEXED BY i",
        renders: Parses::As("SELECT a FROM t INDEXED BY i"),
        glued: "SELECT a FROM t INDEXEDx BY i",
        glued_reads_as: Parses::Not,
    },
    // ---- clauses
    SyntaxKeyword {
        covers: &["where"],
        used: "SELECT a FROM t WHERE a = 1",
        renders: Parses::As("SELECT a FROM t WHERE a = 1"),
        glued: "SELECT a FROM t WHEREx a = 1",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["group", "by"],
        used: "SELECT a FROM t GROUP BY a",
        renders: Parses::As("SELECT a FROM t GROUP BY a"),
        glued: "SELECT a FROM t GROUPx BY a",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["by"],
        used: "SELECT a FROM t GROUP BY a",
        renders: Parses::As("SELECT a FROM t GROUP BY a"),
        glued: "SELECT a FROM t GROUP BYx a",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["having"],
        used: "SELECT a FROM t GROUP BY a HAVING a = 1",
        renders: Parses::As("SELECT a FROM t GROUP BY a HAVING a = 1"),
        glued: "SELECT a FROM t GROUP BY a HAVINGx a = 1",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["window"],
        used: "SELECT sum(a) OVER w FROM t WINDOW w AS ()",
        renders: Parses::As("SELECT sum(a) OVER w FROM t WINDOW w AS ()"),
        glued: "SELECT sum(a) OVER w FROM t WINDOWx w AS ()",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["order"],
        used: "SELECT a FROM t ORDER BY a",
        renders: Parses::As("SELECT a FROM t ORDER BY a ASC"),
        glued: "SELECT a FROM t ORDERx BY a",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["asc"],
        used: "SELECT a FROM t ORDER BY a ASC",
        renders: Parses::As("SELECT a FROM t ORDER BY a ASC"),
        glued: "SELECT a FROM t ORDER BY a ASCx",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["desc"],
        used: "SELECT a FROM t ORDER BY a DESC",
        renders: Parses::As("SELECT a FROM t ORDER BY a DESC"),
        glued: "SELECT a FROM t ORDER BY a DESCx",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["nulls", "first"],
        used: "SELECT a FROM t ORDER BY a NULLS FIRST",
        renders: Parses::As("SELECT a FROM t ORDER BY a ASC NULLS FIRST"),
        glued: "SELECT a FROM t ORDER BY a NULLS FIRSTx",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["last"],
        used: "SELECT a FROM t ORDER BY a NULLS LAST",
        renders: Parses::As("SELECT a FROM t ORDER BY a ASC NULLS LAST"),
        glued: "SELECT a FROM t ORDER BY a NULLSx LAST",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["limit"],
        used: "SELECT a FROM t LIMIT 1",
        renders: Parses::As("SELECT a FROM t LIMIT 1"),
        glued: "SELECT a FROM t LIMITx 1",
        glued_reads_as: Parses::Not,
    },
    // ---- OPTION
    SyntaxKeyword {
        covers: &["option", "sql_vdbe_opcode_max"],
        used: "SELECT a FROM t OPTION (sql_vdbe_opcode_max = 1)",
        renders: Parses::As("SELECT a FROM t OPTION(sql_vdbe_opcode_max = 1)"),
        glued: "SELECT a FROM t OPTIONx (sql_vdbe_opcode_max = 1)",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["sql_motion_row_max"],
        used: "SELECT a FROM t OPTION (sql_motion_row_max = 1)",
        renders: Parses::As("SELECT a FROM t OPTION(sql_motion_row_max = 1)"),
        glued: "SELECT a FROM t OPTION (sql_motion_row_maxx = 1)",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["read_preference", "leader"],
        used: "SELECT a FROM t OPTION (read_preference = leader)",
        renders: Parses::As("SELECT a FROM t OPTION(read_preference = leader)"),
        glued: "SELECT a FROM t OPTION (read_preference = leaderx)",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["replica"],
        used: "SELECT a FROM t OPTION (read_preference = replica)",
        renders: Parses::As("SELECT a FROM t OPTION(read_preference = replica)"),
        glued: "SELECT a FROM t OPTION (read_preference = replicax)",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["any"],
        used: "SELECT a FROM t OPTION (read_preference = any)",
        renders: Parses::As("SELECT a FROM t OPTION(read_preference = any)"),
        glued: "SELECT a FROM t OPTION (read_preference = anyx)",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["forward"],
        used: "SELECT a FROM t OPTION (forward = on)",
        renders: Parses::As("SELECT a FROM t OPTION(forward = on)"),
        glued: "SELECT a FROM t OPTION (forwardx = on)",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["off"],
        used: "SELECT a FROM t OPTION (forward = off)",
        renders: Parses::As("SELECT a FROM t OPTION(forward = off)"),
        glued: "SELECT a FROM t OPTION (forward = offx)",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["ro_to_rw"],
        used: "SELECT a FROM t OPTION (forward = ro_to_rw)",
        renders: Parses::As("SELECT a FROM t OPTION(forward = ro_to_rw)"),
        glued: "SELECT a FROM t OPTION (forward = ro_to_rwx)",
        glued_reads_as: Parses::Not,
    },
    // ---- windows
    SyntaxKeyword {
        covers: &["over"],
        used: "SELECT sum(a) OVER () FROM t",
        renders: Parses::As("SELECT sum(a) OVER () FROM t"),
        // The window body is what tells `Over` apart from a plain call, so the
        // glued spelling has to fall back to a function call plus an alias.
        glued: "SELECT count(*) overw FROM t",
        glued_reads_as: Parses::As("SELECT count(*) AS overw FROM t"),
    },
    SyntaxKeyword {
        covers: &["filter"],
        used: "SELECT count(a) FILTER (WHERE a = 1) OVER () FROM t",
        renders: Parses::As("SELECT count(a) FILTER (WHERE a = 1) OVER () FROM t"),
        glued: "SELECT count(a) filterx FROM t",
        glued_reads_as: Parses::As("SELECT count(a) AS filterx FROM t"),
    },
    SyntaxKeyword {
        covers: &["partition"],
        used: "SELECT sum(a) OVER (PARTITION BY b) FROM t",
        renders: Parses::As("SELECT sum(a) OVER (PARTITION BY b) FROM t"),
        // A window body may open with a base window name, so the glued
        // spelling is read as one.
        glued: "SELECT sum(a) OVER (partitionx) FROM t",
        glued_reads_as: Parses::As("SELECT sum(a) OVER (partitionx) FROM t"),
    },
    SyntaxKeyword {
        covers: &["rows"],
        used: "SELECT sum(a) OVER (ROWS UNBOUNDED PRECEDING) FROM t",
        renders: Parses::As("SELECT sum(a) OVER (ROWS UNBOUNDED PRECEDING) FROM t"),
        glued: "SELECT sum(a) OVER (rowsx) FROM t",
        glued_reads_as: Parses::As("SELECT sum(a) OVER (rowsx) FROM t"),
    },
    SyntaxKeyword {
        covers: &["range"],
        used: "SELECT sum(a) OVER (RANGE UNBOUNDED PRECEDING) FROM t",
        renders: Parses::As("SELECT sum(a) OVER (RANGE UNBOUNDED PRECEDING) FROM t"),
        glued: "SELECT sum(a) OVER (rangex) FROM t",
        glued_reads_as: Parses::As("SELECT sum(a) OVER (rangex) FROM t"),
    },
    SyntaxKeyword {
        covers: &["unbounded", "preceding"],
        used: "SELECT sum(a) OVER (ROWS UNBOUNDED PRECEDING) FROM t",
        renders: Parses::As("SELECT sum(a) OVER (ROWS UNBOUNDED PRECEDING) FROM t"),
        glued: "SELECT sum(a) OVER (ROWS UNBOUNDED PRECEDINGx) FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["following"],
        used: "SELECT sum(a) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t",
        renders: Parses::As(
            "SELECT sum(a) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM t",
        ),
        glued: "SELECT sum(a) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWINGx) FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["current", "row"],
        used: "SELECT sum(a) OVER (ROWS CURRENT ROW) FROM t",
        renders: Parses::As("SELECT sum(a) OVER (ROWS CURRENT ROW) FROM t"),
        glued: "SELECT sum(a) OVER (ROWS CURRENT ROWx) FROM t",
        glued_reads_as: Parses::Not,
    },
    // ---- expression operators
    SyntaxKeyword {
        covers: &["and"],
        used: "SELECT a FROM t WHERE a = 1 AND b = 2",
        renders: Parses::As("SELECT a FROM t WHERE a = 1 AND b = 2"),
        glued: "SELECT a FROM t WHERE a = 1 ANDx b = 2",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["or"],
        used: "SELECT a FROM t WHERE a = 1 OR b = 2",
        renders: Parses::As("SELECT a FROM t WHERE a = 1 OR b = 2"),
        glued: "SELECT a FROM t WHERE a = 1 ORx b = 2",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["not"],
        used: "SELECT a FROM t WHERE NOT a",
        renders: Parses::As("SELECT a FROM t WHERE NOT a"),
        glued: "SELECT a FROM t WHERE notx",
        glued_reads_as: Parses::As("SELECT a FROM t WHERE notx"),
    },
    SyntaxKeyword {
        covers: &["between"],
        used: "SELECT a FROM t WHERE a BETWEEN 1 AND 2",
        renders: Parses::As("SELECT a FROM t WHERE a BETWEEN 1 AND 2"),
        glued: "SELECT a FROM t WHERE a BETWEENx 1 AND 2",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["in"],
        used: "SELECT a FROM t WHERE a IN (1, 2)",
        renders: Parses::As("SELECT a FROM t WHERE a IN (1, 2)"),
        glued: "SELECT a FROM t WHERE a INx (1, 2)",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["is"],
        used: "SELECT a FROM t WHERE a IS NULL",
        renders: Parses::As("SELECT a FROM t WHERE a IS NULL"),
        glued: "SELECT a FROM t WHERE a ISx NULL",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["unknown"],
        used: "SELECT a FROM t WHERE a IS UNKNOWN",
        renders: Parses::As("SELECT a FROM t WHERE a IS NULL"),
        glued: "SELECT a FROM t WHERE a IS UNKNOWNx",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["like"],
        used: "SELECT a FROM t WHERE a LIKE 'x'",
        renders: Parses::As("SELECT a FROM t WHERE a LIKE 'x'"),
        glued: "SELECT a FROM t WHERE a LIKEx 'x'",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["ilike"],
        used: "SELECT a FROM t WHERE a ILIKE 'x'",
        renders: Parses::As("SELECT a FROM t WHERE a ILIKE 'x'"),
        glued: "SELECT a FROM t WHERE a ILIKEx 'x'",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["similar"],
        used: "SELECT a FROM t WHERE a SIMILAR 'x'",
        renders: Parses::As("SELECT a FROM t WHERE a SIMILAR 'x'"),
        glued: "SELECT a FROM t WHERE a SIMILARx 'x'",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["escape"],
        used: "SELECT a FROM t WHERE a LIKE 'x' ESCAPE '!'",
        renders: Parses::As("SELECT a FROM t WHERE a LIKE 'x' ESCAPE '!'"),
        glued: "SELECT a FROM t WHERE a LIKE 'x' ESCAPEx '!'",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["exists"],
        used: "SELECT a FROM t WHERE EXISTS (SELECT b FROM u)",
        renders: Parses::As("SELECT a FROM t WHERE EXISTS (SELECT b FROM u)"),
        glued: "SELECT existsx FROM t",
        glued_reads_as: Parses::As("SELECT existsx FROM t"),
    },
    // ---- literals
    SyntaxKeyword {
        covers: &["true", "false"],
        used: "SELECT true, false FROM t",
        renders: Parses::As("SELECT true, false FROM t"),
        glued: "SELECT truex, falsex FROM t",
        glued_reads_as: Parses::As("SELECT truex, falsex FROM t"),
    },
    SyntaxKeyword {
        covers: &["null"],
        used: "SELECT null FROM t",
        renders: Parses::As("SELECT null FROM t"),
        glued: "SELECT nullx FROM t",
        glued_reads_as: Parses::As("SELECT nullx FROM t"),
    },
    SyntaxKeyword {
        covers: &["e"],
        used: "SELECT 1e3 FROM t",
        renders: Parses::As("SELECT 1e3 FROM t"),
        glued: "SELECT 1ex FROM t",
        glued_reads_as: Parses::Not,
    },
    // ---- CASE
    SyntaxKeyword {
        covers: &["case"],
        used: "SELECT CASE WHEN a THEN 1 ELSE 2 END FROM t",
        renders: Parses::As("SELECT CASE WHEN a THEN 1 ELSE 2 END FROM t"),
        glued: "SELECT casex FROM t",
        glued_reads_as: Parses::As("SELECT casex FROM t"),
    },
    SyntaxKeyword {
        covers: &["when"],
        used: "SELECT CASE WHEN a THEN 1 END FROM t",
        renders: Parses::As("SELECT CASE WHEN a THEN 1 END FROM t"),
        glued: "SELECT CASE WHENx a THEN 1 END FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["then"],
        used: "SELECT CASE WHEN a THEN 1 END FROM t",
        renders: Parses::As("SELECT CASE WHEN a THEN 1 END FROM t"),
        glued: "SELECT CASE WHEN a THENx 1 END FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["else"],
        used: "SELECT CASE WHEN a THEN 1 ELSE 2 END FROM t",
        renders: Parses::As("SELECT CASE WHEN a THEN 1 ELSE 2 END FROM t"),
        glued: "SELECT CASE WHEN a THEN 1 ELSEx 2 END FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["end"],
        used: "SELECT CASE WHEN a THEN 1 END FROM t",
        renders: Parses::As("SELECT CASE WHEN a THEN 1 END FROM t"),
        glued: "SELECT CASE WHEN a THEN 1 ENDx FROM t",
        glued_reads_as: Parses::Not,
    },
    // ---- functions and casts
    SyntaxKeyword {
        covers: &["cast"],
        used: "SELECT CAST(a AS int) FROM t",
        renders: Parses::As("SELECT CAST(a AS int) FROM t"),
        glued: "SELECT castx(a) FROM t",
        glued_reads_as: Parses::As("SELECT castx(a) FROM t"),
    },
    SyntaxKeyword {
        covers: &["array"],
        used: "SELECT ARRAY[1, 2] FROM t",
        renders: Parses::As("SELECT ARRAY[1, 2] FROM t"),
        glued: "SELECT arrayx[1] FROM t",
        glued_reads_as: Parses::As("SELECT arrayx[1] FROM t"),
    },
    SyntaxKeyword {
        // The cast-target spelling of the same literal: `int ARRAY` is `int[]`,
        // but `int arrayx` is a cast to `int` wearing an alias.
        covers: &["array"],
        used: "SELECT a::int ARRAY FROM t",
        renders: Parses::As("SELECT a::int[] FROM t"),
        glued: "SELECT a::int arrayx FROM t",
        glued_reads_as: Parses::As("SELECT a::int AS arrayx FROM t"),
    },
    SyntaxKeyword {
        covers: &["trim"],
        used: "SELECT TRIM(a) FROM t",
        renders: Parses::As("SELECT TRIM(a) FROM t"),
        glued: "SELECT trimx(a) FROM t",
        glued_reads_as: Parses::As("SELECT trimx(a) FROM t"),
    },
    SyntaxKeyword {
        covers: &["leading"],
        used: "SELECT TRIM(LEADING 'x' FROM a) FROM t",
        renders: Parses::As("SELECT TRIM(LEADING 'x' FROM a) FROM t"),
        // Without the keyword reading, `leadingx` is just the trim pattern.
        glued: "SELECT TRIM(leadingx FROM a) FROM t",
        glued_reads_as: Parses::As("SELECT TRIM(leadingx FROM a) FROM t"),
    },
    SyntaxKeyword {
        covers: &["trailing"],
        used: "SELECT TRIM(TRAILING 'x' FROM a) FROM t",
        renders: Parses::As("SELECT TRIM(TRAILING 'x' FROM a) FROM t"),
        glued: "SELECT TRIM(trailingx FROM a) FROM t",
        glued_reads_as: Parses::As("SELECT TRIM(trailingx FROM a) FROM t"),
    },
    SyntaxKeyword {
        covers: &["both"],
        used: "SELECT TRIM(BOTH 'x' FROM a) FROM t",
        renders: Parses::As("SELECT TRIM(BOTH 'x' FROM a) FROM t"),
        glued: "SELECT TRIM(bothx FROM a) FROM t",
        glued_reads_as: Parses::As("SELECT TRIM(bothx FROM a) FROM t"),
    },
    SyntaxKeyword {
        covers: &["substring", "for"],
        used: "SELECT SUBSTRING(a FROM 2 FOR 3) FROM t",
        renders: Parses::As("SELECT SUBSTRING(a FROM 2 FOR 3) FROM t"),
        glued: "SELECT substringx(a) FROM t",
        glued_reads_as: Parses::As("SELECT substringx(a) FROM t"),
    },
    SyntaxKeyword {
        covers: &["for"],
        used: "SELECT SUBSTRING(a FOR 3) FROM t",
        renders: Parses::As("SELECT SUBSTRING(a FOR 3) FROM t"),
        glued: "SELECT SUBSTRING(a FORx 3) FROM t",
        glued_reads_as: Parses::Not,
    },
    // ---- datetime niladics
    SyntaxKeyword {
        covers: &["current_date"],
        used: "SELECT CURRENT_DATE FROM t",
        renders: Parses::As("SELECT CURRENT_DATE FROM t"),
        glued: "SELECT current_date_col FROM t",
        glued_reads_as: Parses::As("SELECT current_date_col FROM t"),
    },
    SyntaxKeyword {
        covers: &["current_time"],
        used: "SELECT CURRENT_TIME(3) FROM t",
        renders: Parses::As("SELECT CURRENT_TIME(3) FROM t"),
        glued: "SELECT current_timex FROM t",
        glued_reads_as: Parses::As("SELECT current_timex FROM t"),
    },
    SyntaxKeyword {
        covers: &["current_timestamp"],
        used: "SELECT CURRENT_TIMESTAMP FROM t",
        renders: Parses::As("SELECT CURRENT_TIMESTAMP FROM t"),
        glued: "SELECT current_timestamp_utc FROM t",
        glued_reads_as: Parses::As("SELECT current_timestamp_utc FROM t"),
    },
    SyntaxKeyword {
        covers: &["localtime"],
        used: "SELECT LOCALTIME FROM t",
        renders: Parses::As("SELECT LOCALTIME FROM t"),
        glued: "SELECT localtime_ms FROM t",
        glued_reads_as: Parses::As("SELECT localtime_ms FROM t"),
    },
    SyntaxKeyword {
        covers: &["localtimestamp"],
        used: "SELECT LOCALTIMESTAMP FROM t",
        renders: Parses::As("SELECT LOCALTIMESTAMP FROM t"),
        glued: "SELECT localtimestampx FROM t",
        glued_reads_as: Parses::As("SELECT localtimestampx FROM t"),
    },
    // ---- cast targets
    SyntaxKeyword {
        covers: &["bool"],
        used: "SELECT a::bool FROM t",
        renders: Parses::As("SELECT a::bool FROM t"),
        glued: "SELECT a::boolx FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["boolean"],
        used: "SELECT a::boolean FROM t",
        renders: Parses::As("SELECT a::bool FROM t"),
        glued: "SELECT a::booleanx FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["datetime"],
        used: "SELECT a::datetime FROM t",
        renders: Parses::As("SELECT a::datetime FROM t"),
        glued: "SELECT a::datetimex FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["decimal"],
        used: "SELECT a::decimal FROM t",
        renders: Parses::As("SELECT a::decimal FROM t"),
        glued: "SELECT a::decimalx FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["number"],
        used: "SELECT a::number FROM t",
        renders: Parses::As("SELECT a::decimal FROM t"),
        glued: "SELECT a::numberx FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["numeric"],
        used: "SELECT a::numeric FROM t",
        renders: Parses::As("SELECT a::decimal FROM t"),
        glued: "SELECT a::numericx FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["double"],
        used: "SELECT a::double FROM t",
        renders: Parses::As("SELECT a::double FROM t"),
        glued: "SELECT a::doublex FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["int"],
        used: "SELECT a::int FROM t",
        renders: Parses::As("SELECT a::int FROM t"),
        glued: "SELECT a::intx FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["integer"],
        used: "SELECT a::integer FROM t",
        renders: Parses::As("SELECT a::int FROM t"),
        glued: "SELECT a::integerx FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["bigint"],
        used: "SELECT a::bigint FROM t",
        renders: Parses::As("SELECT a::int FROM t"),
        glued: "SELECT a::bigintx FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["smallint"],
        used: "SELECT a::smallint FROM t",
        renders: Parses::As("SELECT a::int FROM t"),
        glued: "SELECT a::smallintx FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        // `CastType` has no scalar JSON variant, so the keyword only reaches a
        // supported cast as an array element type.
        covers: &["json"],
        used: "SELECT a::json[] FROM t",
        renders: Parses::As("SELECT a::json[] FROM t"),
        glued: "SELECT a::jsonx[] FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["json"],
        used: "SELECT a::json FROM t",
        renders: Parses::Not,
        glued: "SELECT a::jsonx FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["string"],
        used: "SELECT a::string FROM t",
        renders: Parses::As("SELECT a::string FROM t"),
        glued: "SELECT a::stringx FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["text"],
        used: "SELECT a::text FROM t",
        renders: Parses::As("SELECT a::string FROM t"),
        glued: "SELECT a::textx FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["varchar"],
        used: "SELECT a::varchar(10) FROM t",
        renders: Parses::As("SELECT a::string FROM t"),
        glued: "SELECT a::varcharx(10) FROM t",
        glued_reads_as: Parses::Not,
    },
    SyntaxKeyword {
        covers: &["uuid"],
        used: "SELECT a::uuid FROM t",
        renders: Parses::As("SELECT a::uuid FROM t"),
        glued: "SELECT a::uuidx FROM t",
        glued_reads_as: Parses::Not,
    },
];

/// The token mechanism, in both directions, for every keyword the syntax rules
/// spell.
///
/// All cases run before the assertion so that one run reports every broken
/// keyword rather than only the first.
#[test]
fn syntax_keywords_parse_used_and_are_never_matched_when_glued() {
    let mut failures = Vec::new();

    for case in SYNTAX_KEYWORDS {
        let covered = case.covers.join(", ");
        let mut check = |query: &str, expected: &Parses| match (render(query), expected) {
            (Ok(tree), Parses::As(expected)) if tree == *expected => {}
            (Err(_), Parses::Not) => {}
            (Ok(tree), Parses::As(expected)) => failures.push(format!(
                "[{covered}] `{query}` parsed as `{tree}`, expected `{expected}`"
            )),
            (Ok(tree), Parses::Not) => failures.push(format!(
                "[{covered}] `{query}` should be rejected, parsed as `{tree}`"
            )),
            (Err(err), Parses::As(expected)) => failures.push(format!(
                "[{covered}] `{query}` should parse as `{expected}`, failed: {err}"
            )),
        };

        check(case.used, &case.renders);
        check(case.glued, &case.glued_reads_as);
    }

    assert!(
        failures.is_empty(),
        "{} syntax-keyword case(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// The completeness gate. A keyword literal added to `query.pest` without a
/// [`SYNTAX_KEYWORDS`] row fails here, and a row naming a literal the grammar no
/// longer spells fails here too.
#[test]
fn syntax_keyword_table_covers_the_grammar() {
    let (_, syntax) = grammar_keywords();
    let covered: BTreeSet<&str> = SYNTAX_KEYWORDS
        .iter()
        .flat_map(|case| case.covers.iter().copied())
        .collect();

    let missing: Vec<&String> = syntax
        .iter()
        .filter(|kw| !covered.contains(kw.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "the grammar spells keyword(s) with no case in `SYNTAX_KEYWORDS`: {missing:?}"
    );

    let stale: Vec<&str> = covered
        .iter()
        .filter(|kw| !syntax.contains(**kw))
        .copied()
        .collect();
    assert!(
        stale.is_empty(),
        "`SYNTAX_KEYWORDS` names keyword(s) the grammar no longer spells: {stale:?}"
    );
}

//////////////////////////////// Boundary diagnostics ////////////////////////////////

fn parse_error(query: &str) -> String {
    PairParser::parse(Rule::Command, query)
        .expect_err("expected a parsing failure")
        .to_string()
}

/// The shape of a boundary diagnosis: pest reports the guard rule itself, at
/// the character that failed to end the token.
#[test]
fn boundary_failures_point_at_the_glued_character() {
    insta::assert_snapshot!(parse_error("SELECT $1and FROM t"), @"
     --> 1:10
      |
    1 | SELECT $1and FROM t
      |          ^---
      |
      = expected IdentifierInapplicableSymbol
    ");
    insta::assert_snapshot!(parse_error("SELECT 1and FROM t"), @"
     --> 1:9
      |
    1 | SELECT 1and FROM t
      |         ^---
      |
      = expected IdentifierInapplicableSymbol
    ");
}

/// Where a glued keyword has no identifier reading to fall back to, the
/// rejection must still come *from the boundary* rather than from whatever rule
/// happens to fail downstream. Dropping a token's `KwEnd` leaves the query
/// rejected either way, so only this check tells the two apart — one entry per
/// guarded rule family.
#[test]
fn rejections_without_an_identifier_reading_come_from_the_boundary() {
    let queries = [
        "SELECT a FROM t UNION ALLx SELECT b FROM u",
        "SELECT a FROM t ORDER BY a ASCx",
        "SELECT a FROM t ORDER BY a DESCx",
        "SELECT a FROM t ORDER BY a NULLS FIRSTx",
        "SELECT a FROM t ORDER BY a NULLS LASTx",
        "SELECT a FROM t LIMIT ALLx",
        "SELECT a FROM t OPTION (read_preference = leaderx)",
        "SELECT a FROM t OPTION (read_preference = replicax)",
        "SELECT a FROM t OPTION (read_preference = anyx)",
        "SELECT a FROM t OPTION (forward = onx)",
        "SELECT a FROM t OPTION (forward = offx)",
        "SELECT a FROM t OPTION (forward = ro_to_rwx)",
        "SELECT sum(a) OVER (ROWS UNBOUNDED PRECEDINGx) FROM t",
        "SELECT sum(a) OVER (ROWS 1 PRECEDINGx) FROM t",
        "SELECT sum(a) OVER (ROWS CURRENT ROWx) FROM t",
        "SELECT sum(a) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWINGx) FROM t",
        "SELECT sum(a) OVER (ROWS BETWEEN CURRENT ROW AND 1 FOLLOWINGx) FROM t",
        "SELECT CASE WHEN a THEN 1 ENDx FROM t",
        "SELECT a::intx FROM t",
    ];

    let failures: Vec<String> = queries
        .iter()
        .map(|query| (query, parse_error(query)))
        .filter(|(_, err)| !err.contains("expected IdentifierInapplicableSymbol"))
        .map(|(query, err)| format!("`{query}` was not rejected by the boundary:\n{err}"))
        .collect();

    assert!(
        failures.is_empty(),
        "{} boundary case(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// A keyword glued to an *operand* rather than to a word still reads as the
/// keyword: `KwEnd` is a boundary, not a whitespace requirement, which is what
/// lets `(true)and(false)` parse the way PostgreSQL's lexer makes it parse.
#[test]
fn boundaries_do_not_require_whitespace() {
    let cases = [
        (
            "SELECT a FROM t WHERE a in(1, 2)",
            "SELECT a FROM t WHERE a IN (1, 2)",
        ),
        (
            "SELECT a FROM t WHERE (true)between(false)and(true)",
            "SELECT a FROM t WHERE true BETWEEN false AND true",
        ),
        (
            "SELECT a FROM t WHERE not(false)",
            "SELECT a FROM t WHERE NOT false",
        ),
        (
            "SELECT a FROM t WHERE (a)is null",
            "SELECT a FROM t WHERE a IS NULL",
        ),
        (
            "SELECT sum(a) OVER(PARTITION BY b) FROM t",
            "SELECT sum(a) OVER (PARTITION BY b) FROM t",
        ),
        ("SELECT a::int[] FROM t", "SELECT a::int[] FROM t"),
    ];
    for (query, expected) in cases {
        assert_eq!(render(query).as_deref(), Ok(expected), "for `{query}`");
    }
}

/// A delimited identifier is outside the keyword machinery entirely: quoting is
/// what lets a reserved word name a column, and the rendering has to quote it
/// back or the tree would not parse to itself.
#[test]
fn delimited_identifiers_escape_reservation() {
    let (reserved, _) = grammar_keywords();
    let mut failures = Vec::new();

    for word in &reserved {
        let query = format!(r#"SELECT "{word}" FROM t"#);
        let expected = format!(r#"SELECT "{word}" FROM t"#);
        match render(&query) {
            Ok(tree) if tree == expected => {}
            Ok(tree) => failures.push(format!("`{query}` parsed as `{tree}`")),
            Err(err) => failures.push(format!("`{query}` failed: {err}")),
        }
    }

    assert!(
        failures.is_empty(),
        "{} delimited-identifier case(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
