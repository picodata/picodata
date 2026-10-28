use smallvec::SmallVec;
use smol_str::SmolStr;
use sql_ir::ir::node::{BlockEntryKind, IfBranch, StatementLocation};
use sqlparser::dialect::GenericDialect;
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::{Token, Tokenizer, Whitespace};
use std::fmt;
use std::fmt::Write as _;

use crate::explain::executor::LINE_WIDTH;

pub fn format_sql(explain: &str, params: &[impl ToString], should_fmt: bool) -> String {
    let sql = explain
        .strip_prefix("EXPLAIN QUERY PLAN ")
        .unwrap_or(explain);
    format_query(sql, params, should_fmt)
}

/// Formats the query `sql` as `EXPLAIN (RAW)` does, with the values `params`
/// in place of `$1`, `$2` and so on. A query is on one line, unless
/// `should_fmt` is set and the query is at least [`LINE_WIDTH`] long: then
/// each clause starts a new line and its items are indented.
///
/// The formatter does not parse SQL, and some text it changes: a line comment
/// takes the rest of a single line, and a backslash in a string literal is
/// an escape for it. When the formatted text has other tokens than `sql`,
/// the function gives `sql` unchanged.
///
/// The formatter takes quadratic time or memory on some text, and a query can
/// come from a user without privileges. Queries exceeding the limits below
/// stay unchanged.
pub fn format_query(sql: &str, params: &[impl ToString], should_fmt: bool) -> String {
    if exceeds_text_limits(sql) {
        return sql.to_owned();
    }
    let Ok(before) = Tokenizer::new(&GenericDialect, sql)
        .with_unescape(false)
        .tokenize()
    else {
        return sql.to_owned();
    };
    if exceeds_nesting_limit(&before) {
        return sql.to_owned();
    }

    let mut fmt_options = sqlformat::FormatOptions::<'_> {
        joins_as_top_level: true,
        inline: true,
        ..Default::default()
    };

    if should_fmt && sql.len() >= LINE_WIDTH {
        fmt_options.joins_as_top_level = false;
        fmt_options.inline = false;
    }

    // The values are not SQL text of the query, so the tokens are compared
    // before they replace the placeholders.
    let formatted = sqlformat::format(sql, &sqlformat::QueryParams::None, &fmt_options);
    let Ok(after) = Tokenizer::new(&GenericDialect, &formatted)
        .with_unescape(false)
        .tokenize()
    else {
        return sql.to_owned();
    };
    let significant = |token: &Token| {
        !matches!(
            token,
            Token::Whitespace(Whitespace::Space | Whitespace::Newline | Whitespace::Tab)
        )
    };
    if !before
        .into_iter()
        .filter(significant)
        .eq(after.into_iter().filter(significant))
    {
        return sql.to_owned();
    }
    if params.is_empty() {
        return formatted;
    }

    // A value replaces `$N` as text, and `::` binds tighter than the
    // unary minus: `$1::int` with -1 reads `(-1)::int`, not `-(1::int)`.
    let mut params: Vec<String> = params.iter().map(ToString::to_string).collect();
    parenthesize_cast_negatives(sql, &mut params);
    let indexed_params = sqlformat::QueryParams::Indexed(params);

    sqlformat::format(sql, &indexed_params, &fmt_options)
}

/// How long a query may be to be formatted. For each keyword the formatter
/// copies the rest of the query, whatever its shape: 1 MB of keywords takes 2 s,
/// and that time grows with the square of the length.
const MAX_FMT_LEN: usize = 64 * 1024;

/// How many levels of nesting a query may have to be formatted. The formatter
/// indents each line by its level and looks through a block from each of its
/// parentheses, so without the limit 64 KB of parentheses take 5 GB.
const MAX_FMT_NESTING: usize = 64;

/// How long a word of a query may be to be formatted. For each token the
/// formatter copies the next few words: without spaces, that is the rest of the
/// query, and 1 MB of `a,a,a` takes minutes. A longer string literal is on the
/// safe side: it is left as it is too, though it is only one token.
const MAX_FMT_WORD_LEN: usize = 1024;

fn exceeds_text_limits(sql: &str) -> bool {
    sql.len() > MAX_FMT_LEN
        || sql
            .split_whitespace()
            .any(|word| word.len() > MAX_FMT_WORD_LEN)
}

fn exceeds_nesting_limit(tokens: &[Token]) -> bool {
    let mut level = 0_usize;
    for token in tokens {
        let keyword = match token {
            Token::Word(word) => word.keyword,
            _ => Keyword::NoKeyword,
        };
        if token == &Token::LParen || keyword == Keyword::CASE {
            level += 1;
            if level > MAX_FMT_NESTING {
                return true;
            }
        } else if token == &Token::RParen || keyword == Keyword::END {
            level = level.saturating_sub(1);
        }
    }
    false
}

/// Puts the negative values of the placeholders `$N` that `::` follows
/// in `sql` in parentheses. Quoted identifiers and string literals are skipped.
fn parenthesize_cast_negatives(sql: &str, params: &mut [String]) {
    let mut rest = sql;
    while let Some(at) = rest.find(['\'', '"', '$']) {
        let (mark, tail) = rest[at..].split_at(1);
        rest = if mark == "$" {
            let end = tail
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(tail.len());
            let (number, after) = tail.split_at(end);
            let param = number
                .parse::<usize>()
                .ok()
                .and_then(|n| params.get_mut(n.checked_sub(1)?));
            if let Some(p) = param.filter(|p| after.starts_with("::") && p.starts_with('-')) {
                *p = format!("({p})");
            }
            after
        } else {
            // A doubled quote inside a literal closes and reopens it.
            tail.split_once(mark).map_or("", |(_, after)| after)
        };
    }
}

pub fn format_no_sql(reason: impl fmt::Display) -> String {
    format!("**Error**: cannot generate SQL: {reason}")
}

pub const INDENT: &str = "  ";

/// Width of the `EXPLAIN` section headers.
pub const EXPLAIN_WIDTH: u16 = 70;

/// Width the `FMT` option formats to: textual representations
/// wider than this are not printed as a single line.
pub const FMT_WIDTH: usize = 46;

/// We use this buffer to check if the textual representation
/// is short enough to be printed as a single line.
#[derive(Default)]
pub(crate) struct TinyFmtBuffer(SmallVec<[u8; FMT_WIDTH]>);

impl TinyFmtBuffer {
    /// Does the buffer contain `'\n'`?
    pub(crate) fn has_newline(&self) -> bool {
        self.0.contains(&b'\n')
    }

    fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap()
    }
}

impl fmt::Display for TinyFmtBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl fmt::Write for TinyFmtBuffer {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let this = &mut self.0;
        let new_len = this.len().checked_add(s.len()).ok_or(fmt::Error)?;
        if new_len > this.capacity() {
            return Err(fmt::Error);
        }
        this.extend_from_slice(s.as_bytes());

        Ok(())
    }
}

pub fn make_explain_header1(s: impl ToString) -> comfy_table::Table {
    let mut header = ::comfy_table::Table::new();
    header
        .load_preset(::comfy_table::presets::UTF8_HORIZONTAL_ONLY)
        .set_content_arrangement(::comfy_table::ContentArrangement::DynamicFullWidth)
        .add_row([s.to_string()])
        .set_width(EXPLAIN_WIDTH);

    header
}

pub fn make_explain_header2(s: impl ToString) -> comfy_table::Table {
    let mut header = ::comfy_table::Table::new();
    header
        .load_preset(::comfy_table::presets::UTF8_BORDERS_ONLY)
        .apply_modifier(::comfy_table::modifiers::UTF8_ROUND_CORNERS)
        .set_content_arrangement(::comfy_table::ContentArrangement::Disabled)
        .add_row([s.to_string()]);

    header
}

/// Example:
/// ```markdown
/// ────────────────
///  # Logical plan
/// ────────────────
/// ```
#[macro_export]
macro_rules! write_explain_header1 {
    ($f:expr, $($args:tt)+) => {{
        let header = $crate::explain::utils::make_explain_header1(format!($($args)+));
        writeln!($f, "{header}")
    }};
}

/// Example:
/// ```markdown
/// ╭────────────────────╮
/// │ 1. Query (STORAGE) │
/// ╰────────────────────╯
/// ```
#[macro_export]
macro_rules! write_explain_header2 {
    ($f:expr, $($args:tt)+) => {{
        let header = $crate::explain::utils::make_explain_header2(format!($($args)+));
        writeln!($f, "{header}")
    }};
}

/// Transform a writer into an indented writer. This effect is additive.
pub fn indent<'a, D>(f: &'a mut D) -> indenter::Indented<'a, D> {
    indenter::indented(f).with_str(INDENT)
}

pub fn indent_custom<'a, D>(
    f: &'a mut D,
    inserter: &'a mut indenter::Inserter,
) -> indenter::Indented<'a, D> {
    indenter::indented(f).with_format(indenter::Format::Custom { inserter })
}

pub fn indent_with_prefix(
    level: usize,
    prefix: impl Into<SmolStr>,
) -> impl FnMut(usize, &mut dyn fmt::Write) -> fmt::Result {
    let prefix = prefix.into();
    move |line, f| {
        for _ in 0..level {
            write!(f, "{}", INDENT)?;
        }
        match line {
            0 => write!(f, "{}", prefix)?,
            _ => {
                for _ in 0..prefix.len() {
                    write!(f, " ")?;
                }
            }
        }

        Ok(())
    }
}

/// Dotted position of a block stage, trailing dot included: `2.`, `2.3.1.`.
/// See [`StatementLocation::explain_path`] for the numbering itself, which
/// error messages share so that the two always name a statement alike.
pub fn format_block_stage_number(location: &StatementLocation) -> String {
    let mut number = String::new();
    for idx in location.explain_path() {
        write!(&mut number, "{idx}.").unwrap();
    }
    number
}

/// Label of a block stage: what the statement is, behind one `If body: ` (or
/// `Else body: `) for every IF body it sits in. So `Let "x"` at the top level,
/// `If body: Let "x"` one level in, `If body: Else body: Let "x"` for one in
/// the ELSE branch of an IF nested in another IF's body.
pub fn format_block_stage_label(location: &StatementLocation) -> String {
    let mut label = String::new();
    for step in &location.body_path {
        label.push_str(match step.branch {
            IfBranch::Then => "If body: ",
            IfBranch::Else => "Else body: ",
        });
    }
    match &location.kind {
        BlockEntryKind::IfCondition => label.push_str("If cond"),
        BlockEntryKind::Query => label.push_str("Query"),
        BlockEntryKind::ReturnQuery => label.push_str("Return query"),
        BlockEntryKind::Let { var, is_used } => {
            let var = var.strip_prefix(':').unwrap_or(var.as_str());
            let s = if *is_used {
                format!("Let \"{var}\"")
            } else {
                format!("**Unused** let \"{var}\"")
            };
            label.push_str(&s);
        }
    }
    label
}
