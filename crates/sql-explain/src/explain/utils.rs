use smol_str::SmolStr;
use sql_ir::ir::node::{BlockEntryKind, IfBranch, StatementLocation};
use sql_ir::ir::value::Value;
use std::fmt;
use std::fmt::Write as _;

use crate::explain::executor::LINE_WIDTH;

pub fn format_sql(explain: &str, params: &[Value], should_fmt: bool) -> String {
    let sql = explain
        .strip_prefix("EXPLAIN QUERY PLAN ")
        .unwrap_or(explain);

    let mut fmt_options = sqlformat::FormatOptions::<'_> {
        joins_as_top_level: true,
        inline: true,
        ..Default::default()
    };

    if should_fmt && sql.len() >= LINE_WIDTH {
        fmt_options.joins_as_top_level = false;
        fmt_options.inline = false;
    }

    let params = params.iter().map(|p| p.to_string()).collect();
    let indexed_params = sqlformat::QueryParams::Indexed(params);

    sqlformat::format(sql, &indexed_params, &fmt_options)
}

pub const INDENT: &str = "  ";

pub fn make_explain_header1(s: impl ToString) -> comfy_table::Table {
    let mut header = ::comfy_table::Table::new();
    header
        .load_preset(::comfy_table::presets::UTF8_HORIZONTAL_ONLY)
        .set_content_arrangement(::comfy_table::ContentArrangement::DynamicFullWidth)
        .add_row([s.to_string()])
        .set_width(70);

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

pub fn format_let_entry(is_unused: bool, var_name: &str) -> String {
    if is_unused {
        format!("**Unused** let \"{var_name}\"")
    } else {
        format!("Let \"{var_name}\"")
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
