//! SQL frontend module.
//!
//! Parses an SQL statement to the abstract syntax tree (AST)
//! and builds the intermediate representation (IR).

use smol_str::StrExt;
use tarantool::datetime::Datetime;

use crate::errors::SbroadError;
use crate::executor::engine::helpers::normalize_name_from_sql;
use crate::executor::engine::Metadata;
use crate::ir::node::expression::Expression;
use crate::ir::node::{Constant, NodeId};
use crate::ir::types::DerivedType;
use crate::ir::value::Value;
use crate::ir::Plan;

use crate::frontend::sql::planner::Planner;

mod ast_new;
mod planner;

pub mod ast;
pub mod ir;
pub mod tree;
mod type_system;

trait Ast<'q> {
    type AnalyzedAst;

    fn new(query: &'q str) -> Result<Self, SbroadError>
    where
        Self: Sized;

    fn analyze(
        self,
        metadata: &'q impl Metadata,
        param_types: &'q [DerivedType],
    ) -> Result<Self::AnalyzedAst, SbroadError>;
}

pub(crate) fn transform_into_plan(
    query: &str,
    param_types: &[DerivedType],
    metadata: &impl Metadata,
) -> Result<Plan, SbroadError> {
    if let Ok(ast) = crate::frontend::sql::ast_new::AbstractSyntaxTree::new(query) {
        if let Ok(analyzed) = ast.analyze(metadata, param_types) {
            if let Ok(plan) = Planner::new().plan(analyzed) {
                return Ok(plan);
            }
        }
    }
    transform_into_plan_old(query, param_types, metadata)
}

fn transform_into_plan_old(
    query: &str,
    param_types: &[DerivedType],
    metadata: &impl Metadata,
) -> Result<Plan, SbroadError> {
    let mut ast = crate::frontend::sql::ast::AbstractSyntaxTree::new(query)?;
    ast = ast.analyze(metadata, param_types)?;
    ast.build_ir(metadata, param_types)
}

/// Holds naming metadata for SQL function.
/// Used mostly for correct mapping between identifiers across different subsystems.
#[derive(Default)]
pub struct FunctionNameMapping {
    /// Function name in SQL as exposed to the users (e.g., in queries).
    pub sql: &'static str,
    /// Rust function in the source code, exposed using `#[tarantool::proc]`.
    pub rust_procedure: &'static str,
    /// Used when calling it via Tarantool, composed as '.' + name in sources.
    ///
    /// # Background
    /// - Tarantool looks for `lib<name>` when using plain names (exported by Picodata).
    /// - With `.<name>`, Tarantool searches the current executable instead.
    /// - This is needed for `box.func['proc_name']:call()` and `box.execute("select proc_name()")`.
    ///
    /// Using `.proc_name` makes Tarantool look for `proc_name` in the current executable,
    /// while `proc_name` would make it search for `libproc_name.so` containing `proc_name`.
    pub tarantool_symbol: &'static str,
    /// Initially, we made these functions volatile, but they lack in usability.
    /// These parameters might serve as modifiers to change the volatility to
    /// stable, allowing to a wider usage.
    /// See <https://git.picodata.io/core/picodata/-/issues/2064> for more information.
    /// **NOTE**: uses Tarantool type names.
    pub parameter_list: &'static [&'static str],
}

/// Stores all identifiers mappings for the functions.
pub const FUNCTION_NAME_MAPPINGS: &[FunctionNameMapping] = &[
    // TODO:
    // Deprecated, remove in the future version.
    // Consider using `pico_instance_uuid` instead.
    FunctionNameMapping {
        sql: "instance_uuid",
        rust_procedure: "proc_instance_uuid",
        tarantool_symbol: ".proc_instance_uuid",
        parameter_list: &[],
    },
    FunctionNameMapping {
        sql: "pico_config_file_path",
        rust_procedure: "proc_config_file",
        tarantool_symbol: ".proc_config_file",
        parameter_list: &["string"],
    },
    FunctionNameMapping {
        sql: "pico_instance_dir",
        rust_procedure: "proc_instance_dir",
        tarantool_symbol: ".proc_instance_dir",
        parameter_list: &["string"],
    },
    FunctionNameMapping {
        sql: "pico_instance_name",
        rust_procedure: "proc_instance_name",
        tarantool_symbol: ".proc_instance_name",
        parameter_list: &["string"],
    },
    FunctionNameMapping {
        sql: "pico_instance_uuid",
        rust_procedure: "proc_instance_uuid",
        tarantool_symbol: ".proc_instance_uuid",
        parameter_list: &[],
    },
    FunctionNameMapping {
        sql: "pico_raft_leader_id",
        rust_procedure: "proc_raft_leader_id",
        tarantool_symbol: ".proc_raft_leader_id",
        parameter_list: &[],
    },
    FunctionNameMapping {
        sql: "pico_raft_leader_uuid",
        rust_procedure: "proc_raft_leader_uuid",
        tarantool_symbol: ".proc_raft_leader_uuid",
        parameter_list: &[],
    },
    FunctionNameMapping {
        sql: "pico_replicaset_name",
        rust_procedure: "proc_replicaset_name",
        tarantool_symbol: ".proc_replicaset_name",
        parameter_list: &["string"],
    },
    FunctionNameMapping {
        sql: "pico_tier_name",
        rust_procedure: "proc_tier_name",
        tarantool_symbol: ".proc_tier_name",
        parameter_list: &["string"],
    },
    FunctionNameMapping {
        sql: "version",
        rust_procedure: "proc_picodata_version",
        tarantool_symbol: ".proc_picodata_version",
        parameter_list: &[],
    },
];

/// Maps (maybe quoted or uppercased) name from user to real procedure name in tarantool.
/// Real name stands for name in _func space.
pub fn get_real_function_name(name_from_sql: &str) -> Option<&'static str> {
    let normalized_name = normalize_name_from_sql(name_from_sql);
    FUNCTION_NAME_MAPPINGS
        .iter()
        .find(|&mapping| mapping.sql == normalized_name)
        .map(|mapping| mapping.tarantool_symbol)
}

///This function converts an SQL pattern into a regex string, validating escape characters.
///
///Taken from PG source. Function similar_escape_internal <https://github.com/postgres/postgres/blob/c623e8593ec4ee6987f3cd9350ced7caf8526ed2/src/backend/utils/adt/regexp.c#L767>
pub fn transform_to_regex_pattern(pat_text: &str, esc_text: &str) -> Result<String, String> {
    let escape = match esc_text {
        e if e.len() == 1 => e,
        "" => "",
        _ => {
            return Err(
                "invalid escape string. Escape string must be empty or one character.".into(),
            )
        }
    };

    let mut result = String::from("^(?:");
    let mut after_escape = false;
    let mut in_char_class = false;
    let mut nquotes = 0;

    let chars = pat_text.chars().peekable();

    for c in chars {
        if after_escape {
            if c == '"' && !in_char_class {
                // escape-double-quote?
                if nquotes == 0 {
                    // First quote: end first part, make it non-greedy
                    result.push_str("){1,1}?");
                    result.push('(');
                } else if nquotes == 1 {
                    // Second quote: end second part, make it greedy
                    result.push_str("){1,1}(");
                    result.push_str("?:");
                } else {
                    return Err("SQL regular expression may not contain more than two escape-double-quote separators".into());
                }
                nquotes += 1;
            } else {
                // Escape any character
                result.push('\\');
                result.push(c);
            }
            after_escape = false;
        } else if c.to_string() == escape {
            // SQL escape character; do not send to output
            after_escape = true;
        } else if in_char_class {
            if c == '\\' {
                result.push('\\');
            }
            result.push(c);
            if c == ']' {
                in_char_class = false;
            }
        } else {
            match c {
                '[' => {
                    result.push(c);
                    in_char_class = true;
                }
                '%' => {
                    result.push_str(".*");
                }
                '_' => {
                    result.push('.');
                }
                '(' => {
                    result.push_str("(?:");
                }
                '\\' | '.' | '^' | '$' => {
                    result.push('\\');
                    result.push(c);
                }
                _ => {
                    result.push(c);
                }
            }
        }
    }

    result.push_str(")$");
    Ok(result)
}

pub fn is_negative_number(plan: &Plan, expr_id: NodeId) -> Result<bool, SbroadError> {
    if let Expression::Constant(Constant { value }) = plan.get_expression_node(expr_id)? {
        return Ok(matches!(value, Value::Integer(n) if *n < 0));
    }

    Ok(false)
}

/// Parse boolean values in text format.
/// It supports the same formats as PostgreSQL (grep `parse_bool_with_len`).
pub fn try_parse_bool(s: &str) -> Option<bool> {
    match s.to_lowercase_smolstr().as_str() {
        "t" | "true" | "yes" | "on" | "1" => Some(true),
        "f" | "false" | "no" | "off" | "0" => Some(false),
        _ => None,
    }
}

/// Parse datetime values in text format.
///
/// It tries to support the same formats as in PostgreSQL.
/// PostgreSQL can parse arbitrary datetime formats, as demonstrated in the following functions:
/// * [timestamptz_in](https://github.com/postgres/postgres/blob/ba8f00eef6d/src/backend/utils/adt/timestamp.c#L416)
/// * [ParseDateTime](https://github.com/postgres/postgres/blob/ba8f00eef6d/src/interfaces/ecpg/pgtypeslib/dt_common.c#L1598)
/// * [DecodeDateTime](https://github.com/postgres/postgres/blob/ba8f00eef6d/src/interfaces/ecpg/pgtypeslib/dt_common.c#L1780)
///
/// Since supporting all these formats is impractical, we will focus on parsing some
/// known formats that enable interaction with PostgreSQL drivers.
pub fn try_parse_datetime(s: &str) -> Option<Datetime> {
    use time::format_description::well_known::{Iso8601, Rfc2822, Rfc3339};
    use time::macros::format_description;

    fn try_from_date_without_time(s: &str) -> Option<time::OffsetDateTime> {
        let format = format_description!("[year]-[month]-[day]");

        if let Ok(date) = time::Date::parse(s, format) {
            let dt = date.with_hms(0, 0, 0).ok()?.assume_utc();
            return Some(dt);
        }

        None
    }

    fn try_from_well_known_formats(s: &str) -> Option<time::OffsetDateTime> {
        if let Ok(datetime) = time::OffsetDateTime::parse(s, &Iso8601::PARSING) {
            return Some(datetime);
        }
        if let Ok(datetime) = time::OffsetDateTime::parse(s, &Rfc2822) {
            return Some(datetime);
        }
        if let Ok(datetime) = time::OffsetDateTime::parse(s, &Rfc3339) {
            return Some(datetime);
        }

        None
    }

    fn try_from_custom_formats(s: &str) -> Option<time::OffsetDateTime> {
        // Formats used for encoding timestamptz values.
        // https://time-rs.github.io/book/api/format-description.html
        let formats = [
            format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second][offset_hour]"
            ),
            format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second][offset_hour]:[offset_minute]"
            ),
            format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond][offset_hour]"
            ),
            format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond][offset_hour]:[offset_minute]"
            )
        ];

        for fmt in formats {
            if let Ok(datetime) = time::OffsetDateTime::parse(s, &fmt) {
                return Some(datetime);
            }
        }

        None
    }

    if let Some(datetime) = try_from_well_known_formats(s) {
        return Some(datetime.into());
    }

    if let Some(datetime) = try_from_custom_formats(s) {
        return Some(datetime.into());
    }

    if let Some(datetime) = try_from_date_without_time(s) {
        return Some(datetime.into());
    }

    None
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_datetime_parse_rfc_3339() {
        // https://git.picodata.io/core/picodata/-/issues/1945
        // test that parsing RFC 3339 with non-default separator (` ` instead of `T`) works
        let datetime = super::try_parse_datetime("2025-10-18 02:59:59Z").unwrap();
        assert_eq!(datetime.to_string(), "2025-10-18 2:59:59.0 +00:00:00");
    }

    #[test]
    fn test_datetime_parse_yyyy_mm_dd() {
        let datetime = super::try_parse_datetime("2025-10-18").unwrap();
        assert_eq!(datetime.to_string(), "2025-10-18 0:00:00.0 +00:00:00");
    }
}
