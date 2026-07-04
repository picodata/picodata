//! SQL frontend module.
//!
//! Parses an SQL statement to the abstract syntax tree (AST)
//! and builds the intermediate representation (IR).

use crate::errors::SbroadError;
use crate::ir::metadata::Metadata;
use crate::ir::types::DerivedType;
use crate::ir::Plan;

use crate::frontend::sql::planner::Planner;

mod ast_new;
mod error;
mod insert_conflict;
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

pub fn transform_into_plan(
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

pub use crate::ir::function::{
    get_real_function_name, FunctionNameMapping, FUNCTION_NAME_MAPPINGS,
};

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

pub use crate::ir::value::{try_parse_bool, try_parse_datetime};

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
