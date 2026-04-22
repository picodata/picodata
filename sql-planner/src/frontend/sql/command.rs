use super::ast::{ParseTree, Rule};
use super::conflict_strategy_from_rule;
use crate::errors::{Entity, SbroadError};
use crate::executor::engine::helpers::normalize_name_from_sql;
use crate::{CopyFormat, CopyFrom, CopyOptions, CopyStatement, CopyTableTarget};
use pest::iterators::Pair;
use pest::Parser;
use smol_str::{format_smolstr, SmolStr};

#[derive(Debug)]
pub enum ParsedCommand {
    Sql,
    Copy(CopyStatement),
}

pub fn parse_command(query: &str) -> Result<ParsedCommand, SbroadError> {
    let top_pair = parse_top_level_command(query)?;

    match top_pair.as_rule() {
        Rule::Copy => parse_copy(top_pair).map(ParsedCommand::Copy),
        _ => Ok(ParsedCommand::Sql),
    }
}

fn parse_top_level_command<'query>(query: &'query str) -> Result<Pair<'query, Rule>, SbroadError> {
    let mut command_pair = ParseTree::parse(Rule::Command, query)
        .map_err(|e| SbroadError::ParsingError(Entity::Rule, format_smolstr!("{e}")))?;
    Ok(command_pair
        .next()
        .expect("Query expected as a first parsing tree child."))
}

fn parse_copy(pair: Pair<'_, Rule>) -> Result<CopyStatement, SbroadError> {
    debug_assert_eq!(pair.as_rule(), Rule::Copy);

    let mut target = None;
    let mut columns = Vec::new();
    let mut options = CopyOptions::default();

    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::CopyTableName => target = Some(parse_table_name(child.as_str())?),
            Rule::Identifier => columns.push(normalize_name_from_sql(child.as_str())),
            Rule::CopyFromDirection => parse_copy_from_direction(child, &mut options)?,
            _ => {}
        }
    }

    let (schema_name, table_name) = target.ok_or_else(|| {
        SbroadError::Invalid(Entity::Query, Some("COPY table name is missing".into()))
    })?;
    let table = CopyTableTarget {
        schema_name,
        table_name,
        columns,
    };

    Ok(CopyStatement::From(CopyFrom { table, options }))
}

fn parse_copy_from_direction(
    pair: Pair<'_, Rule>,
    options: &mut CopyOptions,
) -> Result<(), SbroadError> {
    debug_assert_eq!(pair.as_rule(), Rule::CopyFromDirection);

    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::CopyTarget => parse_copy_target(child.as_str())?,
            Rule::CopyWithOptions => parse_with_options(options, child)?,
            Rule::DoNothing | Rule::DoReplace | Rule::DoFail => {
                options.conflict_strategy = conflict_strategy_from_rule(child.as_rule())
                    .expect("COPY conflict strategy rule should be supported");
            }
            unexpected => {
                return Err(SbroadError::ParsingError(
                    Entity::Query,
                    format_smolstr!("unsupported COPY FROM child: {unexpected:?}"),
                ));
            }
        }
    }

    Ok(())
}

fn parse_copy_target(raw: &str) -> Result<(), SbroadError> {
    if raw.eq_ignore_ascii_case("stdin") {
        Ok(())
    } else {
        Err(SbroadError::ParsingError(
            Entity::Query,
            format_smolstr!("unsupported COPY target: {raw}"),
        ))
    }
}

fn parse_with_options(options: &mut CopyOptions, pair: Pair<'_, Rule>) -> Result<(), SbroadError> {
    for option in pair.into_inner() {
        match option.as_rule() {
            Rule::CopyFormatOption => {
                options.format = parse_copy_format(&parse_option_value(option))?;
            }
            Rule::CopyDelimiterOption => options.delimiter = Some(parse_option_value(option)),
            Rule::CopyNullOption => options.null_string = Some(parse_option_value(option)),
            Rule::CopyHeaderOption => {
                options.header = option
                    .into_inner()
                    .next()
                    .map(|value| value.as_str().eq_ignore_ascii_case("true"))
                    .unwrap_or(true);
            }
            Rule::CopySessionFlushRowsOption => {
                options.session_flush_rows =
                    Some(parse_usize_option(option, "session_flush_rows")?);
            }
            Rule::CopyDestinationFlushRowsOption => {
                options.destination_flush_rows =
                    Some(parse_usize_option(option, "destination_flush_rows")?);
            }
            Rule::CopySessionFlushBytesOption => {
                options.session_flush_bytes =
                    Some(parse_usize_option(option, "session_flush_bytes")?);
            }
            Rule::CopyDestinationFlushBytesOption => {
                options.destination_flush_bytes =
                    Some(parse_usize_option(option, "destination_flush_bytes")?);
            }
            Rule::CopyRowBytesOption => {
                options.row_bytes = Some(parse_usize_option(option, "row_bytes")?);
            }
            _ => {}
        }
    }

    Ok(())
}

fn parse_usize_option(option: Pair<'_, Rule>, name: &str) -> Result<usize, SbroadError> {
    let raw = option
        .into_inner()
        .next()
        .expect("COPY unsigned option must have a value")
        .as_str();
    let value = raw.parse::<usize>().map_err(|e| {
        SbroadError::Invalid(
            Entity::Query,
            Some(format_smolstr!("invalid COPY {name} {raw}: {e}")),
        )
    })?;
    if value == 0 {
        return Err(SbroadError::Invalid(
            Entity::Query,
            Some(format_smolstr!("COPY {name} must be greater than zero")),
        ));
    }
    Ok(value)
}

fn parse_copy_format(raw: &str) -> Result<CopyFormat, SbroadError> {
    match raw.to_ascii_lowercase().as_str() {
        "text" => Ok(CopyFormat::Text),
        "csv" => Ok(CopyFormat::Csv),
        "binary" => Ok(CopyFormat::Binary),
        unsupported => Err(SbroadError::Invalid(
            Entity::Query,
            Some(format_smolstr!("unsupported COPY format: {unsupported}")),
        )),
    }
}

fn parse_option_value(option: Pair<'_, Rule>) -> String {
    let value = option
        .into_inner()
        .next()
        .expect("COPY option must have a value");

    match value.as_rule() {
        Rule::Identifier => value.as_str().to_string(),
        Rule::SingleQuotedString => unquote_single_quoted(value.as_str()),
        _ => unreachable!("unexpected COPY option value rule"),
    }
}

fn unquote_single_quoted(raw: &str) -> String {
    raw[1..raw.len() - 1].replace("''", "'")
}

fn parse_table_name(raw: &str) -> Result<(Option<SmolStr>, SmolStr), SbroadError> {
    Ok(match split_top_level_dot(raw) {
        Some(dot_idx) => (
            Some(normalize_name_from_sql(&raw[..dot_idx])),
            normalize_name_from_sql(&raw[dot_idx + 1..]),
        ),
        None => (None, normalize_name_from_sql(raw)),
    })
}

fn split_top_level_dot(name: &str) -> Option<usize> {
    let bytes = name.as_bytes();
    let mut idx = 0usize;
    let mut in_quotes = false;

    while idx < bytes.len() {
        match bytes[idx] {
            b'"' => {
                if in_quotes && bytes.get(idx + 1) == Some(&b'"') {
                    idx += 2;
                    continue;
                }
                in_quotes = !in_quotes;
            }
            b'.' if !in_quotes => return Some(idx),
            _ => {}
        }
        idx += 1;
    }

    None
}

#[cfg(test)]
mod tests {
    use super::{parse_command, ParsedCommand};
    use crate::ir::operator::ConflictStrategy;
    use crate::{CopyFormat, CopyFrom, CopyOptions, CopyStatement, CopyTableTarget};

    fn parse_copy(query: &str) -> CopyStatement {
        match parse_command(query).expect("parse command") {
            ParsedCommand::Copy(copy) => copy,
            ParsedCommand::Sql => panic!("expected COPY command"),
        }
    }

    #[test]
    fn parses_copy_from_stdin() {
        let parsed = parse_copy(
            r#"COPY "t" ("id", "value") FROM STDIN WITH (DELIMITER '|', NULL 'nil', HEADER true)"#,
        );

        assert_eq!(
            parsed,
            CopyStatement::From(CopyFrom {
                table: CopyTableTarget {
                    schema_name: None,
                    table_name: "t".into(),
                    columns: vec!["id".into(), "value".into()],
                },
                options: CopyOptions {
                    format: CopyFormat::Text,
                    delimiter: Some("|".into()),
                    null_string: Some("nil".into()),
                    header: true,
                    conflict_strategy: ConflictStrategy::DoFail,
                    session_flush_rows: None,
                    destination_flush_rows: None,
                    session_flush_bytes: None,
                    destination_flush_bytes: None,
                    row_bytes: None,
                },
            })
        );
    }

    #[test]
    fn parses_copy_flush_threshold_options() {
        let parsed = parse_copy(
            r#"COPY "t" FROM STDIN WITH (SESSION_FLUSH_ROWS = 3, DESTINATION_FLUSH_ROWS = 4, SESSION_FLUSH_BYTES = 4096, DESTINATION_FLUSH_BYTES = 8192, ROW_BYTES = 16384)"#,
        );

        assert_eq!(
            parsed,
            CopyStatement::From(CopyFrom {
                table: CopyTableTarget {
                    schema_name: None,
                    table_name: "t".into(),
                    columns: vec![],
                },
                options: CopyOptions {
                    session_flush_rows: Some(3),
                    destination_flush_rows: Some(4),
                    session_flush_bytes: Some(4096),
                    destination_flush_bytes: Some(8192),
                    row_bytes: Some(16384),
                    ..CopyOptions::default()
                },
            })
        );
    }

    #[test]
    fn rejects_zero_copy_flush_option() {
        let error = parse_command(r#"COPY "t" FROM STDIN WITH (SESSION_FLUSH_BYTES = 0)"#)
            .expect_err("zero COPY flush option should be rejected");

        assert_eq!(
            error.to_string(),
            "invalid query: COPY session_flush_bytes must be greater than zero"
        );
    }

    #[test]
    fn rejects_zero_copy_row_bytes_option() {
        let error = parse_command(r#"COPY "t" FROM STDIN WITH (ROW_BYTES = 0)"#)
            .expect_err("zero COPY row_bytes option should be rejected");

        assert_eq!(
            error.to_string(),
            "invalid query: COPY row_bytes must be greater than zero"
        );
    }

    #[test]
    fn parses_copy_conflict_policy() {
        let parsed = parse_copy(
            r#"COPY "t" FROM STDIN WITH (SESSION_FLUSH_ROWS = 3) ON CONFLICT DO REPLACE"#,
        );

        assert_eq!(
            parsed,
            CopyStatement::From(CopyFrom {
                table: CopyTableTarget {
                    schema_name: None,
                    table_name: "t".into(),
                    columns: vec![],
                },
                options: CopyOptions {
                    session_flush_rows: Some(3),
                    conflict_strategy: ConflictStrategy::DoReplace,
                    ..CopyOptions::default()
                },
            })
        );
    }

    #[test]
    fn rejects_copy_to_stdout() {
        assert!(parse_command(r#"COPY "t" TO STDOUT"#).is_err());
    }

    #[test]
    fn rejects_copy_legacy_option_syntax() {
        assert!(parse_command(r#"COPY "t" FROM STDIN DELIMITER AS '|'"#).is_err());
        assert!(parse_command(r#"COPY "t" FROM STDIN NULL AS 'nil'"#).is_err());
    }

    #[test]
    fn classifies_non_copy_as_sql() {
        assert!(matches!(
            parse_command("SELECT 1").unwrap(),
            ParsedCommand::Sql
        ));
    }

    #[test]
    fn normalizes_schema_qualified_copy_target() {
        let parsed = parse_copy(r#"COPY public."Mixed Table" ("Mixed Column") FROM STDIN"#);

        assert_eq!(
            parsed,
            CopyStatement::From(CopyFrom {
                table: CopyTableTarget {
                    schema_name: Some("public".into()),
                    table_name: "Mixed Table".into(),
                    columns: vec!["Mixed Column".into()],
                },
                options: CopyOptions::default(),
            })
        );
    }
}
