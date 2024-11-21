//! Selection and Join operators selectivity estimation logic.
//! TODO: move the logic of selectivity calculation from prototype.
//! TODO: add docs.
//! TODO: add adequate default values.

use crate::cbo::histogram::Scalar;
use crate::cbo::{ColumnStats, TableColumnPair, TableStats};
use crate::errors::{Entity, SbroadError};
use crate::executor::engine::helpers::to_user;
use crate::executor::engine::{Metadata, Router, Statistics};
use crate::ir::operator::Bool;
use crate::ir::relation::{Column, Type};
use crate::ir::value::double::Double;
use crate::ir::value::Value;
use crate::utils::MutexLike;
use smol_str::{format_smolstr, SmolStr};
use std::any::Any;
use std::fmt::Display;
use std::num::TryFromIntError;
use std::rc::Rc;
use std::str::FromStr;
use tarantool::decimal;
use tarantool::decimal::{Decimal, DecimalToIntError};

#[allow(dead_code)]
const DEFAULT_FILTER_EQ_SELECTIVITY: f64 = 0.0;
#[allow(dead_code)]
const DEFAULT_FILTER_RANGE_SELECTIVITY: f64 = 0.0;
#[allow(dead_code)]
const DEFAULT_CONDITION_EQ_SELECTIVITY: f64 = 0.0;
#[allow(dead_code)]
const DEFAULT_CONDITION_RANGE_SELECTIVITY: f64 = 0.0;

impl<T: Scalar> ColumnStats<T> {
    #[allow(clippy::unused_self)]
    fn filter_selectivity(
        &self,
        table_stats: &Rc<TableStats>,
        constant: &T,
        operator: &Bool,
    ) -> Result<Decimal, SbroadError> {
        match operator {
            Bool::Eq => self.filter_eq_selectivity(constant, table_stats),
            Bool::NotEq => self.filter_neq_selectivity(constant, table_stats),
            Bool::Gt | Bool::GtEq | Bool::Lt | Bool::LtEq => {
                self.filter_range_selectivity(constant, table_stats, operator)
            }
            _ => Err(SbroadError::Invalid(
                Entity::Statistics,
                Some(SmolStr::from(
                    "Unexpected boolean operator met for selectivity estimation: {operator:?}",
                )),
            )),
        }
    }

    #[allow(clippy::unused_self)]
    #[allow(clippy::unnecessary_wraps)]
    fn filter_eq_selectivity(
        &self,
        _constant: &T,
        _table_stats: &Rc<TableStats>,
    ) -> Result<Decimal, SbroadError> {
        Ok(decimal!(1.0))
    }

    #[allow(clippy::unused_self)]
    #[allow(clippy::unnecessary_wraps)]
    fn filter_neq_selectivity(
        &self,
        constant: &T,
        table_stats: &Rc<TableStats>,
    ) -> Result<Decimal, SbroadError> {
        Ok(Decimal::from(1) - self.filter_eq_selectivity(constant, table_stats)?)
    }

    #[allow(clippy::unused_self)]
    #[allow(clippy::unnecessary_wraps)]
    fn filter_range_selectivity(
        &self,
        _constant: &T,
        _table_stats: &Rc<TableStats>,
        _operator: &Bool,
    ) -> Result<Decimal, SbroadError> {
        Ok(decimal!(1.0))
    }

    #[allow(clippy::unused_self)]
    fn condition_selectivity(
        &self,
        self_table_stats: &Rc<TableStats>,
        other: &ColumnStats<T>,
        other_table_stats: &Rc<TableStats>,
        operator: &Bool,
    ) -> Result<Decimal, SbroadError> {
        match operator {
            Bool::Eq => self.condition_eq_selectivity(self_table_stats, other, other_table_stats),
            Bool::NotEq => {
                self.condition_neq_selectivity(self_table_stats, other, other_table_stats)
            }
            Bool::Gt | Bool::GtEq | Bool::Lt | Bool::LtEq => self.condition_range_selectivity(
                self_table_stats,
                other,
                other_table_stats,
                operator,
            ),
            _ => Err(SbroadError::Invalid(
                Entity::Statistics,
                Some(SmolStr::from(
                    "Unexpected boolean operator met for selectivity estimation: {operator:?}",
                )),
            )),
        }
    }

    #[allow(clippy::unused_self)]
    #[allow(clippy::unnecessary_wraps)]
    #[allow(dead_code)]
    fn condition_eq_selectivity(
        &self,
        _self_table_stats: &Rc<TableStats>,
        _other: &ColumnStats<T>,
        _other_table_stats: &Rc<TableStats>,
    ) -> Result<Decimal, SbroadError> {
        Ok(decimal!(1.0))
    }

    #[allow(clippy::unused_self)]
    #[allow(clippy::unnecessary_wraps)]
    fn condition_neq_selectivity(
        &self,
        self_table_stats: &Rc<TableStats>,
        other: &ColumnStats<T>,
        other_table_stats: &Rc<TableStats>,
    ) -> Result<Decimal, SbroadError> {
        Ok(Decimal::from(1)
            - self.condition_eq_selectivity(self_table_stats, other, other_table_stats)?)
    }

    #[allow(clippy::unused_self)]
    #[allow(clippy::unnecessary_wraps)]
    #[allow(dead_code)]
    fn condition_range_selectivity(
        &self,
        _self_table_stats: &Rc<TableStats>,
        _other: &ColumnStats<T>,
        _other_table_stats: &Rc<TableStats>,
        _operator: &Bool,
    ) -> Result<Decimal, SbroadError> {
        Ok(decimal!(1.0))
    }
}

/// Helper function to downcast boxed stats to given type.
fn downcast_column_stats<'stats, T: Scalar>(
    column_stats: &'stats Rc<Box<dyn Any>>,
    column: &Column,
) -> Result<&'stats ColumnStats<T>, SbroadError> {
    let column_name = &column.name;
    let column_type = &column.r#type;

    if let Some(column_stats) = column_stats.downcast_ref::<ColumnStats<T>>() {
        Ok(column_stats)
    } else {
        Err(SbroadError::Invalid(
            Entity::Statistics,
            Some(format_smolstr!(
                "Unable to downcast {column_name} column statistics to {column_type:?} type"
            )),
        ))
    }
}

/// Helper function to cast `value` to `Decimal`.
///
/// # Errors
/// - Unable to cast.
pub fn decimal_from_str(value: &dyn Display) -> Result<Decimal, SbroadError> {
    if let Ok(decimal) = Decimal::from_str(&format!("{value}")) {
        Ok(decimal)
    } else {
        Err(SbroadError::Invalid(
            Entity::Value,
            Some(format_smolstr!("Unable to cast {value} to decimal")),
        ))
    }
}

/// Helper function to cast `value` to `Double` through string formatting.
///
/// # Errors
/// - Unable to cast.
pub fn double_from_str(value: &dyn Display) -> Result<Double, SbroadError> {
    if let Ok(double) = Double::from_str(&format!("{value}")) {
        Ok(double)
    } else {
        Err(SbroadError::Invalid(
            Entity::Value,
            Some(format_smolstr!("Unable to cast {value} to double")),
        ))
    }
}

impl From<DecimalToIntError> for SbroadError {
    fn from(_: DecimalToIntError) -> Self {
        Self::Invalid(
            Entity::Value,
            Some(SmolStr::from("Unable to convert decimal to int")),
        )
    }
}

impl From<TryFromIntError> for SbroadError {
    fn from(_: TryFromIntError) -> Self {
        Self::Invalid(
            Entity::Value,
            Some(SmolStr::from("Unable to convert int into wanted type")),
        )
    }
}

/// Generic function for `WHERE` clause selectivity estimation.
///
/// # Errors
/// - Unable to get statistics.
/// - Unable to get metadata.
/// - Types mismatch.
/// - Selectivity calculation error.
///
/// # Panics
/// - Unimplemented logic.
#[allow(clippy::module_name_repetitions)]
#[allow(clippy::too_many_lines)]
pub fn calculate_filter_selectivity(
    statistics: &(impl Statistics + Router),
    table_column_pair: &TableColumnPair,
    constant: &Value,
    operator: &Bool,
) -> Result<Decimal, SbroadError> {
    let TableColumnPair(table_name, colum_index) = table_column_pair;
    let table_stats = statistics.get_table_stats(table_name)?;
    let column_stats = statistics.get_column_stats(table_column_pair)?;

    let (Some(table_stats), Some(column_stats)) = (table_stats, column_stats) else {
        return decimal_from_str(&DEFAULT_FILTER_EQ_SELECTIVITY);
    };

    let table = statistics.metadata().lock().table(table_name.as_str())?;
    let column = table.columns.get(*colum_index).ok_or_else(|| {
        SbroadError::Invalid(
            Entity::Statistics,
            Some(format_smolstr!(
                "Column with name {} is not found in metadata",
                to_user(table_name)
            )),
        )
    })?;
    let column_type = &column.r#type;

    let types_mismatch_error = Err(SbroadError::Invalid(
        Entity::Statistics,
        Some(format_smolstr!(
            "Column type {column_type:?} mismatches with constant type {constant:?}"
        )),
    ));

    match column_type {
        Type::Boolean => match constant {
            Value::Boolean(b) => {
                let downcasted_column_stats = downcast_column_stats::<bool>(&column_stats, column)?;
                downcasted_column_stats.filter_selectivity(&table_stats, b, operator)
            }
            _ => types_mismatch_error,
        },
        Type::Decimal => {
            let downcasted_column_stats = downcast_column_stats::<Decimal>(&column_stats, column)?;
            let casted_constant = match constant {
                Value::Decimal(d) => *d,
                Value::Unsigned(u) => Decimal::from(*u),
                Value::Integer(i) => Decimal::from(*i),
                Value::Double(d) => decimal_from_str(d)?,
                _ => return types_mismatch_error,
            };
            downcasted_column_stats.filter_selectivity(&table_stats, &casted_constant, operator)
        }
        Type::Unsigned => {
            let downcasted_column_stats = downcast_column_stats::<u64>(&column_stats, column)?;
            let casted_constant = match constant {
                Value::Decimal(d) => (*d).try_into()?,
                Value::Unsigned(u) => *u,
                Value::Integer(i) => (*i).try_into()?,
                Value::Double(d) => {
                    let decimal: Decimal = decimal_from_str(&d)?;
                    decimal.try_into()?
                }
                _ => return types_mismatch_error,
            };
            downcasted_column_stats.filter_selectivity(&table_stats, &casted_constant, operator)
        }
        Type::Double => {
            let downcasted_column_stats = downcast_column_stats::<Double>(&column_stats, column)?;
            let casted_constant = match constant {
                Value::Decimal(d) => double_from_str(d)?,
                Value::Unsigned(u) => Double::from(*u),
                Value::Integer(i) => Double::from(*i),
                Value::Double(d) => d.clone(),
                _ => return types_mismatch_error,
            };
            downcasted_column_stats.filter_selectivity(&table_stats, &casted_constant, operator)
        }
        Type::Integer => {
            let downcasted_column_stats = downcast_column_stats::<i64>(&column_stats, column)?;
            let casted_constant = match constant {
                Value::Decimal(d) => (*d).try_into()?,
                Value::Unsigned(u) => (*u).try_into()?,
                Value::Integer(i) => *i,
                Value::Double(d) => {
                    let decimal: Decimal = decimal_from_str(&d)?;
                    decimal.try_into()?
                }
                _ => return types_mismatch_error,
            };
            downcasted_column_stats.filter_selectivity(&table_stats, &casted_constant, operator)
        }
        Type::String => match constant {
            Value::String(s) => {
                let downcasted_column_stats =
                    downcast_column_stats::<String>(&column_stats, column)?;
                downcasted_column_stats.filter_selectivity(&table_stats, s, operator)
            }
            _ => types_mismatch_error,
        },
        Type::Number => {
            todo!("Don't know what to do here")
        }
        Type::Scalar => {
            todo!("Don't know what to do here")
        }
        Type::Array | Type::Any | Type::Map | Type::Datetime => Err(SbroadError::Invalid(
            Entity::Statistics,
            Some(SmolStr::from(
                "Unable to calculate selectivity for array type column",
            )),
        )),
        Type::Uuid => {
            todo!("Don't know what to do here")
        }
    }
}

/// Generic function for `ON` clause selectivity estimation.
///
/// # Errors
/// - Unable to get stats.
/// - Column types mismatch.
#[allow(clippy::module_name_repetitions)]
pub fn calculate_condition_selectivity(
    statistics: &(impl Statistics + Router),
    left_table_column_pair: &TableColumnPair,
    right_table_column_pair: &TableColumnPair,
    operator: &Bool,
) -> Result<Decimal, SbroadError> {
    let TableColumnPair(left_table_name, left_colum_index) = left_table_column_pair;
    let left_table_stats = statistics.get_table_stats(left_table_name)?;
    let left_column_stats = statistics.get_column_stats(left_table_column_pair)?;
    let (Some(left_table_stats), Some(left_column_stats)) = (left_table_stats, left_column_stats)
    else {
        return decimal_from_str(&DEFAULT_FILTER_EQ_SELECTIVITY);
    };
    let left_table = statistics
        .metadata()
        .lock()
        .table(left_table_name.as_str())?;
    let left_column = left_table.columns.get(*left_colum_index).ok_or_else(|| {
        SbroadError::Invalid(
            Entity::Statistics,
            Some(format_smolstr!(
                "Column with name {} is not found in metadata",
                to_user(left_table_name)
            )),
        )
    })?;
    let left_column_type = &left_column.r#type;

    let TableColumnPair(right_table_name, right_colum_index) = right_table_column_pair;
    let right_table_stats = statistics.get_table_stats(right_table_name)?;
    let right_column_stats = statistics.get_column_stats(right_table_column_pair)?;
    let (Some(right_table_stats), Some(right_column_stats)) =
        (right_table_stats, right_column_stats)
    else {
        return decimal_from_str(&DEFAULT_FILTER_EQ_SELECTIVITY);
    };
    let right_table = statistics
        .metadata()
        .lock()
        .table(left_table_name.as_str())?;
    let right_column = right_table.columns.get(*right_colum_index).ok_or_else(|| {
        SbroadError::Invalid(
            Entity::Statistics,
            Some(format_smolstr!(
                "Column with name {} is not found in metadata",
                to_user(right_table_name),
            )),
        )
    })?;
    let right_column_type = &right_column.r#type;

    let types_mismatch_error = Err(SbroadError::Invalid(
        Entity::Statistics,
        Some(format_smolstr!(
            "Column types {left_column_type:?} and {right_column_type:?} doesn't correspond."
        )),
    ));

    match (left_column_type, right_column_type) {
        (Type::Boolean, Type::Boolean) => {
            let left_downcasted_stats =
                downcast_column_stats::<bool>(&left_column_stats, left_column)?;
            let right_downcasted_stats =
                downcast_column_stats::<bool>(&right_column_stats, right_column)?;
            left_downcasted_stats.condition_selectivity(
                &left_table_stats,
                right_downcasted_stats,
                &right_table_stats,
                operator,
            )
        }
        _ => {
            // TODO: Fill remaining pairs logic, throw error otherwise (or implement logic of column stats cast).
            types_mismatch_error
        }
    }
}

#[cfg(test)]
mod tests;
