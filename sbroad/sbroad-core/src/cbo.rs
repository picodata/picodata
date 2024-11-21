//! Cost Based Optimizer (CBO).
//!
//! Module used to optimize IR tree using statistics and plan cost calculation algorithms.
//!
//! As soon as the biggest part of the logic is taken from
//! `PostgreSQL` implementation, you may see `PostgreSQL lines` comments
//! in some places with indication of function names and corresponding lines of code.
//! `PostgreSQL` version: `REL_15_2`.

use smol_str::SmolStr;

use crate::cbo::histogram::{merge_histograms, Histogram, HistogramRowsNumberPair, Scalar};
use crate::errors::{Entity, SbroadError};
use std::fmt::Debug;
use std::hash::Hash;

/// Struct representing statistics for the whole table.
#[derive(Debug, Clone, PartialEq)]
pub struct TableStats {
    /// Number of rows in the table.
    rows_number: u64,
}

impl TableStats {
    #[must_use]
    pub fn new(rows_number: u64) -> Self {
        Self { rows_number }
    }
}

/// Struct representing statistics for concrete column.
///
/// The reason some values are stored in that structure and not in `Histogram`: some values may be
/// useful for selectivity estimation when histograms are on the stage of rebuilding and
/// actualization. `min_value`, `max_value` and `null_fraction` may be stored without histogram
/// creation.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStats<T: Scalar> {
    /// Column MIN value.
    min_value: T,
    /// Column MAX value.
    max_value: T,
    /// Average size of column row in bytes.
    avg_size: u64,
    /// Column Compressed histogram.
    histogram: Option<Histogram<T>>,
}

#[allow(dead_code)]
impl<T: Scalar> ColumnStats<T> {
    #[must_use]
    pub fn new(min_value: T, max_value: T, avg_size: u64, histogram: Option<Histogram<T>>) -> Self {
        Self {
            min_value,
            max_value,
            avg_size,
            histogram,
        }
    }
}

/// Struct that represents column information needed for its size calculation (in bytes).
///
/// These are the only two fields that are passed from bottom to top during recursive
/// relational tree traversal.
/// These two fields change when relational operator or filter are applied to the column.
#[allow(dead_code)]
pub struct ColumnVolumeInfo {
    /// Number of rows.
    rows_number: u64,
    /// Average size of column rows in bytes.
    avg_size: u64,
}

/// Alias for pair of table name and column id in the table.
/// Used as a key for statistics retrieval from system space.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableColumnPair(SmolStr, usize);

#[allow(dead_code)]
impl TableColumnPair {
    pub(crate) fn new(table_name: SmolStr, column_id: usize) -> Self {
        Self(table_name, column_id)
    }
}

/// Helper struct representing pair of (`TableStats`, `ColumnStats` (upcasted)).
/// One such pair is a statistics gathered on one of replicasets.
#[derive(Debug)]
pub struct TableColumnStatsPair<T: Scalar>(pub TableStats, pub ColumnStats<T>);

/// Function for merging statistics from several replicasets.
/// It takes vec of `TableColumnStatsPair` and return single `TableColumnStatsPair`.
///
/// # Errors
/// - Unable to find min/max values.
/// - Unable to get histogram from stats.
/// - Unable to merge histograms.
#[allow(dead_code)]
pub fn merge_stats<T: Scalar>(
    vec_of_stats: &Vec<TableColumnStatsPair<T>>,
) -> Result<TableColumnStatsPair<T>, SbroadError> {
    let merged_rows_number = vec_of_stats
        .iter()
        .map(|TableColumnStatsPair(table_stats, _)| table_stats.rows_number)
        .sum();
    let merged_table_stats = TableStats::new(merged_rows_number);

    let merged_min_value = vec_of_stats
        .iter()
        .map(|TableColumnStatsPair(_, column_stats)| &column_stats.min_value)
        .min()
        .ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Statistics,
                Some(SmolStr::from(
                    "Unable to find min value among column statistics",
                )),
            )
        })?
        .clone();

    let merged_max_value = vec_of_stats
        .iter()
        .map(|TableColumnStatsPair(_, column_stats)| &column_stats.max_value)
        .max()
        .ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Statistics,
                Some(SmolStr::from(
                    "Unable to find min value among column statistics",
                )),
            )
        })?
        .clone();

    let merged_avg_size = {
        let sum_of_total_sized: u64 = vec_of_stats
            .iter()
            .map(|TableColumnStatsPair(table_stats, column_stats)| {
                table_stats.rows_number * column_stats.avg_size
            })
            .sum();
        sum_of_total_sized / merged_rows_number
    };

    let mut vec_of_histograms_info = Vec::with_capacity(vec_of_stats.len());
    for TableColumnStatsPair(table_stats, column_stats) in vec_of_stats {
        if let Some(histogram) = &column_stats.histogram {
            vec_of_histograms_info
                .push(HistogramRowsNumberPair(table_stats.rows_number, histogram));
        }
    }
    let merged_histogram = match vec_of_histograms_info.len() {
        0 => None,
        1 => {
            let TableColumnStatsPair(_, column_stats) = vec_of_stats.first().ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues(SmolStr::from("No histograms to merge"))
            })?;
            if let Some(histogram) = &column_stats.histogram {
                Some(histogram.clone())
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Histogram,
                    Some(SmolStr::from("Columns stats must have non None histogram")),
                ));
            }
        }
        _ => {
            let merged_histogram = merge_histograms(&vec_of_histograms_info)?;
            Some(merged_histogram)
        }
    };

    let merged_column_stats = ColumnStats::new(
        merged_min_value,
        merged_max_value,
        merged_avg_size,
        merged_histogram,
    );

    Ok(TableColumnStatsPair(
        merged_table_stats,
        merged_column_stats,
    ))
}

pub mod helpers;
pub mod histogram;
pub mod selectivity;
#[cfg(feature = "mock")]
pub mod tests;
