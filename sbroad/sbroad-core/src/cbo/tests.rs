use smol_str::SmolStr;

use crate::cbo::histogram::HistogramBuckets;
use crate::cbo::histogram::{Histogram, Scalar};
use crate::cbo::{ColumnStats, TableColumnPair, TableStats};
use crate::errors::{Entity, SbroadError};
use crate::executor::engine::mock::RouterRuntimeMock;
use crate::executor::engine::Statistics;
use std::any::Any;
use std::rc::Rc;

/// Helper function to construct `Buckets` struct (of `i64` type) that is similar to one gathered by
/// the real algorithm of statistics creation on storages.
///
/// # Errors
/// - Unable to construct `HistogramBucket` from gotten boundaries list.
pub fn construct_i64_buckets(
    buckets_count: usize,
    value_min: i64,
    value_max: i64,
) -> Result<HistogramBuckets<i64>, SbroadError> {
    let casted_buckets_count = i64::try_from(buckets_count).map_err(|_| {
        SbroadError::Invalid(
            Entity::Value,
            Some(SmolStr::from("Unable to cast buckets_count to i64")),
        )
    })?;
    let bucket_len = (value_max - value_min) / casted_buckets_count;
    let mut boundaries = vec![value_min];
    for i in 0..buckets_count {
        let next_index = i64::try_from(i + 1).map_err(|_| {
            SbroadError::Invalid(
                Entity::Value,
                Some(SmolStr::from("Unable to cast next index to i64")),
            )
        })?;
        let adding = next_index * bucket_len;
        boundaries.push(value_min + adding);
    }
    let buckets = HistogramBuckets::try_from(&mut boundaries)?;
    Ok(buckets)
}

/// `ColumnStats` structure with unwrapped fields
#[allow(dead_code)]
pub struct ColumnStatsUnwrapped<T: Scalar> {
    pub(crate) min_value: T,
    pub(crate) max_value: T,
    pub(crate) avg_size: u64,
    pub(crate) histogram: Histogram<T>,
}

impl<T: Scalar> ColumnStatsUnwrapped<T> {
    /// # Panics
    /// - No histogram in statistics.
    pub fn from_column_stats(column_stats: &ColumnStats<T>) -> ColumnStatsUnwrapped<T> {
        ColumnStatsUnwrapped {
            min_value: column_stats.min_value.clone(),
            max_value: column_stats.max_value.clone(),
            avg_size: column_stats.avg_size,
            histogram: column_stats.histogram.clone().unwrap(),
        }
    }
}

/// Helper function to retrieve upcasted mock table and column stats.
/// Used for tests purposes.
///
/// # Panics
/// - Requested stats are not found.
#[allow(dead_code)]
pub fn get_table_column_stats_upcasted<T>(
    coordinator: &RouterRuntimeMock,
    table_name: &str,
    column_index: usize,
) -> (TableStats, Rc<Box<dyn Any>>) {
    let table_stats = (*(coordinator.get_table_stats(table_name).unwrap().unwrap())).clone();

    let column_stats_upcasted = coordinator
        .get_column_stats(&TableColumnPair::new(
            SmolStr::from(table_name),
            column_index,
        ))
        .unwrap()
        .unwrap();

    (table_stats, column_stats_upcasted)
}

/// Helper function to retrieve downcasted mock table and column stats.
/// Used for tests purposes.
///
/// # Panics
/// - Requested stats are not found.
pub fn get_table_column_stats_downcasted<T: Scalar>(
    coordinator: &RouterRuntimeMock,
    table_name: &str,
    column_index: usize,
) -> (TableStats, ColumnStats<T>) {
    let table_stats = (*(coordinator.get_table_stats(table_name).unwrap().unwrap())).clone();

    let column_stats_upcasted = coordinator
        .get_column_stats(&TableColumnPair::new(
            SmolStr::from(table_name),
            column_index,
        ))
        .unwrap()
        .unwrap();
    let column_stats = (*column_stats_upcasted)
        .downcast_ref::<ColumnStats<T>>()
        .unwrap()
        .clone();

    (table_stats, column_stats)
}
