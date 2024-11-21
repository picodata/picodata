use crate::cbo::histogram::normalization::{
    merge_buckets, BucketsFrequencyPair, DEFAULT_HISTOGRAM_BUCKETS_NUMBER,
};
use crate::cbo::tests::construct_i64_buckets;
use crate::cbo::tests::{get_table_column_stats_downcasted, ColumnStatsUnwrapped};
use crate::cbo::{merge_stats, TableColumnStatsPair};
use crate::executor::engine::mock::RouterRuntimeMock;
use tarantool::decimal;
use tarantool::decimal::Decimal;

#[test]
fn stats_merge() {
    let coordinator = RouterRuntimeMock::new();

    let (table_stats, sys_from_stats_wrapped) =
        get_table_column_stats_downcasted::<i64>(&coordinator, &String::from("test_space"), 1);
    let test_space_rows_count = 25000.0;
    let column_stats_unwrapped = ColumnStatsUnwrapped::from_column_stats(&sys_from_stats_wrapped);

    let sys_from_stats_expected_buckets =
        construct_i64_buckets(DEFAULT_HISTOGRAM_BUCKETS_NUMBER, 100, 400).unwrap();
    assert_eq!(
        sys_from_stats_expected_buckets,
        column_stats_unwrapped.histogram.buckets
    );

    let replicasets_mock_number = 7;
    let mut gathered_statistics = Vec::new();
    for _ in 0..replicasets_mock_number {
        gathered_statistics.push(TableColumnStatsPair(
            table_stats.clone(),
            sys_from_stats_wrapped.clone(),
        ))
    }

    let TableColumnStatsPair(merged_table_stats, merged_sys_from_stats) =
        merge_stats(&gathered_statistics).unwrap();
    let merged_column_stats_unwrapped =
        ColumnStatsUnwrapped::from_column_stats(&merged_sys_from_stats);
    let expected_merged_rows_number = (test_space_rows_count as u64) * replicasets_mock_number;
    assert_eq!(expected_merged_rows_number, merged_table_stats.rows_number);
    assert_eq!(
        column_stats_unwrapped.avg_size,
        merged_column_stats_unwrapped.avg_size
    );
    assert_eq!(
        column_stats_unwrapped.min_value,
        merged_column_stats_unwrapped.min_value
    );
    assert_eq!(
        column_stats_unwrapped.max_value,
        merged_column_stats_unwrapped.max_value
    );
    assert_eq!(
        column_stats_unwrapped.histogram.null_fraction,
        merged_column_stats_unwrapped.histogram.null_fraction
    );
    assert_eq!(
        column_stats_unwrapped.histogram.distinct_values_fraction,
        merged_column_stats_unwrapped
            .histogram
            .distinct_values_fraction
    );

    for mcv_old in &column_stats_unwrapped.histogram.most_common_values.inner {
        let value_absolute_frequency = mcv_old.frequency
            * (Decimal::try_from(test_space_rows_count).unwrap()
                * (decimal!(1.0) - decimal!(sys_from_null_fraction)));

        let merged_mcv = merged_column_stats_unwrapped
            .histogram
            .most_common_values
            .get(mcv_old)
            .unwrap();
        assert_eq!(
            merged_mcv.frequency,
            value_absolute_frequency / (decimal!(expected_merged_rows_number))
        )
    }
}

#[test]
fn normalization_same_min_max_same_frequency() {
    let buckets_frequency: u64 = 50;
    let first_buckets =
        construct_i64_buckets(DEFAULT_HISTOGRAM_BUCKETS_NUMBER, -500, 8000).unwrap();
    let second_buckets =
        construct_i64_buckets(DEFAULT_HISTOGRAM_BUCKETS_NUMBER, 99000, 9900099).unwrap();
    let vec_of_infos = vec![
        BucketsFrequencyPair(buckets_frequency, &first_buckets),
        BucketsFrequencyPair(buckets_frequency, &second_buckets),
    ];
    assert!(merge_buckets(&vec_of_infos).is_ok());
}

#[test]
fn normalization_different_min_max_same_frequency() {
    let buckets_frequency = 50;
    let first_buckets =
        construct_i64_buckets(DEFAULT_HISTOGRAM_BUCKETS_NUMBER, -500, 8000).unwrap();
    let second_buckets =
        construct_i64_buckets(DEFAULT_HISTOGRAM_BUCKETS_NUMBER, 99000, 9900099).unwrap();
    let vec_of_infos = vec![
        BucketsFrequencyPair(buckets_frequency, &first_buckets),
        BucketsFrequencyPair(buckets_frequency, &second_buckets),
    ];
    assert!(merge_buckets(&vec_of_infos).is_ok())
}

#[test]
fn normalization_same_min_max_different_frequency() {
    let first_frequency = 50;
    let first_buckets =
        construct_i64_buckets(DEFAULT_HISTOGRAM_BUCKETS_NUMBER, -500, 8000).unwrap();
    let second_frequency = 9999;
    let second_buckets =
        construct_i64_buckets(DEFAULT_HISTOGRAM_BUCKETS_NUMBER, -500, 8000).unwrap();
    let vec_of_infos = vec![
        BucketsFrequencyPair(first_frequency, &first_buckets),
        BucketsFrequencyPair(second_frequency, &second_buckets),
    ];
    assert!(merge_buckets(&vec_of_infos).is_ok())
}

#[test]
fn normalization_different_min_max_different_frequency() {
    let first_frequency = 5;
    let first_buckets =
        construct_i64_buckets(DEFAULT_HISTOGRAM_BUCKETS_NUMBER, -500, 1000).unwrap();
    let second_frequency = 100;
    let second_buckets =
        construct_i64_buckets(DEFAULT_HISTOGRAM_BUCKETS_NUMBER, 500, 5000).unwrap();
    let vec_of_infos = vec![
        BucketsFrequencyPair(first_frequency, &first_buckets),
        BucketsFrequencyPair(second_frequency, &second_buckets),
    ];
    assert!(merge_buckets(&vec_of_infos).is_ok())
}

#[test]
fn normalization_different_min_max_different_frequency_inexact_buckets() {
    let first_frequency = 50;
    let first_buckets =
        construct_i64_buckets(DEFAULT_HISTOGRAM_BUCKETS_NUMBER, -5432, 6789).unwrap();
    let second_frequency = 9999;
    let second_buckets =
        construct_i64_buckets(DEFAULT_HISTOGRAM_BUCKETS_NUMBER, 5555, 56789).unwrap();
    let vec_of_infos = vec![
        BucketsFrequencyPair(first_frequency, &first_buckets),
        BucketsFrequencyPair(second_frequency, &second_buckets),
    ];
    assert!(merge_buckets(&vec_of_infos).is_ok())
}
