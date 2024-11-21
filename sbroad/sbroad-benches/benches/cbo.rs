use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::Rng;
use sbroad::cbo::histogram::normalization::DEFAULT_HISTOGRAM_BUCKETS_NUMBER;
use sbroad::cbo::histogram::{Histogram, Mcv, McvSet};
use sbroad::cbo::tests::construct_i64_buckets;
use sbroad::cbo::{merge_stats, ColumnStats, TableColumnStatsPair, TableStats};
use tarantool::decimal::Decimal;

/// Helper function to construct random column statistics vector with given
/// number of replicasets.
fn get_numerical_column_stats(
    rows_number: u64,
    replicasets_mock_number: usize,
) -> Vec<ColumnStats<i64>> {
    let mut rng = rand::thread_rng();
    let mcv_count = 10;
    let mut vec_of_stats = Vec::new();
    for _ in 0..replicasets_mock_number {
        let min_value: i64 = rng.gen_range(-10000..0);
        let max_value: i64 = rng.gen_range(1..10000);

        let mut most_common = McvSet::new();
        for _ in 0..mcv_count {
            let value = rng.gen_range(min_value..max_value);
            let absolute_frequency: u64 = rng.gen_range(1000..10000);
            most_common.insert(Mcv::new(
                value,
                Decimal::from(absolute_frequency) / Decimal::from(rows_number),
            ));
        }

        let buckets =
            construct_i64_buckets(DEFAULT_HISTOGRAM_BUCKETS_NUMBER, min_value, max_value).unwrap();

        let null_fraction = rng.gen_range(0.0..0.3);
        let decimal_null_fraction = Decimal::try_from(null_fraction).unwrap();
        let distinct_values_fraction = rng.gen_range(0.0..0.3);
        let decimal_distinct_values_fraction = Decimal::try_from(distinct_values_fraction).unwrap();

        let histogram = Histogram::new(
            most_common,
            buckets,
            decimal_null_fraction,
            decimal_distinct_values_fraction,
        );

        let avg_size = 8;

        let stats_initial = ColumnStats::new(min_value, max_value, avg_size, Some(histogram));
        vec_of_stats.push(stats_initial);
    }
    vec_of_stats
}

fn bench_merge_several_column_statistics(c: &mut Criterion) {
    let replicasets_mock_number = 8;

    let table_rows_number = 10000;
    let table_stats = TableStats::new(table_rows_number);

    let vec_of_stats = get_numerical_column_stats(table_rows_number, replicasets_mock_number);
    let vec_of_pairs: Vec<TableColumnStatsPair<i64>> = vec_of_stats
        .iter()
        .map(|stats| TableColumnStatsPair(table_stats.clone(), stats.clone()))
        .collect();
    c.bench_function("merge_several_column_statistics", |b| {
        b.iter(|| merge_stats(black_box(&vec_of_pairs)));
    });
}

criterion_group!(histogram, bench_merge_several_column_statistics);
criterion_main!(histogram);
