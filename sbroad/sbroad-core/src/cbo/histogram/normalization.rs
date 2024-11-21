//! Logic of histogram buckets normalization during the process of statistics merge.

use crate::cbo::histogram::{Bucket, BucketType, HistogramBuckets, Scalar};
use crate::errors::{Entity, SbroadError};
use itertools::enumerate;
use smol_str::{format_smolstr, SmolStr};
use std::cmp::Ordering;
use tarantool::decimal;
use tarantool::decimal::Decimal;

/// Helper structure that represents pair of the Compressed histogram boundary and
/// frequency of the bucket this boundary was taken from.
#[derive(Debug, Clone)]
pub struct BoundaryWithFrequency<T: Scalar> {
    boundary: T,
    frequency: u64,
}

impl<T: Scalar> BoundaryWithFrequency<T> {
    pub(crate) fn new(boundary: T, frequency: u64) -> Self {
        BoundaryWithFrequency {
            boundary,
            frequency,
        }
    }
}

impl<T: Scalar> PartialEq for BoundaryWithFrequency<T> {
    fn eq(&self, other: &Self) -> bool {
        self.boundary.eq(&other.boundary)
    }
}
impl<T: Scalar> Eq for BoundaryWithFrequency<T> {}
impl<T: Scalar> PartialOrd for BoundaryWithFrequency<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<T: Scalar> Ord for BoundaryWithFrequency<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.boundary.cmp(&other.boundary)
    }
}

/// Default value for number of buckets in Compressed histogram (that we store on storages).
/// Value 100 might be taken from `PostgreSQL` database (as soon as it's planned to use their algorithm of
/// statistics construction), but it's taken from `CockroachDB` (as 200) in order to mitigate
/// the influence of `EXPECTED_MERGING_ERROR` resulting from approximate merging algorithm:
/// 1. Less the error, less the number of buckets in resulting merged normalized histogram;
/// 2. Less the number of buckets, less the accuracy of selectivity estimation.
///
/// Example of why does number of buckets matters:
/// Let's assume we have statistics that is numerical data in [0 ... 100] interval -- 100 values
/// totally (suppose for the only column `a` of table `T`) represented with two histograms:
/// 1. Containing one bucket with boundaries [0, 100].
/// 2. Containing two buckets with boundaries [0, 20, 100].
///
/// In case we have query `select * from T where a < 10` it will get selectivity (see logic of
/// selectivity estimation on histogram buckets) equal to:
/// 1. `(10 / 100) / 1 = 0.1`
/// 2. `(10 / 20) / 2 = 0.25`
///
/// In case the second histogram is a real histogram that we've gathered on several storages and the
/// first one resulted from histograms merge algorithm we see the decrease of accuracy. That's why
/// we don't want `get_expected_number_of_buckets` calculation to return small amount of buckets and
/// try to choose both constants wisely.
pub const DEFAULT_HISTOGRAM_BUCKETS_NUMBER: usize = 200;
/// Value of expected error resulting from approximate merging algorithm.
/// See it's usage and explained calculations in `merge_buckets` function.
pub const EXPECTED_MERGING_ERROR: f64 = 0.1;

/// Helper function to calculate expected number of buckets in the resulting merged buckets
/// sequence based on given `DEFAULT_HISTOGRAM_BUCKETS_NUMBER` and `EXPECTED_MERGING_ERROR` values.
/// See details in `merge_buckets` function.
///
/// # Errors
/// - Decimal conversion error.
pub fn get_expected_number_of_buckets() -> Result<u64, SbroadError> {
    let Ok(decimal_error) = Decimal::try_from(EXPECTED_MERGING_ERROR) else {
        return Err(SbroadError::Invalid(
            Entity::Value,
            Some(format_smolstr!(
                "Unable to convert {EXPECTED_MERGING_ERROR} to decimal"
            )),
        ));
    };
    let number_of_buckets =
        (Decimal::from(DEFAULT_HISTOGRAM_BUCKETS_NUMBER) * (decimal_error / decimal!(2.0))).floor();
    if let Some(number_ok_buckets) = number_of_buckets.to_u64() {
        Ok(number_ok_buckets)
    } else {
        Err(SbroadError::Invalid(
            Entity::Value,
            Some(format_smolstr!(
                "Unable to convert {number_of_buckets} to u64"
            )),
        ))
    }
}

/// Helper function to calculate max error of buckets frequency.
///
/// # Errors
/// - Decimal conversion error.
pub fn get_max_buckets_frequency_error(total_buckets_rows_number: u64) -> Result<u64, SbroadError> {
    let error = (decimal!(2.0) / Decimal::from(DEFAULT_HISTOGRAM_BUCKETS_NUMBER)
        * Decimal::from(total_buckets_rows_number))
    .floor();
    if let Some(error) = error.to_u64() {
        Ok(error)
    } else {
        Err(SbroadError::Invalid(
            Entity::Value,
            Some(format_smolstr!("Unable to convert {error} to u64")),
        ))
    }
}

/// Helper struct representing bucket with its accumulated frequency.
/// Accumulated frequency = frequency of all buckets that precede this one + it's own frequency.
#[derive(Debug)]
struct BucketWithAccumulatedFrequency<T: Scalar> {
    bucket: Bucket<T>,
    accumulated_frequency: u64,
}

impl<T: Scalar> BucketWithAccumulatedFrequency<T> {
    fn get_from_boundary(&self) -> Result<T, SbroadError> {
        self.bucket.get_from_boundary()
    }
}

/// Helper struct representing raw histogram bucket boundaries as vector
/// of `BucketWithAccumulatedFrequency`.
#[derive(Debug)]
struct BucketsWithAccumulatedFrequencies<T: Scalar> {
    vec: Vec<BucketWithAccumulatedFrequency<T>>,
}

impl<T: Scalar> BucketsWithAccumulatedFrequencies<T> {
    pub fn new() -> BucketsWithAccumulatedFrequencies<T> {
        BucketsWithAccumulatedFrequencies { vec: Vec::new() }
    }

    /// Helper function to transform
    /// vec of boundaries with frequencies
    /// into
    /// vec of buckets with frequencies.
    pub fn from_boundaries(
        boundaries: &[BoundaryWithFrequency<T>],
    ) -> Result<BucketsWithAccumulatedFrequencies<T>, SbroadError> {
        let mut buckets = BucketsWithAccumulatedFrequencies::new();
        if boundaries.len() < 2 {
            return Err(SbroadError::Invalid(
                Entity::Statistics,
                Some(SmolStr::from(
                    "Boundaries list must contain at least 2 values",
                )),
            ));
        }

        let mut prev_boundary_with_freq: Option<(&T, &u64)> = None;
        // Flag, indicating whether first bucket is already created.
        let mut first_added = false;

        let mut accumulated_freq = 0u64;

        for (
            index,
            BoundaryWithFrequency {
                boundary,
                frequency,
            },
        ) in enumerate(boundaries)
        {
            if index == boundaries.len() - 1 && *frequency != 0 {
                return Err(SbroadError::Invalid(
                    Entity::Statistics,
                    Some(SmolStr::from(
                        "Last boundary in histograms boundaries must be equal to 0",
                    )),
                ));
            }

            if let Some((prev_boundary, prev_freq)) = prev_boundary_with_freq {
                let new_bucket = if first_added {
                    Bucket {
                        bucket_type: BucketType::NonFirst,
                        to_boundary: boundary.clone(),
                    }
                } else {
                    first_added = true;
                    Bucket {
                        bucket_type: BucketType::First {
                            from_boundary: prev_boundary.clone(),
                        },
                        to_boundary: boundary.clone(),
                    }
                };

                accumulated_freq += prev_freq;
                buckets.vec.push(BucketWithAccumulatedFrequency {
                    bucket: new_bucket,
                    accumulated_frequency: accumulated_freq,
                });
            }
            prev_boundary_with_freq = Some((boundary, frequency));
        }
        Ok(buckets)
    }

    pub fn first(&self) -> Result<&BucketWithAccumulatedFrequency<T>, SbroadError> {
        self.vec.first().ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Value,
                Some(SmolStr::from("There is no buckets in merged array")),
            )
        })
    }
}

/// Normalization algorithm must traverse buckets from the `merged_buckets_with_freqs` vector
/// from left to right summing them (their frequencies) into new buckets with
/// new frequency equal to (or due to discrepancy a little bit greater than) the
/// `expected_frequency` value.
/// Note, that normalization process main goal is to gather new buckets boundaries that will
/// correspond to equi `expected_frequency`.
fn normalize_buckets<T: Scalar>(
    expected_frequency: u64,
    merged_buckets_with_freqs: &BucketsWithAccumulatedFrequencies<T>,
    acceptable_buckets_frequency_error: u64,
) -> Result<HistogramBuckets<T>, SbroadError> {
    // The `from` value of the new bucket.
    // It's the `from` value of the first bucket in a sequence we're going to merge.
    let mut current_from = merged_buckets_with_freqs.first()?.get_from_boundary()?;

    // `to` boundary of previously seen bucket.
    let mut last_to = merged_buckets_with_freqs
        .first()?
        .bucket
        .to_boundary
        .clone();

    // Flag, indicating whether first bucket is already created.
    let mut first_added = false;
    // Resulted vec of boundaries.
    let mut boundaries = Vec::new();
    // Number of buckets already created during the process of normalization.
    let mut added_buckets_number = 0u64;
    // Number of initial merged buckets.
    let unnormalized_buckets_number = merged_buckets_with_freqs.vec.len();
    // Accumulated selectivity of bucket which `to` boundary was added last.
    // We need it for error calculation.
    let mut last_accumulated_frequency = 0u64;
    let mut previously_accumulated_frequency = 0u64;

    for (index, bucket_with_freq) in enumerate(&merged_buckets_with_freqs.vec) {
        let accumulated_frequency = bucket_with_freq.accumulated_frequency;
        // Current bound the accumulated frequency have to overflow in order to merge previously
        // traversed buckets.
        let current_bound = expected_frequency * (added_buckets_number + 1);

        // In case we're dealing with last bucket we have no choice but to merge previously
        // traversed buckets.
        if index == unnormalized_buckets_number - 1 {
            last_to = bucket_with_freq.bucket.to_boundary.clone();
            previously_accumulated_frequency = accumulated_frequency;
        } else if accumulated_frequency <= current_bound {
            last_to = bucket_with_freq.bucket.to_boundary.clone();
            previously_accumulated_frequency = accumulated_frequency;
            continue;
        }
        let current_buckets_sequence_total_frequency =
            previously_accumulated_frequency - last_accumulated_frequency;
        if (current_buckets_sequence_total_frequency).abs_diff(expected_frequency)
            > acceptable_buckets_frequency_error
        {
            return Err(SbroadError::Invalid(
                Entity::Statistics,
                Some(SmolStr::from("Normalization algorithm resulted in a bucket with unacceptable frequency error"))
            ));
        }
        if first_added {
            boundaries.push(last_to.clone());
        } else {
            boundaries.push(current_from.clone());
            boundaries.push(last_to.clone());
            first_added = true;
        }

        current_from = last_to.clone();
        added_buckets_number += 1;
        last_to = bucket_with_freq.bucket.to_boundary.clone();
        last_accumulated_frequency = accumulated_frequency;
        previously_accumulated_frequency = accumulated_frequency;
    }

    HistogramBuckets::try_from(&mut boundaries)
}

/// Helper struct representing pair of (buckets frequency, `HistogramBuckets`).
pub struct BucketsFrequencyPair<'buckets, T: Scalar>(pub u64, pub &'buckets HistogramBuckets<T>);

/// Function for merging histogram buckets from several starages.
///
/// The logic used in function (and its subroutines) is taken from the
/// [article](https://arxiv.org/pdf/1606.05633.pdf). The main idea is that we consider all the
/// values stored in the bucket to be placed in its `from` boundary.
///
/// **FUTURE WORK**: Such an approximate algorithm may not seem to be suitable for accurate plan
/// cost calculation. Consider other variants:
/// * Apply the logic of samples aggregation from
///   [`CockroachDB`](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20170908_sql_optimizer_statistics.md).
///   Instead of merging histograms they aggregate samples (resulted from reservoir sampling) on
///   router at first and then build a histogram from merged samples. Also note the question of
///   optimized sampling (with[blog link](https://ballsandbins.wordpress.com/2014/04/13/distributedparallel-reservoir-sampling/))
///   as an example.
/// * Apache Druid deprecated histograms are aggregated using the algorithm from
///   [article](https://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf) (see section 2.1).
///
/// # Errors
/// - Unable to normalize merged buckets sequence.
pub fn merge_buckets<T: Scalar>(
    vec_of_infos: &Vec<BucketsFrequencyPair<T>>,
) -> Result<HistogramBuckets<T>, SbroadError> {
    // ------------Algorithm of histograms merge starts-----------------------------------------
    // 1. Build a sorted vector of combined boundaries.
    let total_buckets_rows_number: u64 = vec_of_infos
        .iter()
        .map(|BucketsFrequencyPair(frequency, buckets)| (buckets.len() as u64) * frequency)
        .sum();

    let merged_boundaries_capacity: usize = vec_of_infos
        .iter()
        .map(|BucketsFrequencyPair(_, buckets)| buckets.len())
        .sum();
    let mut merged_boundaries: Vec<BoundaryWithFrequency<T>> =
        Vec::with_capacity(merged_boundaries_capacity);
    for BucketsFrequencyPair(frequency, buckets) in vec_of_infos {
        let mut boundaries = buckets.gather_boundaries_with_frequency(*frequency);
        merged_boundaries.append(&mut boundaries);
    }
    merged_boundaries.sort();

    // 2. Convert boundaries into vector of `BucketsWithAccumulatedFrequencies`'s.
    let merged_buckets = BucketsWithAccumulatedFrequencies::from_boundaries(&merged_boundaries)?;

    // 3. Normalize vector of `BucketsWithAccumulatedFrequencies`'s into
    //    histogram with `expected_number_of_buckets` buckets number and
    //    `new_buckets_frequency` frequency for each bucket.
    //    For given:
    //    * N = total number of rows in merging histograms
    //    * b = expected number of buckets in transformed histogram,
    //    * T = number of buckets in single merging histogram
    //    by the formula given in the article the value of `expected_error` (error of resulting
    //    buckets size) is bounded by the following inequality:
    //    `(2b / T) * (N / b) <= expected_error * (N / b)` -> `b <= (expected_error * T) / 2`
    //    that gives us the formula for applicable value of b:
    //    `b = floor(T * (expected_error / 2))`.
    let expected_number_of_buckets = get_expected_number_of_buckets()?;
    let new_buckets_frequency = total_buckets_rows_number / expected_number_of_buckets;
    let expected_buckets_frequency_error =
        get_max_buckets_frequency_error(total_buckets_rows_number)?;

    let normalized_buckets = normalize_buckets(
        new_buckets_frequency,
        &merged_buckets,
        expected_buckets_frequency_error,
    )?;
    if (normalized_buckets.len() as u64) != expected_number_of_buckets {
        return Err(SbroadError::Invalid(
            Entity::Statistics,
            Some(SmolStr::from(
                "Normalization algorithm returned unexpected number of buckets",
            )),
        ));
    }
    Ok(normalized_buckets)
}
