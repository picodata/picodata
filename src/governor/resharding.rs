use crate::cas;
use crate::catalog::pico_bucket::BucketRecord;
use crate::catalog::pico_bucket::PicoBucket;
use crate::column_name;
use crate::config::AlterSystemParameters;
use crate::governor::batch::get_next_batch;
use crate::governor::batch::LastStepInfo;
use crate::governor::ActualizeBucketStateVersion;
use crate::governor::AwaitResharding;
use crate::governor::InitializeBucketDistribution;
use crate::governor::Plan;
use crate::governor::SleepDueToBackoff;
use crate::replicaset::Replicaset;
use crate::rpc;
use crate::schema::ADMIN_ID;
use crate::sharding::TierBucketsInfo;
use crate::storage;
use crate::storage::SystemTable;
use crate::tier::Tier;
use crate::tlog;
use crate::topology_cache::TopologyCacheRef;
use crate::traft::op::Dml;
use crate::traft::op::Op;
#[allow(unused_imports)]
use crate::traft::op::PluginRaftOp;
use crate::traft::RaftIndex;
use crate::traft::RaftTerm;
use crate::traft::Result;
use smol_str::SmolStr;
use std::collections::HashMap;
use std::time::Duration;
use tarantool::space::UpdateOps;

////////////////////////////////////////////////////////////////////////////////
// handle_resharding
////////////////////////////////////////////////////////////////////////////////

/// Prepares actions to initialize bucket distribution as well as perform bucket
/// rebalancing in response to corresponding topology change events.
pub fn handle_resharding<'i>(
    last_step_info: &mut LastStepInfo,
    term: RaftTerm,
    applied: RaftIndex,
    topology_ref: &TopologyCacheRef,
    db_config: &AlterSystemParameters,
    rpc_timeout: Duration,
) -> Result<Option<Plan<'i>>> {
    // Resharding finished
    if let Some((tier, cas)) = get_bucket_version_actualizations(applied, topology_ref, db_config)?
    {
        return Ok(Some(ActualizeBucketStateVersion { tier, cas }.into()));
    }

    // Propose automatic resharding state changes
    if let Some(action) = get_resharding_state_changes(applied, topology_ref, db_config)? {
        return Ok(Some(action));
    }

    // Request masters to do resharding among each other
    if let Some(action) = get_resharding_rpc_requests(
        last_step_info,
        term,
        applied,
        topology_ref,
        db_config,
        rpc_timeout,
    )? {
        return Ok(Some(action));
    }

    Ok(None)
}

////////////////////////////////////////////////////////////////////////////////
// get_bucket_version_actualizations
////////////////////////////////////////////////////////////////////////////////

fn get_bucket_version_actualizations(
    applied: RaftIndex,
    topology_ref: &TopologyCacheRef,
    db_config: &AlterSystemParameters,
) -> Result<Option<(SmolStr, cas::Request)>> {
    'next_tier: for tier in topology_ref.all_tiers() {
        if !db_config.experimental_sharding_implementation(&tier.name) {
            // This tier is using the legacy vshard implementation
            continue;
        }

        if tier.current_bucket_state_version == tier.target_bucket_state_version {
            // No resharding on this tier
            continue;
        }

        for replicaset in topology_ref.tier_replicasets(&tier.name) {
            if replicaset.current_bucket_state_version != replicaset.target_bucket_state_version {
                // At least one of the replicasets didn't confirm resharding completion yet
                continue 'next_tier;
            }
        }

        let dml = storage::Tiers::dml_update(
            &[&tier.name],
            UpdateOps::new().into_assign(
                column_name!(Tier, current_bucket_state_version),
                tier.target_bucket_state_version,
            )?,
        );

        let predicate = cas::Predicate::new(applied, []);
        let cas = cas::Request::new(dml, predicate, ADMIN_ID)?;

        return Ok(Some((tier.name.clone(), cas)));
    }

    Ok(None)
}

////////////////////////////////////////////////////////////////////////////////
// get_resharding_state_changes
////////////////////////////////////////////////////////////////////////////////

pub fn get_resharding_state_changes<'i>(
    applied: RaftIndex,
    topology_ref: &TopologyCacheRef,
    db_config: &AlterSystemParameters,
) -> Result<Option<Plan<'i>>> {
    for info in topology_ref.all_buckets_infos() {
        if !db_config.experimental_sharding_implementation(&info.tier_name) {
            // This tier is using the legacy vshard implementation
            continue;
        }

        if info.bucket_count_by_target_replicaset_name.is_empty() {
            // Note: `bucket_count_by_target_replicaset_name` is updated asynchronously
            // in raft_main_loop, so it's possible that we get here before that. We
            // must wait until it's updated before we can procceed
            continue;
        }

        let tier = topology_ref.tier_by_name(&info.tier_name)?;
        if tier.current_bucket_state_version != tier.target_bucket_state_version {
            // Rebalancing is already in progress on this tier
            continue;
        }

        if tier.target_bucket_state_version == 0 {
            // Check if we're ready to make an initial bucket distribution
            if let Some(plan) = make_initial_distribution(applied, tier, info, topology_ref)? {
                crate::error_injection!("BLOCK_INITIAL_BUCKET_DISTRIBUTION" => return Ok(Some(SleepDueToBackoff::error_injection().into())));

                return Ok(Some(plan));
            }

            // Not ready yet, likely because no replicaset is filled up to `replication_factor` yet
            continue;
        }

        // TODO(resharding): check if buckets are balanced correctly according to sharding weights
        // and propose a resharding plan
    }

    Ok(None)
}

////////////////////////////////////////////////////////////////////////////////
// make_initial_distribution
////////////////////////////////////////////////////////////////////////////////

fn make_initial_distribution<'i>(
    applied: RaftIndex,
    tier: &Tier,
    info: &TierBucketsInfo,
    topology_ref: &TopologyCacheRef,
) -> Result<Option<Plan<'i>>> {
    debug_assert_eq!(tier.current_bucket_state_version, 0);
    debug_assert_eq!(tier.target_bucket_state_version, 0);

    let Some(imbalance) = compute_bucket_distribution_imbalance(info, topology_ref)? else {
        // Not ready to make initial distribution, possible if no replicaset is
        // filled to replication_factor yet
        return Ok(None);
    };

    debug_assert_eq!(imbalance.actual_total, 0);
    debug_assert!(
        imbalance.have_extra_buckets.is_empty(),
        "{:?}",
        imbalance.have_extra_buckets
    );

    let dmls = state_changes_for_initial_distribution(
        crate::catalog::pico_bucket::BUCKET_ID_MIN,
        info.total_count,
        &imbalance.need_more_buckets,
        tier,
        topology_ref,
    )?;

    let ranges = vec![
        // Operation should fail if we or someone else has already requested
        // a change to this tier's bucket distribution
        cas::Range::new(PicoBucket::TABLE_ID).eq([&info.tier_name]),
        // Implictitly fail if someone updates _pico_tier or _pico_replicaset
    ];
    let predicate = cas::Predicate::new(applied, ranges);
    let op = Op::single_dml_or_batch(dmls);
    let cas = cas::Request::new(op, predicate, ADMIN_ID)?;

    Ok(Some(
        InitializeBucketDistribution {
            tier: tier.name.clone(),
            cas,
        }
        .into(),
    ))
}

/// Prepare global DML operations to initialize _pico_bucket distribution in
/// `tier` among replicasets in `need_more_buckets` according to their needs.
///
/// `start` is the smallest bucket id to be assigned to one of the replicasets.
/// `end` is the largest bucket id to be assigned to one of the replicasets.
///
/// Note that counts in `need_more_buckets` must add up to `end - start + 1`.
fn state_changes_for_initial_distribution(
    start: u64,
    end: u64,
    need_more_buckets: &[(&SmolStr, u64)],
    tier: &Tier,
    topology_ref: &TopologyCacheRef,
) -> Result<Vec<Dml>> {
    debug_assert!(end >= start, "{end} >= {start}");
    let mut dmls = Vec::new();

    dmls.push(storage::Tiers::dml_update(
        &[&tier.name],
        UpdateOps::new().into_assign(
            column_name!(Tier, target_bucket_state_version),
            tier.target_bucket_state_version + 1,
        )?,
    ));

    let mut curr_start = start;
    for (replicaset_name, needed) in need_more_buckets.iter().copied() {
        let curr_end = curr_start + needed - 1;
        let new_record = BucketRecord::new_active(
            tier.name.clone(),
            curr_start,
            curr_end,
            replicaset_name.clone(),
        );
        dmls.push(PicoBucket::dml_insert(&new_record));

        curr_start = curr_end + 1;

        let replicaset = topology_ref.replicaset_by_name(replicaset_name)?;
        dmls.push(storage::Replicasets::dml_update(
            &[replicaset_name],
            UpdateOps::new().into_assign(
                column_name!(Replicaset, target_bucket_state_version),
                replicaset.target_bucket_state_version + 1,
            )?,
        ));
    }

    debug_assert_eq!(curr_start, end + 1);

    Ok(dmls)
}

////////////////////////////////////////////////////////////////////////////////
// compute_bucket_distribution_imbalance
////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone)]
struct BucketDistributionImbalance<'a> {
    /// A mapping from replicaset name to the number of buckets that it should
    /// transfer to someone else
    have_extra_buckets: HashMap<&'a SmolStr, u64>,

    /// An array of replicaset names with the numbers of buckets that they
    /// should receive from someone else
    need_more_buckets: Vec<(&'a SmolStr, u64)>,

    /// Actual number of buckets currently distributed in the cluster
    actual_total: u64,
}

fn compute_bucket_distribution_imbalance<'t>(
    info: &'t TierBucketsInfo,
    topology_ref: &'t TopologyCacheRef,
) -> Result<Option<BucketDistributionImbalance<'t>>> {
    let mut res = BucketDistributionImbalance::default();
    let mut bucket_count_by_target_replicaset_name_sorted =
        Vec::with_capacity(info.bucket_count_by_target_replicaset_name.len());

    let mut weight_sum = 0.0;
    for (target_replicaset_name, current_count) in &info.bucket_count_by_target_replicaset_name {
        weight_sum += topology_ref
            .replicaset_by_name(target_replicaset_name)?
            .weight;

        bucket_count_by_target_replicaset_name_sorted
            .push((target_replicaset_name, *current_count));
    }

    if weight_sum == 0.0 {
        tlog!(Debug, "all replicasets have 0 weight, no imbalance");
        return Ok(None);
    }

    // TODO: support `rebalancer_disbalance_threshold` analog
    // https://git.picodata.io/core/picodata/-/issues/2939

    #[cfg(debug_assertions)]
    let mut control_total = 0u64;

    let mut weight_so_far = 0.0;
    let mut allocated_so_far = 0u64;

    // Sort the array so that the order of iteration is deterministic
    bucket_count_by_target_replicaset_name_sorted.sort_by(|lhs, rhs| lhs.0.cmp(rhs.0));

    for (target_replicaset_name, current_count) in bucket_count_by_target_replicaset_name_sorted {
        res.actual_total += current_count;

        let replicaset = topology_ref.replicaset_by_name(target_replicaset_name)?;

        weight_so_far += replicaset.weight;
        let next_allocated = (weight_so_far / weight_sum * info.total_count as f64).round() as u64;

        let target_count = next_allocated - allocated_so_far;
        allocated_so_far = next_allocated;

        let needed = target_count as i64 - current_count as i64;
        if needed > 0 {
            res.need_more_buckets
                .push((target_replicaset_name, needed as u64));
        } else if needed < 0 {
            res.have_extra_buckets
                .insert(target_replicaset_name, -needed as u64);
        }

        #[cfg(debug_assertions)]
        {
            control_total += target_count;
        }
    }

    // This is alwyas true, because each `allocated_so_far` is computed based on
    // the running weight sum, and each `target_count` is just the increment of
    // the consequtive `allocated_so_far`.
    debug_assert_eq!(allocated_so_far, info.total_count);
    #[cfg(debug_assertions)]
    debug_assert_eq!(control_total, info.total_count);

    // No imbalance detected
    if res.need_more_buckets.is_empty() && res.have_extra_buckets.is_empty() {
        return Ok(None);
    }

    Ok(Some(res))
}

////////////////////////////////////////////////////////////////////////////////
// get_resharding_rpc_requests
////////////////////////////////////////////////////////////////////////////////

fn get_resharding_rpc_requests<'i>(
    last_step_info: &mut LastStepInfo,
    term: RaftTerm,
    applied: RaftIndex,
    topology_ref: &TopologyCacheRef,
    db_config: &AlterSystemParameters,
    rpc_timeout: Duration,
) -> Result<Option<Plan<'i>>> {
    let mut targets_total = vec![];

    for replicaset in topology_ref.all_replicasets() {
        if !db_config.experimental_sharding_implementation(&replicaset.tier) {
            // This tier is using the legacy vshard implementation
            continue;
        }

        if replicaset.current_bucket_state_version == replicaset.target_bucket_state_version {
            // No resharding needed
            continue;
        }

        debug_assert_eq!(
            replicaset.current_master_name, replicaset.target_master_name,
            "master switchover must already have been handled"
        );

        let tier = topology_ref.tier_by_name(&replicaset.tier)?;

        // _pico_tier.current_bucket_state_version = 0 means the bucket
        // distribution is not initialized yet
        // TODO(resharding): support resharding
        debug_assert_eq!(tier.current_bucket_state_version, 0);

        targets_total.push(replicaset.current_master_name.clone());
    }

    if targets_total.is_empty() {
        return Ok(None);
    }

    let request_kind = rpc::sharding::ReshardingRequestKind::Initialize;

    // We must check if step kind is different from the one we tried on
    // previous iteration, so that we know not to use irrelevant results
    type Action = AwaitResharding;
    let step_kind = Action::KIND;
    last_step_info.update_step_kind(step_kind);

    // Clear previous results if bucket state version changed, because now
    // everybody needs to apply the new configuration
    last_step_info.update_bucket_state_versions(topology_ref);

    // Note at this point all the instances should have their replication configured,
    // so it's ok to configure sharding for them
    let res = get_next_batch(
        &targets_total,
        last_step_info,
        db_config.governor_rpc_batch_size,
    );
    let targets_batch = match res {
        Ok(v) => v,
        Err(next_try) => {
            return Ok(Some(SleepDueToBackoff::new(next_try, step_kind).into()));
        }
    };

    let rpc = rpc::sharding::ReshardingRequest {
        term,
        applied,
        timeout: rpc_timeout,
        kind: request_kind,
    };

    Ok(Some(
        Action {
            targets_total,
            targets_batch,
            rpc,
        }
        .into(),
    ))
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance::Instance;
    use crate::topology_cache::TopologyCache;
    use rand::RngExt;
    use rand::SeedableRng;
    use smol_str::format_smolstr;
    use smol_str::ToSmolStr;
    use std::rc::Rc;

    fn test_tier(tier_name: impl ToSmolStr, bucket_count: u64) -> Tier {
        let mut tier = Tier::default();
        tier.name = tier_name.to_smolstr();
        tier.bucket_count = bucket_count;

        tier
    }

    fn test_tier_buckets_info(tier: &Tier) -> TierBucketsInfo {
        let mut info = TierBucketsInfo::default();
        info.tier_name = tier.name.clone();
        info.total_count = tier.bucket_count;

        info
    }

    fn test_replicaset(replicaset_name: impl ToSmolStr, weight: f64) -> Replicaset {
        let mut instance = Instance::for_tests();
        instance.replicaset_name = replicaset_name.to_smolstr().into();
        instance.replicaset_uuid = uuid::Uuid::new_v4().to_smolstr();

        let mut replicaset = Replicaset::with_one_instance(&instance);
        replicaset.weight = weight;

        replicaset
    }

    /// See `wrap_compute_bucket_distribution_imbalance` for parameter meaning
    fn check_compute_bucket_distribution_imbalance(
        total_bucket_count: u64,
        replicaset_weights: &[f64],
        current_distribution: &[u64],
        expected_imbalance: Option<&[i64]>,
    ) {
        let actual_imbalance = wrap_compute_bucket_distribution_imbalance(
            total_bucket_count,
            replicaset_weights,
            current_distribution,
        );

        assert_eq!(actual_imbalance.as_deref(), expected_imbalance);
    }

    /// - `total_bucket_count`: total bucket count of given tier
    /// - `replicaset_weights`: weight configuration of replicasets in a given tier
    /// - `current_distribution`: current bucket distribution among replicasets in a given tier
    ///
    /// Returns result of `compute_bucket_distribution_imbalance` in the
    /// following format:
    ///
    /// `None`, then all replicasets are well balanced.
    ///
    /// Otherwise `Some(imbalance)`, where for `imbalance[i] = X`
    /// If `X > 0`, then `replicaset[i]` needs `X` more buckets
    /// Else if `X < 0`, then it has `X` extra buckets
    /// Else it's currently well balanced.
    fn wrap_compute_bucket_distribution_imbalance(
        total_bucket_count: u64,
        replicaset_weights: &[f64],
        current_distribution: &[u64],
    ) -> Option<Vec<i64>> {
        assert_eq!(replicaset_weights.len(), current_distribution.len());

        //
        // Setup a pretend topology based on provided current distribution
        //

        let topology = Rc::new(TopologyCache::for_tests());

        let tier = test_tier("storage", total_bucket_count);
        topology.update_tier(None, Some(tier.clone()));

        let mut info = test_tier_buckets_info(&tier);

        // Order of `replicaset_names` matches order of `replicaset_weights` &
        // `current_distribution`. This is important for correctness and test
        // output readabilty
        let mut replicaset_names = vec![];

        for ((weight, buckets), i) in replicaset_weights.iter().zip(current_distribution).zip(1..) {
            let replicaset_name = format_smolstr!("storage_{i}");
            topology.update_replicaset(None, Some(test_replicaset(&replicaset_name, *weight)));
            info.add_replicaset(&replicaset_name);
            info.add_buckets(&replicaset_name, *buckets);
            replicaset_names.push(replicaset_name);
        }

        let topology_ref = topology.get();

        //
        // Call the function under test
        //

        let imbalance = compute_bucket_distribution_imbalance(&info, &topology_ref).unwrap()?;

        //
        // Check result invariants
        //

        // Check `actual_total` is computed correctly
        assert_eq!(
            imbalance.actual_total,
            current_distribution.iter().copied().sum::<u64>()
        );

        // Order of `actual_imbalance` matches order of `replicaset_names` and others
        let mut actual_imbalance = vec![];

        let mut sum_need = 0;
        let mut sum_extra = 0;

        for replicaset_name in &replicaset_names {
            let needs_more = imbalance
                .need_more_buckets
                .iter()
                .find(|(name, _)| *name == replicaset_name)
                .map(|(_, count)| *count);
            let has_extra = imbalance.have_extra_buckets.get(replicaset_name).copied();

            if needs_more.is_some() && has_extra.is_some() {
                panic!("cannot be true at the same time");
            }

            // Normalize back to a single value for ease of testing
            let mut value = 0;

            if let Some(needs_more) = needs_more {
                assert!(needs_more > 0, "{imbalance:?}");
                value = needs_more as _;
                sum_need += needs_more;
            }

            if let Some(has_extra) = has_extra {
                assert!(has_extra > 0, "{imbalance:?}");
                value = -(has_extra as i64);
                sum_extra += has_extra;
            }

            actual_imbalance.push(value);
        }

        // Conservation law: the net signed delta equals the gap between the
        // desired total and what is actually allocated.
        assert_eq!(
            sum_need as i64 - sum_extra as i64,
            total_bucket_count as i64 - imbalance.actual_total as i64,
            "{imbalance:?}, total_bucket_count: {total_bucket_count}, sum_need: {sum_need}, sum_extra: {sum_extra}"
        );

        Some(actual_imbalance)
    }

    #[test]
    fn test_compute_bucket_distribution_imbalance() {
        check_compute_bucket_distribution_imbalance(0, &[1.0], &[0], None);
        check_compute_bucket_distribution_imbalance(0, &[1.0, 2.0, 3.0], &[0, 0, 0], None);

        check_compute_bucket_distribution_imbalance(3000, &[1.0], &[0], Some(&[3000]));

        check_compute_bucket_distribution_imbalance(3000, &[1.0], &[3000], None);

        check_compute_bucket_distribution_imbalance(
            3000,
            &[1.0, 1.0],
            &[3000, 0],
            Some(&[-1500, 1500]),
        );

        check_compute_bucket_distribution_imbalance(
            3000,
            &[1.0, 1.0],
            &[0, 0],
            Some(&[1500, 1500]),
        );

        check_compute_bucket_distribution_imbalance(
            3000,
            &[1.0, 1.0, 1.0],
            &[0, 0, 0],
            Some(&[1000, 1000, 1000]),
        );

        check_compute_bucket_distribution_imbalance(
            3000,
            &[2.0, 1.0, 1.0],
            &[1000, 1000, 1000],
            Some(&[500, -250, -250]),
        );

        check_compute_bucket_distribution_imbalance(
            10,
            &[1.0, 1.0, 1.0],
            &[4, 4, 2],
            Some(&[-1, 0, 1]),
        );

        check_compute_bucket_distribution_imbalance(
            60000,
            &[4.0, 2.0, 1.0, 0.5, 0.5],
            &[1000, 1000, 1000, 1000, 1000],
            Some(&[29000, 14000, 6500, 2750, 2750]),
        );
    }

    fn init_rng() -> rand::rngs::StdRng {
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        // And also run with a random seed for chaos
        let seed = (time.as_secs_f64() * 1000_000.0) as u64;

        tlog!(Info, "rng seed: {seed}");

        let rng = rand::rngs::StdRng::seed_from_u64(seed);

        rng
    }

    #[test]
    fn test_compute_bucket_distribution_imbalance_randomized_single_replicaset() {
        let mut rng = init_rng();

        const N_ROUNDS: usize = 100;

        for _ in 0..N_ROUNDS {
            let total_count = rng.random_range(0..=60_000);
            let weight = rng.random_range(1.0..=100.0);
            let current_count = rng.random_range(0..=60_000);

            let actual_imbalance = wrap_compute_bucket_distribution_imbalance(
                total_count,
                &[weight],
                &[current_count],
            );

            let expected_imbalance = total_count as i64 - current_count as i64;
            if expected_imbalance == 0 {
                assert_eq!(actual_imbalance, None);
            } else {
                assert_eq!(
                    actual_imbalance.as_deref(),
                    Some([expected_imbalance].as_slice())
                );
            }
        }
    }

    #[test]
    fn test_compute_bucket_distribution_imbalance_invariants_randomized() {
        let mut rng = init_rng();

        const N_ROUNDS: usize = 100;

        for _ in 0..N_ROUNDS {
            let replication_factor = rng.random_range(1..=8);
            let total_count = rng.random_range(0..=60_000);
            let replicaset_weights: Vec<_> = (0..replication_factor)
                .map(|_| rng.random_range(1.0..=100.0))
                .collect();
            let current_distribution: Vec<_> = (0..replication_factor)
                .map(|_| rng.random_range(0..=60_000))
                .collect();

            // Run `compute_bucket_distribution_imbalance` and check invariants.
            let actual_imbalance = wrap_compute_bucket_distribution_imbalance(
                total_count,
                &replicaset_weights,
                &current_distribution,
            );

            let Some(actual_imbalance) = actual_imbalance else {
                continue;
            };

            // Apply the imbalance
            let mut next_distribution = vec![];
            for (current_count, delta) in current_distribution.into_iter().zip(actual_imbalance) {
                let next_count = (current_count as i64 + delta) as u64;
                next_distribution.push(next_count);
            }

            // Run `compute_bucket_distribution_imbalance` and check invariants.
            let next_imbalance = wrap_compute_bucket_distribution_imbalance(
                total_count,
                &replicaset_weights,
                &next_distribution,
            );

            // The constructed imbalance when applied always makes a balanced
            // distribution in one iteration
            assert_eq!(next_imbalance, None);
        }
    }
}
