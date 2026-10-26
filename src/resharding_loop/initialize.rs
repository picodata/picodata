use crate::catalog::pico_bucket::BucketIdRange;
use crate::catalog::pico_bucket::BucketState;
use crate::catalog_cache::CatalogCacheRef;
use crate::replicaset::Replicaset;
use crate::resharding_loop::action::ReshardingAction;
use crate::resharding_loop::action::*;
use crate::resharding_loop::bucket_version::make_actualization_dmls;
use crate::resharding_loop::find_sharded_bucket_updates::find_sharded_bucket_updates;
use crate::simulation::platform::Platform;
use crate::tlog;
use crate::vshard::VshardBucketState as VBS;
use crate::Result;

////////////////////////////////////////////////////////////////////////////////
// initialize_sharded_states
////////////////////////////////////////////////////////////////////////////////

pub fn initialize_sharded_states(
    platform: &impl Platform,
    this_replicaset: &Replicaset,
    topology_ref: &CatalogCacheRef,
) -> Result<ReshardingAction> {
    let bucket_ranges = &topology_ref.buckets_info(&this_replicaset.tier)?.ranges;

    let mut ranges: Vec<BucketIdRange> = vec![];

    //
    // Lookup bucket ranges of interest in _pico_bucket
    //
    for (range, _) in bucket_ranges.iter().zip(0..) {
        if range.current_replicaset_name != *this_replicaset.name {
            // Not our buckets
            continue;
        }

        if let BucketState::Unknown(unknown) = &range.state {
            // FIXME: rate limit the log message
            tlog!(
                Warning,
                "unknown bucket state '{unknown}' for bucket range {range:?}, going to skip"
            );
            continue;
        }

        debug_assert!(range.state.is_active(), "{range:?}");

        if let Some(last) = ranges.last() {
            debug_assert!(*last.end() < range.bucket_id_start, "must be ordered");
        }

        ranges.push(range.bucket_ids());
    }

    if ranges.is_empty() {
        // No action needed on this replicaset
        tlog!(
            Debug,
            "bucket distribution initialized, actualizing distribution version"
        );

        let version_bump = make_actualization_dmls(this_replicaset)?;

        return Ok(ReshardingAction::ActualizeBucketStateVersion(
            ActualizeBucketStateVersion { version_bump },
        ));
    }

    //
    // Check states in _bucket
    //

    for range in ranges.iter().cloned() {
        // Note: must first set as "receiving" because otherwise vshard's on_commit trigger will fail
        let (from_state, to_state) = (None, VBS::Receiving);
        let changes =
            find_sharded_bucket_updates(platform, range.clone(), from_state, to_state, None)?;
        if !changes.is_empty() {
            return Ok(ReshardingAction::ActualizeShardedState(
                ActualizeShardedState {
                    from_state,
                    to_state,
                    range,
                    changes,
                },
            ));
        }

        let (from_state, to_state) = (Some(VBS::Receiving), VBS::Active);
        let changes =
            find_sharded_bucket_updates(platform, range.clone(), from_state, to_state, None)?;
        if !changes.is_empty() {
            return Ok(ReshardingAction::ActualizeShardedState(
                ActualizeShardedState {
                    from_state,
                    to_state,
                    range,
                    changes,
                },
            ));
        }
    }

    //
    // All _bucket states are up to date with _pico_bucket
    //

    tlog!(
        Debug,
        "bucket distribution initialized, actualizing distribution version"
    );

    let version_bump = make_actualization_dmls(this_replicaset)?;

    Ok(ReshardingAction::ActualizeBucketStateVersion(
        ActualizeBucketStateVersion { version_bump },
    ))
}
