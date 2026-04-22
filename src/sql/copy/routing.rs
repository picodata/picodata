use super::pending::ShardedCopyDestination;
use super::target::CopyTargetError;
use crate::catalog::pico_bucket::BucketState;
use crate::schema::TableDef;
use crate::sql::lua::bucket_into_rs;
use crate::sql::router::DEFAULT_QUERY_TIMEOUT;
use crate::storage::{Catalog, ToEntryIter};
use smol_str::SmolStr;
use std::time::Duration;
use tarantool::time::Instant;

#[derive(Debug)]
pub(super) enum CopyWriteMode {
    Global,
    Sharded(ShardedCopyRouting),
}

#[derive(Debug)]
pub(super) struct ShardedCopyRouting {
    pub(super) tier_name: SmolStr,
    pub(super) schema_version: u64,
    pub(super) prepared_bucket_state_version: u64,
    pub(super) dispatch_timeout: Duration,
    bucket_routes: Vec<BucketRoute>,
}

impl ShardedCopyRouting {
    pub(super) fn ensure_current(&self) -> Result<(), CopyTargetError> {
        let storage = Catalog::try_get(false).expect("storage should be initialized");
        let tier =
            storage
                .tiers
                .by_name(&self.tier_name)?
                .ok_or_else(|| CopyTargetError::NoSuchTier {
                    tier_name: self.tier_name.clone(),
                })?;
        if tier.current_bucket_state_version != self.prepared_bucket_state_version
            || tier.target_bucket_state_version != self.prepared_bucket_state_version
        {
            return Err(CopyTargetError::BucketRoutingStale {
                tier_name: self.tier_name.clone(),
                prepared_bucket_state_version: self.prepared_bucket_state_version,
                live_current_bucket_state_version: tier.current_bucket_state_version,
                live_target_bucket_state_version: tier.target_bucket_state_version,
            });
        }
        Ok(())
    }

    fn replicaset_uuid_for_bucket(&self, bucket_id: u64) -> Option<&str> {
        let index = self
            .bucket_routes
            .partition_point(|route| route.bucket_id_start <= bucket_id);
        let route = self.bucket_routes.get(index.checked_sub(1)?)?;
        (bucket_id <= route.bucket_id_end).then_some(route.replicaset_uuid.as_str())
    }

    pub(super) fn destination_for_bucket(
        &self,
        bucket_id: u64,
    ) -> Result<ShardedCopyDestination, CopyTargetError> {
        let replicaset_uuid = self.replicaset_uuid_for_bucket(bucket_id).ok_or_else(|| {
            CopyTargetError::MissingBucketRoute {
                tier_name: self.tier_name.clone(),
                bucket_id,
            }
        })?;
        if crate::sql::dispatch::should_dispatch_locally(
            Some(self.tier_name.as_str()),
            replicaset_uuid,
            "leader",
        )? {
            Ok(ShardedCopyDestination::Local)
        } else {
            Ok(ShardedCopyDestination::Replicaset(replicaset_uuid.into()))
        }
    }
}

#[derive(Debug)]
struct BucketRoute {
    bucket_id_start: u64,
    bucket_id_end: u64,
    replicaset_uuid: SmolStr,
}

pub(super) fn build_sharded_copy_routing(
    table_def: &TableDef,
    tier_name: &SmolStr,
) -> Result<ShardedCopyRouting, CopyTargetError> {
    let dispatch_timeout = DEFAULT_QUERY_TIMEOUT;
    let storage = Catalog::try_get(false).expect("storage should be initialized");
    let tier = storage
        .tiers
        .by_name(tier_name)?
        .ok_or_else(|| CopyTargetError::NoSuchTier {
            tier_name: tier_name.clone(),
        })?;
    if tier.current_bucket_state_version != tier.target_bucket_state_version {
        return Err(CopyTargetError::BucketRebalancingInProgress {
            tier_name: tier_name.clone(),
            current_bucket_state_version: tier.current_bucket_state_version,
            target_bucket_state_version: tier.target_bucket_state_version,
        });
    }

    let mut bucket_routes = Vec::new();
    for record in storage.pico_bucket.iter()? {
        if record.tier_name != *tier_name {
            continue;
        }
        if record.state != BucketState::Active {
            return Err(CopyTargetError::FeatureNotSupported(
                "COPY FROM STDIN currently does not support sharded writes while bucket rebalancing is in progress".into(),
            ));
        }

        let replicaset = storage
            .replicasets
            .get(record.current_replicaset_name.as_str())?
            .ok_or_else(|| {
                CopyTargetError::internal(format!(
                    "replicaset does not exist: {}",
                    record.current_replicaset_name
                ))
            })?;
        bucket_routes.push(BucketRoute {
            bucket_id_start: record.bucket_id_start,
            bucket_id_end: record.bucket_id_end,
            replicaset_uuid: replicaset.uuid,
        });
    }

    if bucket_routes.is_empty() {
        bucket_routes = build_router_bucket_routes(tier_name, tier.bucket_count, dispatch_timeout)?;
    }

    bucket_routes.sort_by_key(|route| route.bucket_id_start);
    validate_bucket_routes(tier_name, tier.bucket_count, &bucket_routes)?;
    Ok(ShardedCopyRouting {
        tier_name: tier_name.clone(),
        schema_version: table_def.schema_version,
        prepared_bucket_state_version: tier.current_bucket_state_version,
        bucket_routes,
        dispatch_timeout,
    })
}

fn build_router_bucket_routes(
    tier_name: &SmolStr,
    bucket_count: u64,
    dispatch_timeout: Duration,
) -> Result<Vec<BucketRoute>, CopyTargetError> {
    let lua = tarantool::lua_state();
    let deadline = Instant::now_fiber().saturating_add(dispatch_timeout);
    let mut bucket_routes = Vec::new();
    let mut current_uuid: Option<String> = None;
    let mut range_start = 1u64;

    for bucket_id in 1..=bucket_count {
        let replicaset_uuid = bucket_into_rs(&lua, bucket_id, Some(tier_name.as_str()), deadline)
            .map_err(CopyTargetError::from)?;
        match current_uuid.as_deref() {
            Some(current) if current == replicaset_uuid => {}
            Some(current) => {
                bucket_routes.push(BucketRoute {
                    bucket_id_start: range_start,
                    bucket_id_end: bucket_id - 1,
                    replicaset_uuid: current.into(),
                });
                current_uuid = Some(replicaset_uuid);
                range_start = bucket_id;
            }
            None => current_uuid = Some(replicaset_uuid),
        }
    }

    let Some(last_uuid) = current_uuid else {
        return Err(CopyTargetError::internal(format!(
            "failed to build bucket routes for tier {tier_name}"
        )));
    };
    bucket_routes.push(BucketRoute {
        bucket_id_start: range_start,
        bucket_id_end: bucket_count,
        replicaset_uuid: last_uuid.into(),
    });
    Ok(bucket_routes)
}

fn validate_bucket_routes(
    tier_name: &SmolStr,
    bucket_count: u64,
    bucket_routes: &[BucketRoute],
) -> Result<(), CopyTargetError> {
    let Some(first) = bucket_routes.first() else {
        return Err(CopyTargetError::internal(format!(
            "failed to build bucket routes for tier {tier_name}"
        )));
    };
    if first.bucket_id_start != 1 {
        return Err(CopyTargetError::internal(format!(
            "bucket routes for tier {tier_name} do not start from bucket 1"
        )));
    }

    let mut expected_start = 1u64;
    for route in bucket_routes {
        if route.bucket_id_start != expected_start || route.bucket_id_end < route.bucket_id_start {
            return Err(CopyTargetError::internal(format!(
                "bucket routes for tier {tier_name} are not contiguous"
            )));
        }
        expected_start = route.bucket_id_end + 1;
    }

    if expected_start != bucket_count + 1 {
        return Err(CopyTargetError::internal(format!(
            "bucket routes for tier {tier_name} do not cover all buckets"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn routing_for_test() -> ShardedCopyRouting {
        ShardedCopyRouting {
            tier_name: "default".into(),
            schema_version: 1,
            prepared_bucket_state_version: 1,
            bucket_routes: vec![
                BucketRoute {
                    bucket_id_start: 1,
                    bucket_id_end: 100,
                    replicaset_uuid: "local-rs".into(),
                },
                BucketRoute {
                    bucket_id_start: 101,
                    bucket_id_end: 200,
                    replicaset_uuid: "remote-rs-1".into(),
                },
                BucketRoute {
                    bucket_id_start: 201,
                    bucket_id_end: 300,
                    replicaset_uuid: "remote-rs-2".into(),
                },
            ],
            dispatch_timeout: DEFAULT_QUERY_TIMEOUT,
        }
    }

    #[test]
    fn replicaset_uuid_for_bucket_maps_to_target_replicasets() {
        let routing = routing_for_test();

        assert_eq!(routing.replicaset_uuid_for_bucket(7), Some("local-rs"));
        assert_eq!(routing.replicaset_uuid_for_bucket(150), Some("remote-rs-1"));
        assert_eq!(routing.replicaset_uuid_for_bucket(250), Some("remote-rs-2"));
        assert_eq!(routing.replicaset_uuid_for_bucket(301), None);
    }

    #[test]
    fn destination_for_bucket_requires_bucket_id_for_sharded_routing() {
        let target = crate::sql::copy::target::PreparedCopyTarget::for_test_with_write_mode(
            CopyWriteMode::Sharded(routing_for_test()),
        );

        let error = target
            .destination_for_bucket(None)
            .expect_err("sharded routing should require bucket id");
        assert!(matches!(error, CopyTargetError::MissingBucketId));
    }
}
