//! [`Platform`] abstracts every leaf IO operation performed by
//! simulated code behind a trait, so that multiple isolated instances can be
//! run in one process for the purpose of simulation testing.

use crate::catalog_cache::CatalogCache;
use crate::config::AlterSystemParametersRef;
use crate::resharding_loop;
use crate::resharding_loop::local_bucket;
use crate::resharding_loop::ReshardingStatus;
use crate::storage::index_by_ids_unchecked;
use crate::tarantool::box_schema_version;
use crate::traft::node;
use crate::traft::op::Dml;
use crate::traft::RaftIndex;
use crate::vshard;
use crate::vshard::BucketRef;
use crate::vshard::VshardBucketRecord;
use crate::Result;
use ::tarantool::fiber::r#async::watch;
use std::time::Duration;
use tarantool::error::BoxError;
use tarantool::index::IndexId;
use tarantool::index::IteratorType;
use tarantool::index::TuplePosition;
use tarantool::space::SpaceId;
use tarantool::tuple::Tuple;

////////////////////////////////////////////////////////////////////////////////
// Platform
////////////////////////////////////////////////////////////////////////////////

/// Abstractions for all platform interactions in resharding_loop.
///
/// See also module-level doc-comments.
pub trait Platform {
    //
    // accessors
    //

    /// Read only access to current instance's catalog cache.
    ///
    /// Real implementation redirects to [`node::Node::catalog_cache`].
    fn catalog_cache(&self) -> &CatalogCache;

    /// Read only access to current instance's _pico_db_config cache.
    ///
    /// Real implementation redirects to [`node::Node::alter_system_parameters`].
    fn alter_system_parameters(&self) -> &AlterSystemParametersRef;

    //
    // requested status
    //
    async fn wait_action_requested(
        &self,
        requested_status: &mut watch::Receiver<(ReshardingStatus, u64)>,
    ) -> Result<()>;

    //
    // raft / CAS
    //

    /// Current instance's raft applied index.
    ///
    /// Real implementation redirects to [`node::Node::get_index`].
    fn applied_index(&self) -> RaftIndex;

    /// Sends a single CAS request for the provided `dmls` using the implicit
    /// CAS predicate. Waits for `timeout` for the raft entry to be applied for.
    ///
    /// Real implementation redirects to [`resharding_loop::do_cas_requests`].
    fn do_cas(&self, applied: RaftIndex, dmls: Vec<Dml>, timeout: Duration) -> Result<()>;

    /// Wait until current instance becomes the replicaset master.
    ///
    /// Real implementation redirects to [`node::Node::wait_index_change`].
    fn wait_until_master(&self, timeout: Duration) -> Result<RaftIndex>;

    //
    // local `_bucket`
    //

    /// Read all local `_bucket` records with id in `start..=end`.
    ///
    /// Real implementation redirects to [`local_bucket::read_local_buckets`].
    fn read_local_buckets(&self, start: u64, end: u64) -> Result<Vec<VshardBucketRecord>>;

    /// Write records into the local `_bucket` space in a transaction.
    ///
    /// Real implementation redirects to [`local_bucket::write_local_buckets`].
    fn write_local_buckets(&self, v: Vec<VshardBucketRecord>) -> Result<()>;

    //
    // sharded table data
    //

    /// Current box schema version.
    ///
    /// Real implementation redirects to [`box_schema_version`].
    fn box_schema_version(&self) -> u64;

    /// Returns an index iterator over the bucket_id index selected on the given
    /// `bucket_id`.
    ///
    /// `index_id` must be the id of the bucket_id index in space with `space_id`.
    ///
    /// `after` should be a position previously returned from [`Self::tuple_position`]
    /// called on the tuple returned from the iterator return by this function.
    ///
    /// Real implementation redirects to [`Index::select_after_v2`].
    ///
    /// [`Index::select_after_v2`]: tarantool::index::Index::select_after_v2
    fn index_bucket_id_select_after(
        &self,
        space_id: SpaceId,
        index_id: IndexId,
        bucket_id: u64,
        after: &TuplePosition,
    ) -> Result<impl Iterator<Item = Result<Tuple, BoxError>> + use<Self>>;

    /// Returns the position of the tuple in the given space index for use in
    /// [`Self::index_bucket_id_select_after`].
    ///
    /// # Safety
    /// `tuple_data` must be a valid msgpack array encoding.
    ///
    /// Real implementation redirects to [`Index::tuple_position`].
    ///
    /// [`Index::tuple_position`]: tarantool::index::Index::tuple_position
    unsafe fn tuple_position(
        &self,
        space_id: SpaceId,
        index_id: IndexId,
        tuple_data: &[u8],
    ) -> Result<TuplePosition>;

    //
    // vshard refs/locks/router/move
    //

    /// Check this bucket's vshard's in-memory bucket refs.
    ///
    /// Real implementation is not there yet, it will read vshard's
    /// `bucket_refs` from `vshard/storage/init.lua`.
    fn get_bucket_ref(&self, bucket_id: u64) -> Result<Option<BucketRef>>;

    /// Wait for vshard router discovery to complete on the current instance.
    /// This ensures route_map is up-to-date before bumping the bucket state version.
    ///
    /// Real implementation redirects to [`vshard::wait_router_discovery_complete`].
    fn wait_router_discovery_complete(&self, tier: &str, timeout: Duration) -> Result<()>;
}

////////////////////////////////////////////////////////////////////////////////
// impl for Node
////////////////////////////////////////////////////////////////////////////////

/// Production implementation of [`Platform`].
/// See also module-level doc-comments.
impl Platform for node::Node {
    fn catalog_cache(&self) -> &CatalogCache {
        &self.catalog_cache
    }

    fn alter_system_parameters(&self) -> &AlterSystemParametersRef {
        &self.alter_system_parameters
    }

    async fn wait_action_requested(
        &self,
        requested_status: &mut watch::Receiver<(ReshardingStatus, u64)>,
    ) -> Result<()> {
        _ = requested_status.changed().await;
        Ok(())
    }

    fn applied_index(&self) -> RaftIndex {
        self.get_index()
    }

    fn do_cas(&self, applied: RaftIndex, dmls: Vec<Dml>, timeout: Duration) -> Result<()> {
        resharding_loop::do_cas_requests(applied, dmls, timeout)
    }

    fn wait_until_master(&self, timeout: Duration) -> Result<RaftIndex> {
        self.wait_index_change(timeout)
    }

    fn read_local_buckets(&self, start: u64, end: u64) -> Result<Vec<VshardBucketRecord>> {
        local_bucket::read_local_buckets(start, end)
    }

    fn write_local_buckets(&self, v: Vec<VshardBucketRecord>) -> Result<()> {
        local_bucket::write_local_buckets(v)
    }

    fn box_schema_version(&self) -> u64 {
        box_schema_version()
    }

    fn index_bucket_id_select_after(
        &self,
        space_id: SpaceId,
        index_id: IndexId,
        bucket_id: u64,
        after: &TuplePosition,
    ) -> Result<impl Iterator<Item = Result<Tuple, BoxError>> + use<>> {
        let index = index_by_ids_unchecked(space_id, index_id);
        Ok(index.select_after_v2(IteratorType::Eq, &[bucket_id], after)?)
    }

    unsafe fn tuple_position(
        &self,
        space_id: SpaceId,
        index_id: IndexId,
        tuple_data: &[u8],
    ) -> Result<TuplePosition> {
        let index = index_by_ids_unchecked(space_id, index_id);
        // SAFETY: guaranteed by the caller.
        Ok(unsafe { index.tuple_position_unchecked(tuple_data)? })
    }

    fn wait_router_discovery_complete(&self, tier: &str, timeout: Duration) -> Result<()> {
        vshard::wait_router_discovery_complete(tier, timeout)
    }

    fn get_bucket_ref(&self, _bucket_id: u64) -> Result<Option<BucketRef>> {
        unimplemented!("coming soon")
    }
}
