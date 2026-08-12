//! [`Platform`] abstracts every leaf IO operation performed by
//! [`resharding_loop`] behind a trait, so that multiple isolated
//! instances of the resharding logic can be run in one process for the purpose
//! of simulation testing.
//!
//! [`PlatformActual`] is the actual production implementation; the simulation
//! provides one of its own.

use crate::config::AlterSystemParametersRef;
use crate::resharding_loop;
use crate::topology_cache::TopologyCache;
use crate::traft::node;
use crate::traft::op::Dml;
use crate::traft::RaftIndex;
use crate::vshard;
use crate::vshard::VshardBucketRecord;
use crate::Result;
use std::time::Duration;

////////////////////////////////////////////////////////////////////////////////
// Platform
////////////////////////////////////////////////////////////////////////////////

/// Abstractions for all platform interactions in resharding_loop.
///
/// See also module-level doc-comments.
pub(crate) trait Platform {
    //
    // accessors
    //

    /// Read only access to current instance's topology cache.
    ///
    /// Real implementation redirects to [`node::Node::topology_cache`].
    fn topology_cache(&self) -> &TopologyCache;

    /// Read only access to current instance's _pico_db_config cache.
    ///
    /// Real implementation redirects to [`node::Node::alter_system_parameters`].
    fn alter_system_parameters(&self) -> &AlterSystemParametersRef;

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

    /// Wait until current instance's raft applied index changes.
    ///
    /// Real implementation redirects to [`node::Node::wait_index_change`].
    fn wait_index_change(&self, timeout: Duration) -> Result<RaftIndex>;

    //
    // local `_bucket`
    //

    /// Read all local `_bucket` records with id in `start..=end`.
    ///
    /// Real implementation redirects to [`resharding_loop::read_local_buckets`].
    fn read_local_buckets(&self, start: u64, end: u64) -> Result<Vec<VshardBucketRecord>>;

    /// Write records into the local `_bucket` space in a transaction.
    ///
    /// Real implementation redirects to [`resharding_loop::write_local_buckets`].
    fn write_local_buckets(&self, v: Vec<VshardBucketRecord>) -> Result<()>;

    //
    // vshard router
    //

    /// Wait for vshard router discovery to complete on the current instance.
    /// This ensures route_map is up-to-date before bumping the bucket state version.
    ///
    /// Real implementation redirects to [`vshard::wait_router_discovery_complete`].
    fn wait_router_discovery_complete(&self, tier: &str, timeout: Duration) -> Result<()>;
}

////////////////////////////////////////////////////////////////////////////////
// PlatformActual
////////////////////////////////////////////////////////////////////////////////

/// Production implementation of [`Platform`].
/// See also module-level doc-comments.
pub(crate) struct PlatformActual {
    node: &'static node::Node,
}

impl PlatformActual {
    pub fn new(node: &'static node::Node) -> Self {
        Self { node }
    }
}

impl Platform for PlatformActual {
    fn topology_cache(&self) -> &TopologyCache {
        &self.node.topology_cache
    }

    fn alter_system_parameters(&self) -> &AlterSystemParametersRef {
        &self.node.alter_system_parameters
    }

    fn applied_index(&self) -> RaftIndex {
        self.node.get_index()
    }

    fn do_cas(&self, applied: RaftIndex, dmls: Vec<Dml>, timeout: Duration) -> Result<()> {
        resharding_loop::do_cas_requests(applied, dmls, timeout)
    }

    fn wait_index_change(&self, timeout: Duration) -> Result<RaftIndex> {
        self.node.wait_index_change(timeout)
    }

    fn read_local_buckets(&self, start: u64, end: u64) -> Result<Vec<VshardBucketRecord>> {
        resharding_loop::read_local_buckets(start, end)
    }

    fn write_local_buckets(&self, v: Vec<VshardBucketRecord>) -> Result<()> {
        resharding_loop::write_local_buckets(v)
    }

    fn wait_router_discovery_complete(&self, tier: &str, timeout: Duration) -> Result<()> {
        vshard::wait_router_discovery_complete(tier, timeout)
    }
}
