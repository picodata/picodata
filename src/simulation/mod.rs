//! Deterministic multi-instance simulation engine.
//!
//! Currently only [`crate::resharding_loop`] is supported.
//!
//! The simulation is deterministic. Scheduler picks actions (see
//! [`action::PretendAction`]) to perform out of a set of possible actions
//! computed at each iterations (see [`engine::determine_potential_actions`]).
//!
//! Action are performed in [`engine::do_action`] and recorded
//! into a [`cluster::PretendCluster::trace`] which can be tested for
//! determinism (see test
//! [`crate::resharding_loop::test::simulation_trace_is_deterministic`]).
//!
//! The scheduler keeps track of multiple fibers allowing at most one to run
//! at any moment. Every possible sequence of fiber interleavings is
//! possible to test this way.
//!
//! Whenever possible actual code is used instead of modeling it:
//! the CAS conflict checks, DML application, topology_cache updates.
//!
//! Modeled systems are kept as realistic as possible: sharded replication
//! requires first a successful modeled write to disk along with
//! on_replace/on_commit triggers if needed in correct sequence, each
//! instance applies replication stream independently and asynchronously.

pub mod action;
pub mod action_helpers;
pub mod catalog;
pub mod cluster;
pub mod engine;
pub mod fiber;
pub mod instance;
pub mod platform;
pub mod raft;
pub mod sharded_wal;
pub mod sharding;
