//! Whitebox tests of the SQL planning pipeline.
//!
//! These suites live in the facade crate because they parse SQL text
//! end-to-end: they need the frontend, the executor's mock router and
//! the IR at the same time. Purely IR-local unit tests stay in sql-ir.
//!
//! The `mock` feature is always enabled here via the crate's
//! self-referential dev-dependency.

mod block_pattern;
mod distribution;
mod explain;
mod ir_helpers;
mod plugin;
mod redistribution;
mod subtree_cloner;
mod transformation;
