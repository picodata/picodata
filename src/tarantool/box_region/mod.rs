#![warn(missing_docs)]

//! An interface to the tarantool box region allocator.
//!
//! Box region allocator is a tarantool per-fiber allocator from the `small` library.
//! It is useful for allocating tons of small objects and free them at once.
//!
//! The main use in picodata is implementing tarantool APIs that expect
//! the returned value to be allocated in region allocator.
//!
//! If you want to use box region allocator in picodata for reasons other than
//! tarantool interoperability, please consider using a rust-native bump allocator instead.

mod allocation;
mod checkpoint;

pub use self::{
    allocation::{alloc_unaligned, grow_unaligned},
    checkpoint::BoxRegionCheckpoint,
};
