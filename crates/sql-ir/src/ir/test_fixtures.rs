//! Column/table constructors shared by tests across the SQL crates.

use crate::ir::node::NodeId;
use crate::ir::relation::{Column, ColumnRole};
use crate::ir::types::{DerivedType, UnrestrictedType};
use crate::ir::Plan;
use smol_str::SmolStr;

pub fn column_integer_user_non_null(name: SmolStr) -> Column {
    Column {
        name,
        r#type: DerivedType::new(UnrestrictedType::Integer),
        role: ColumnRole::User,
        is_nullable: false,
    }
}

pub fn column_user_non_null(name: SmolStr, r#type: UnrestrictedType) -> Column {
    Column {
        name,
        r#type: DerivedType::new(r#type),
        role: ColumnRole::User,
        is_nullable: false,
    }
}

pub fn sharding_column() -> Column {
    Column {
        name: SmolStr::from("bucket_id"),
        r#type: DerivedType::new(UnrestrictedType::Integer),
        role: ColumnRole::Sharding,
        is_nullable: true,
    }
}

/// Helper function to extract a motion id from a plan.
///
/// # Panics
///   Motion node does not found
#[must_use]
pub fn get_motion_id(plan: &Plan, slice_id: usize, motion_idx: usize) -> Option<&NodeId> {
    plan.slices.slice(slice_id).unwrap().position(motion_idx)
}
