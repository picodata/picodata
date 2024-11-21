use super::*;
use crate::executor::vtable::VTableColumn;
use crate::ir::relation::{Column, ColumnRole, SpaceEngine, Table, Type};
use pretty_assertions::assert_eq;
use smol_str::SmolStr;

/// Helper function to create `Column` object with given name and default:
///
/// * `type` = Integer
/// * `is_nullable` = false
/// * `role` = User
///
/// Used only for tests purposes.
#[must_use]
#[cfg(test)]
pub fn column_integer_user_non_null(name: SmolStr) -> Column {
    Column {
        name,
        r#type: Type::Integer,
        role: ColumnRole::User,
        is_nullable: false,
    }
}

/// Copy of `column_integer_user_non_null` but without `name`.
#[must_use]
#[cfg(test)]
pub fn vcolumn_integer_user_non_null() -> VTableColumn {
    VTableColumn {
        r#type: Type::Integer,
        role: ColumnRole::User,
        is_nullable: false,
    }
}

/// Helper function to create `Column` object with given name, type and default
/// * `is_nullable` = false
/// * `role` = User
///
/// Used only for tests purposes.
#[must_use]
#[cfg(test)]
pub fn column_user_non_null(name: SmolStr, r#type: Type) -> Column {
    Column {
        name,
        r#type,
        role: ColumnRole::User,
        is_nullable: false,
    }
}

/// Copy of `column_integer_user_non_null` but without `name`.
#[must_use]
#[cfg(test)]
pub fn vcolumn_user_non_null(r#type: Type) -> VTableColumn {
    VTableColumn {
        r#type,
        role: ColumnRole::User,
        is_nullable: false,
    }
}

/// Helper function to create sharding `Column` object with default
/// * `is_nullable` = true
/// * `role` = Sharding
/// * `type` = Unsigned
///
/// Used only for tests purposes.
#[must_use]
#[cfg(test)]
pub fn sharding_column() -> Column {
    Column {
        name: SmolStr::from("bucket_id"),
        r#type: Type::Unsigned,
        role: ColumnRole::Sharding,
        is_nullable: true,
    }
}

#[test]
fn get_node() {
    let mut plan = Plan::default();

    let t = Table::new_sharded(
        "t",
        vec![Column::new("a", Type::Boolean, ColumnRole::User, false)],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t);

    let scan_id = plan.add_scan("t", None).unwrap();

    if let Node::Relational(Relational::ScanRelation(ScanRelation { relation, .. })) =
        plan.get_node(scan_id).unwrap()
    {
        assert_eq!(relation, "t");
    } else {
        panic!("Unexpected node returned!")
    }
}

#[test]
fn get_node_oor() {
    let plan = Plan::default();
    assert_eq!(
        SbroadError::NotFound(Entity::Node, "from Arena32 with index 42".into()),
        plan.get_node(NodeId {
            offset: 42,
            arena_type: ArenaType::Arena32
        })
        .unwrap_err()
    );
}

//TODO: add relation test
