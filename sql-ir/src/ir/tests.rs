use super::*;
use crate::ir::relation::{Column, ColumnRole, SpaceEngine, Table};
use crate::ir::types::{DerivedType, UnrestrictedType};
use pretty_assertions::assert_eq;
use rand::random;

#[test]
fn get_node() {
    let mut plan = Plan::default();

    let t = Table::new_sharded(
        random(),
        "t",
        vec![Column::new(
            "a",
            DerivedType::new(UnrestrictedType::Boolean),
            ColumnRole::User,
            false,
        )],
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
