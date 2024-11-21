use crate::{
    ir::node::NodeId,
    ir::{transformation::helpers::sql_to_optimized_ir, ArenaType},
};
use pretty_assertions::assert_eq;

#[test]
fn simple_select() {
    let query = r#"SELECT "product_code" FROM "hash_testing""#;
    let plan = sql_to_optimized_ir(query, vec![]);

    let actual_arena = plan.formatted_arena().unwrap();

    let mut expected_arena = String::new();
    expected_arena.push_str(
        r#"---------------------------------------------
[id: 164] relation: ScanRelation
	Relation: hash_testing
	[No children]
	Output_id: 064
		[id: 064] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 032] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 1, arena_type: Arena64 }), targets: None, position: 0, col_type: Integer, asterisk_source: None })]
				[id: 132] expression: Alias [name = product_code, child = Reference(Reference { parent: Some(NodeId { offset: 1, arena_type: Arena64 }), targets: None, position: 1, col_type: String, asterisk_source: None })]
				[id: 232] expression: Alias [name = product_units, child = Reference(Reference { parent: Some(NodeId { offset: 1, arena_type: Arena64 }), targets: None, position: 2, col_type: Boolean, asterisk_source: None })]
				[id: 332] expression: Alias [name = sys_op, child = Reference(Reference { parent: Some(NodeId { offset: 1, arena_type: Arena64 }), targets: None, position: 3, col_type: Unsigned, asterisk_source: None })]
				[id: 432] expression: Alias [name = bucket_id, child = Reference(Reference { parent: Some(NodeId { offset: 1, arena_type: Arena64 }), targets: None, position: 4, col_type: Unsigned, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 364] relation: Projection
	Children:
		Child_id = 164
	Output_id: 264
		[id: 264] expression: Row [distribution = Some(Any)]
			List:
				[id: 532] expression: Alias [name = product_code, child = Reference(Reference { parent: Some(NodeId { offset: 3, arena_type: Arena64 }), targets: Some([0]), position: 1, col_type: String, asterisk_source: None })]
---------------------------------------------
"#);

    assert_eq!(expected_arena, actual_arena);
}

#[test]
fn simple_join() {
    let query = r#"SELECT "id" FROM
                        (SELECT "id" FROM "test_space") as "t1"
                        INNER JOIN
                        (SELECT "identification_number" FROM "hash_testing") as "t2"
                        ON "t1"."id" = "t2"."identification_number""#;
    let plan = sql_to_optimized_ir(query, vec![]);
    let actual_arena = plan.formatted_arena().unwrap();

    let mut expected_arena = String::new();
    expected_arena.push_str(
        r#"---------------------------------------------
[id: 164] relation: ScanRelation
	Relation: test_space
	[No children]
	Output_id: 064
		[id: 064] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 032] expression: Alias [name = id, child = Reference(Reference { parent: Some(NodeId { offset: 1, arena_type: Arena64 }), targets: None, position: 0, col_type: Unsigned, asterisk_source: None })]
				[id: 132] expression: Alias [name = sysFrom, child = Reference(Reference { parent: Some(NodeId { offset: 1, arena_type: Arena64 }), targets: None, position: 1, col_type: Unsigned, asterisk_source: None })]
				[id: 232] expression: Alias [name = FIRST_NAME, child = Reference(Reference { parent: Some(NodeId { offset: 1, arena_type: Arena64 }), targets: None, position: 2, col_type: String, asterisk_source: None })]
				[id: 332] expression: Alias [name = sys_op, child = Reference(Reference { parent: Some(NodeId { offset: 1, arena_type: Arena64 }), targets: None, position: 3, col_type: Unsigned, asterisk_source: None })]
				[id: 432] expression: Alias [name = bucket_id, child = Reference(Reference { parent: Some(NodeId { offset: 1, arena_type: Arena64 }), targets: None, position: 4, col_type: Unsigned, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 364] relation: Projection
	Children:
		Child_id = 164
	Output_id: 264
		[id: 264] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 532] expression: Alias [name = id, child = Reference(Reference { parent: Some(NodeId { offset: 3, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Unsigned, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 564] relation: ScanSubQuery
	Alias: t1
	Children:
		Child_id = 364
	Output_id: 464
		[id: 464] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 632] expression: Alias [name = id, child = Reference(Reference { parent: Some(NodeId { offset: 5, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Unsigned, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 764] relation: ScanRelation
	Relation: hash_testing
	[No children]
	Output_id: 664
		[id: 664] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 732] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 7, arena_type: Arena64 }), targets: None, position: 0, col_type: Integer, asterisk_source: None })]
				[id: 832] expression: Alias [name = product_code, child = Reference(Reference { parent: Some(NodeId { offset: 7, arena_type: Arena64 }), targets: None, position: 1, col_type: String, asterisk_source: None })]
				[id: 932] expression: Alias [name = product_units, child = Reference(Reference { parent: Some(NodeId { offset: 7, arena_type: Arena64 }), targets: None, position: 2, col_type: Boolean, asterisk_source: None })]
				[id: 1032] expression: Alias [name = sys_op, child = Reference(Reference { parent: Some(NodeId { offset: 7, arena_type: Arena64 }), targets: None, position: 3, col_type: Unsigned, asterisk_source: None })]
				[id: 1132] expression: Alias [name = bucket_id, child = Reference(Reference { parent: Some(NodeId { offset: 7, arena_type: Arena64 }), targets: None, position: 4, col_type: Unsigned, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 964] relation: Projection
	Children:
		Child_id = 764
	Output_id: 864
		[id: 864] expression: Row [distribution = Some(Any)]
			List:
				[id: 1232] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 9, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Integer, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 1164] relation: ScanSubQuery
	Alias: t2
	Children:
		Child_id = 964
	Output_id: 1064
		[id: 1064] expression: Row [distribution = Some(Any)]
			List:
				[id: 1332] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 11, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Integer, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 0136] relation: Motion [policy = Segment(MotionKey { targets: [Reference(0)] }), alias = t2]
	Children:
		Child_id = 1164
	Output_id: 2064
		[id: 2064] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 1932] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 0, arena_type: Arena136 }), targets: Some([0]), position: 0, col_type: Integer, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 1564] relation: InnerJoin
	Condition:
		[id: 1832] expression: Bool [op: =]
			Left child
			[id: 1864] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
				List:
					[id: 1496] expression: Reference
						Alias: id
						Referenced table name (or alias): t1
						Parent: Some(NodeId { offset: 15, arena_type: Arena64 })
						target_id: 0
						Column type: unsigned
			Right child
			[id: 1964] expression: Row [distribution = Some(Any)]
				List:
					[id: 1596] expression: Reference
						Alias: identification_number
						Referenced table name (or alias): t2
						Parent: Some(NodeId { offset: 15, arena_type: Arena64 })
						target_id: 1
						Column type: integer
	Children:
		Child_id = 564
		Child_id = 0136
	Output_id: 1464
		[id: 1464] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [1] }, Key { positions: [0] }}) })]
			List:
				[id: 1532] expression: Alias [name = id, child = Reference(Reference { parent: Some(NodeId { offset: 15, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Unsigned, asterisk_source: None })]
				[id: 1632] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 15, arena_type: Arena64 }), targets: Some([1]), position: 0, col_type: Integer, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 1764] relation: Projection
	Children:
		Child_id = 1564
	Output_id: 1664
		[id: 1664] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 1732] expression: Alias [name = id, child = Reference(Reference { parent: Some(NodeId { offset: 17, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Unsigned, asterisk_source: None })]
---------------------------------------------
"#);

    assert_eq!(expected_arena, actual_arena);
}

#[test]
fn simple_join_subtree() {
    let query = r#"SELECT "id" FROM
                        (SELECT "id" FROM "test_space") as "t1"
                        INNER JOIN
                        (SELECT "identification_number" FROM "hash_testing") as "t2"
                        ON "t1"."id" = "t2"."identification_number""#;
    let plan = sql_to_optimized_ir(query, vec![]);

    // Taken from the expected arena output in the `simple_join` test.
    let inner_join_inner_child_id = NodeId {
        offset: 0,
        arena_type: ArenaType::Arena136,
    };

    let actual_arena_subtree = plan
        .formatted_arena_subtree(inner_join_inner_child_id)
        .unwrap();
    let mut expected_arena_subtree = String::new();
    expected_arena_subtree.push_str(
        r#"---------------------------------------------
[id: 764] relation: ScanRelation
	Relation: hash_testing
	[No children]
	Output_id: 664
		[id: 664] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 732] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 7, arena_type: Arena64 }), targets: None, position: 0, col_type: Integer, asterisk_source: None })]
				[id: 832] expression: Alias [name = product_code, child = Reference(Reference { parent: Some(NodeId { offset: 7, arena_type: Arena64 }), targets: None, position: 1, col_type: String, asterisk_source: None })]
				[id: 932] expression: Alias [name = product_units, child = Reference(Reference { parent: Some(NodeId { offset: 7, arena_type: Arena64 }), targets: None, position: 2, col_type: Boolean, asterisk_source: None })]
				[id: 1032] expression: Alias [name = sys_op, child = Reference(Reference { parent: Some(NodeId { offset: 7, arena_type: Arena64 }), targets: None, position: 3, col_type: Unsigned, asterisk_source: None })]
				[id: 1132] expression: Alias [name = bucket_id, child = Reference(Reference { parent: Some(NodeId { offset: 7, arena_type: Arena64 }), targets: None, position: 4, col_type: Unsigned, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 964] relation: Projection
	Children:
		Child_id = 764
	Output_id: 864
		[id: 864] expression: Row [distribution = Some(Any)]
			List:
				[id: 1232] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 9, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Integer, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 1164] relation: ScanSubQuery
	Alias: t2
	Children:
		Child_id = 964
	Output_id: 1064
		[id: 1064] expression: Row [distribution = Some(Any)]
			List:
				[id: 1332] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 11, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Integer, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 0136] relation: Motion [policy = Segment(MotionKey { targets: [Reference(0)] }), alias = t2]
	Children:
		Child_id = 1164
	Output_id: 2064
		[id: 2064] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 1932] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 0, arena_type: Arena136 }), targets: Some([0]), position: 0, col_type: Integer, asterisk_source: None })]
---------------------------------------------
"#
    );

    assert_eq!(expected_arena_subtree, actual_arena_subtree);
}

#[test]
fn simple_aggregation_with_group_by() {
    let query = r#"SELECT "product_code" FROM "hash_testing" GROUP BY "product_code""#;
    let plan = sql_to_optimized_ir(query, vec![]);

    let actual_arena = plan.formatted_arena().unwrap();
    let mut expected_arena = String::new();
    expected_arena.push_str(
        r#"---------------------------------------------
[id: 164] relation: ScanRelation
	Relation: hash_testing
	[No children]
	Output_id: 064
		[id: 064] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 032] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 1, arena_type: Arena64 }), targets: None, position: 0, col_type: Integer, asterisk_source: None })]
				[id: 132] expression: Alias [name = product_code, child = Reference(Reference { parent: Some(NodeId { offset: 1, arena_type: Arena64 }), targets: None, position: 1, col_type: String, asterisk_source: None })]
				[id: 232] expression: Alias [name = product_units, child = Reference(Reference { parent: Some(NodeId { offset: 1, arena_type: Arena64 }), targets: None, position: 2, col_type: Boolean, asterisk_source: None })]
				[id: 332] expression: Alias [name = sys_op, child = Reference(Reference { parent: Some(NodeId { offset: 1, arena_type: Arena64 }), targets: None, position: 3, col_type: Unsigned, asterisk_source: None })]
				[id: 432] expression: Alias [name = bucket_id, child = Reference(Reference { parent: Some(NodeId { offset: 1, arena_type: Arena64 }), targets: None, position: 4, col_type: Unsigned, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 364] relation: GroupBy [is_final = false]
	Gr_cols:
		Gr_col: Reference(Reference { parent: Some(NodeId { offset: 3, arena_type: Arena64 }), targets: Some([0]), position: 1, col_type: String, asterisk_source: None })
	Children:
		Child_id = 164
	Output_id: 264
		[id: 264] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0, 1] }}) })]
			List:
				[id: 532] expression: Alias [name = identification_number, child = Reference(Reference { parent: Some(NodeId { offset: 3, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: Integer, asterisk_source: None })]
				[id: 632] expression: Alias [name = product_code, child = Reference(Reference { parent: Some(NodeId { offset: 3, arena_type: Arena64 }), targets: Some([0]), position: 1, col_type: String, asterisk_source: None })]
				[id: 732] expression: Alias [name = product_units, child = Reference(Reference { parent: Some(NodeId { offset: 3, arena_type: Arena64 }), targets: Some([0]), position: 2, col_type: Boolean, asterisk_source: None })]
				[id: 832] expression: Alias [name = sys_op, child = Reference(Reference { parent: Some(NodeId { offset: 3, arena_type: Arena64 }), targets: Some([0]), position: 3, col_type: Unsigned, asterisk_source: None })]
				[id: 932] expression: Alias [name = bucket_id, child = Reference(Reference { parent: Some(NodeId { offset: 3, arena_type: Arena64 }), targets: Some([0]), position: 4, col_type: Unsigned, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 764] relation: Projection
	Children:
		Child_id = 364
	Output_id: 664
		[id: 664] expression: Row [distribution = Some(Any)]
			List:
				[id: 1132] expression: Alias [name = column_596, child = Reference(Reference { parent: Some(NodeId { offset: 3, arena_type: Arena64 }), targets: Some([0]), position: 1, col_type: String, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 0136] relation: Motion [policy = Segment(MotionKey { targets: [Reference(0)] }), alias = None]
	Children:
		Child_id = 764
	Output_id: 1064
		[id: 1064] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 1332] expression: Alias [name = column_596, child = Reference(Reference { parent: Some(NodeId { offset: 0, arena_type: Arena136 }), targets: Some([0]), position: 0, col_type: String, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 964] relation: GroupBy [is_final = true]
	Gr_cols:
		Gr_col: Reference(Reference { parent: Some(NodeId { offset: 9, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: String, asterisk_source: None })
	Children:
		Child_id = 0136
	Output_id: 864
		[id: 864] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 1232] expression: Alias [name = column_596, child = Reference(Reference { parent: Some(NodeId { offset: 9, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: String, asterisk_source: None })]
---------------------------------------------
---------------------------------------------
[id: 564] relation: Projection
	Children:
		Child_id = 964
	Output_id: 464
		[id: 464] expression: Row [distribution = Some(Segment { keys: KeySet({Key { positions: [0] }}) })]
			List:
				[id: 1032] expression: Alias [name = product_code, child = Reference(Reference { parent: Some(NodeId { offset: 5, arena_type: Arena64 }), targets: Some([0]), position: 0, col_type: String, asterisk_source: None })]
---------------------------------------------
"#);

    assert_eq!(expected_arena, actual_arena);
}
