use crate::{
    ir::node::NodeId,
    ir::{transformation::helpers::sql_to_optimized_ir, ArenaType},
};

#[test]
fn simple_select() {
    let query = r#"SELECT "product_code" FROM "hash_testing""#;
    let plan = sql_to_optimized_ir(query, vec![]);

    insta::assert_snapshot!(plan.formatted_arena().unwrap(), @r"
	---------------------------------------------
	[id: 164] relation: ScanRelation
		Relation: hash_testing
		[No children]
		Output:	[id: 064] expression: Row [distribution = Segment { keys: KeySet({Key { positions: [0, 1] }}) }]
			List:
			[id: 032] expression: Alias [name = identification_number]
				Child:
				[id: 096] expression: Reference
					Position: 0
					Column type: integer
			[id: 132] expression: Alias [name = product_code]
				Child:
				[id: 196] expression: Reference
					Position: 1
					Column type: string
			[id: 232] expression: Alias [name = product_units]
				Child:
				[id: 296] expression: Reference
					Position: 2
					Column type: boolean
			[id: 332] expression: Alias [name = sys_op]
				Child:
				[id: 396] expression: Reference
					Position: 3
					Column type: unsigned
			[id: 432] expression: Alias [name = bucket_id]
				Child:
				[id: 496] expression: Reference
					Position: 4
					Column type: unsigned
	---------------------------------------------
	---------------------------------------------
	[id: 364] relation: Projection
		Children:
			Child_id = 164
		Output:	[id: 264] expression: Row [distribution = Any]
			List:
			[id: 532] expression: Alias [name = product_code]
				Child:
				[id: 596] expression: Reference
					Alias: product_code
					Referenced table name (or alias): hash_testing
					target_id: 164
					Position: 1
					Column type: string
	---------------------------------------------
	");
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

    insta::assert_snapshot!(actual_arena, @r"
    ---------------------------------------------
    [id: 164] relation: ScanRelation
    	Relation: test_space
    	[No children]
    	Output:	[id: 064] expression: Row [distribution = Segment { keys: KeySet({Key { positions: [0] }}) }]
    		List:
    		[id: 032] expression: Alias [name = id]
    			Child:
    			[id: 096] expression: Reference
    				Position: 0
    				Column type: unsigned
    		[id: 132] expression: Alias [name = sysFrom]
    			Child:
    			[id: 196] expression: Reference
    				Position: 1
    				Column type: unsigned
    		[id: 232] expression: Alias [name = FIRST_NAME]
    			Child:
    			[id: 296] expression: Reference
    				Position: 2
    				Column type: string
    		[id: 332] expression: Alias [name = sys_op]
    			Child:
    			[id: 396] expression: Reference
    				Position: 3
    				Column type: unsigned
    		[id: 432] expression: Alias [name = bucket_id]
    			Child:
    			[id: 496] expression: Reference
    				Position: 4
    				Column type: unsigned
    ---------------------------------------------
    ---------------------------------------------
    [id: 364] relation: Projection
    	Children:
    		Child_id = 164
    	Output:	[id: 264] expression: Row [distribution = Segment { keys: KeySet({Key { positions: [0] }}) }]
    		List:
    		[id: 532] expression: Alias [name = id]
    			Child:
    			[id: 596] expression: Reference
    				Alias: id
    				Referenced table name (or alias): test_space
    				target_id: 164
    				Position: 0
    				Column type: unsigned
    ---------------------------------------------
    ---------------------------------------------
    [id: 564] relation: ScanSubQuery
    	Alias: t1
    	Children:
    		Child_id = 364
    	Output:	[id: 464] expression: Row [distribution = Segment { keys: KeySet({Key { positions: [0] }}) }]
    		List:
    		[id: 632] expression: Alias [name = id]
    			Child:
    			[id: 696] expression: Reference
    				Alias: id
    				Referenced table name (or alias): test_space
    				target_id: 364
    				Position: 0
    				Column type: unsigned
    ---------------------------------------------
    ---------------------------------------------
    [id: 764] relation: ScanRelation
    	Relation: hash_testing
    	[No children]
    	Output:	[id: 664] expression: Row [distribution = Segment { keys: KeySet({Key { positions: [0, 1] }}) }]
    		List:
    		[id: 732] expression: Alias [name = identification_number]
    			Child:
    			[id: 796] expression: Reference
    				Position: 0
    				Column type: integer
    		[id: 832] expression: Alias [name = product_code]
    			Child:
    			[id: 896] expression: Reference
    				Position: 1
    				Column type: string
    		[id: 932] expression: Alias [name = product_units]
    			Child:
    			[id: 996] expression: Reference
    				Position: 2
    				Column type: boolean
    		[id: 1032] expression: Alias [name = sys_op]
    			Child:
    			[id: 1096] expression: Reference
    				Position: 3
    				Column type: unsigned
    		[id: 1132] expression: Alias [name = bucket_id]
    			Child:
    			[id: 1196] expression: Reference
    				Position: 4
    				Column type: unsigned
    ---------------------------------------------
    ---------------------------------------------
    [id: 964] relation: Projection
    	Children:
    		Child_id = 764
    	Output:	[id: 864] expression: Row [distribution = Any]
    		List:
    		[id: 1232] expression: Alias [name = identification_number]
    			Child:
    			[id: 1296] expression: Reference
    				Alias: identification_number
    				Referenced table name (or alias): hash_testing
    				target_id: 764
    				Position: 0
    				Column type: integer
    ---------------------------------------------
    ---------------------------------------------
    [id: 1164] relation: ScanSubQuery
    	Alias: t2
    	Children:
    		Child_id = 964
    	Output:	[id: 1064] expression: Row [distribution = Any]
    		List:
    		[id: 1332] expression: Alias [name = identification_number]
    			Child:
    			[id: 1396] expression: Reference
    				Alias: identification_number
    				Referenced table name (or alias): hash_testing
    				target_id: 964
    				Position: 0
    				Column type: integer
    ---------------------------------------------
    ---------------------------------------------
    [id: 0136] relation: Motion [policy = Segment(MotionKey { targets: [Reference(0)] }), alias = t2]
    	Children:
    		Child_id = 1164
    	Output:	[id: 1664] expression: Row [distribution = Segment { keys: KeySet({Key { positions: [0] }}) }]
    		List:
    		[id: 1832] expression: Alias [name = identification_number]
    			Child:
    			[id: 1996] expression: Reference
    				Alias: identification_number
    				Referenced table name (or alias): t2
    				target_id: 1164
    				Position: 0
    				Column type: integer
    ---------------------------------------------
    ---------------------------------------------
    [id: 1364] relation: InnerJoin
    	Condition:
    		[id: 1432] expression: Bool [op: =]
    			Left child
    			[id: 1496] expression: Reference
    				Alias: id
    				Referenced table name (or alias): t1
    				target_id: 564
    				Position: 0
    				Column type: unsigned
    			Right child
    			[id: 1596] expression: Reference
    				Alias: identification_number
    				Referenced table name (or alias): t2
    				target_id: 0136
    				Position: 0
    				Column type: integer
    	Children:
    		Child_id = 564
    		Child_id = 0136
    	Output:	[id: 1264] expression: Row [distribution = Segment { keys: KeySet({Key { positions: [1] }, Key { positions: [0] }}) }]
    		List:
    		[id: 1532] expression: Alias [name = id]
    			Child:
    			[id: 1696] expression: Reference
    				Alias: id
    				Referenced table name (or alias): t1
    				target_id: 564
    				Position: 0
    				Column type: unsigned
    		[id: 1632] expression: Alias [name = identification_number]
    			Child:
    			[id: 1796] expression: Reference
    				Alias: identification_number
    				Referenced table name (or alias): t2
    				target_id: 0136
    				Position: 0
    				Column type: integer
    ---------------------------------------------
    ---------------------------------------------
    [id: 1564] relation: Projection
    	Children:
    		Child_id = 1364
    	Output:	[id: 1464] expression: Row [distribution = Segment { keys: KeySet({Key { positions: [0] }}) }]
    		List:
    		[id: 1732] expression: Alias [name = id]
    			Child:
    			[id: 1896] expression: Reference
    				Alias: id
    				Referenced table name (or alias): t1
    				target_id: 1364
    				Position: 0
    				Column type: unsigned
    ---------------------------------------------
    ");
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

    insta::assert_snapshot!(actual_arena_subtree, @r"
    ---------------------------------------------
    [id: 764] relation: ScanRelation
    	Relation: hash_testing
    	[No children]
    	Output:	[id: 664] expression: Row [distribution = Segment { keys: KeySet({Key { positions: [0, 1] }}) }]
    		List:
    		[id: 732] expression: Alias [name = identification_number]
    			Child:
    			[id: 796] expression: Reference
    				Position: 0
    				Column type: integer
    		[id: 832] expression: Alias [name = product_code]
    			Child:
    			[id: 896] expression: Reference
    				Position: 1
    				Column type: string
    		[id: 932] expression: Alias [name = product_units]
    			Child:
    			[id: 996] expression: Reference
    				Position: 2
    				Column type: boolean
    		[id: 1032] expression: Alias [name = sys_op]
    			Child:
    			[id: 1096] expression: Reference
    				Position: 3
    				Column type: unsigned
    		[id: 1132] expression: Alias [name = bucket_id]
    			Child:
    			[id: 1196] expression: Reference
    				Position: 4
    				Column type: unsigned
    ---------------------------------------------
    ---------------------------------------------
    [id: 964] relation: Projection
    	Children:
    		Child_id = 764
    	Output:	[id: 864] expression: Row [distribution = Any]
    		List:
    		[id: 1232] expression: Alias [name = identification_number]
    			Child:
    			[id: 1296] expression: Reference
    				Alias: identification_number
    				Referenced table name (or alias): hash_testing
    				target_id: 764
    				Position: 0
    				Column type: integer
    ---------------------------------------------
    ---------------------------------------------
    [id: 1164] relation: ScanSubQuery
    	Alias: t2
    	Children:
    		Child_id = 964
    	Output:	[id: 1064] expression: Row [distribution = Any]
    		List:
    		[id: 1332] expression: Alias [name = identification_number]
    			Child:
    			[id: 1396] expression: Reference
    				Alias: identification_number
    				Referenced table name (or alias): hash_testing
    				target_id: 964
    				Position: 0
    				Column type: integer
    ---------------------------------------------
    ---------------------------------------------
    [id: 0136] relation: Motion [policy = Segment(MotionKey { targets: [Reference(0)] }), alias = t2]
    	Children:
    		Child_id = 1164
    	Output:	[id: 1664] expression: Row [distribution = Segment { keys: KeySet({Key { positions: [0] }}) }]
    		List:
    		[id: 1832] expression: Alias [name = identification_number]
    			Child:
    			[id: 1996] expression: Reference
    				Alias: identification_number
    				Referenced table name (or alias): t2
    				target_id: 1164
    				Position: 0
    				Column type: integer
    ---------------------------------------------
    "
    );
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
	Output:	[id: 064] expression: Row [distribution = Segment { keys: KeySet({Key { positions: [0, 1] }}) }]
		List:
		[id: 032] expression: Alias [name = identification_number]
			Child:
			[id: 096] expression: Reference
				Position: 0
				Column type: integer
		[id: 132] expression: Alias [name = product_code]
			Child:
			[id: 196] expression: Reference
				Position: 1
				Column type: string
		[id: 232] expression: Alias [name = product_units]
			Child:
			[id: 296] expression: Reference
				Position: 2
				Column type: boolean
		[id: 332] expression: Alias [name = sys_op]
			Child:
			[id: 396] expression: Reference
				Position: 3
				Column type: unsigned
		[id: 432] expression: Alias [name = bucket_id]
			Child:
			[id: 496] expression: Reference
				Position: 4
				Column type: unsigned
---------------------------------------------
---------------------------------------------
[id: 364] relation: GroupBy
	Gr_cols:
		[id: 596] expression: Reference
			Alias: product_code
			Referenced table name (or alias): hash_testing
			target_id: 164
			Position: 1
			Column type: string
	Children:
		Child_id = 164
	Output:	[id: 264] expression: Row [distribution = Segment { keys: KeySet({Key { positions: [0, 1] }}) }]
		List:
		[id: 532] expression: Alias [name = identification_number]
			Child:
			[id: 696] expression: Reference
				Alias: identification_number
				Referenced table name (or alias): hash_testing
				target_id: 164
				Position: 0
				Column type: integer
		[id: 632] expression: Alias [name = product_code]
			Child:
			[id: 796] expression: Reference
				Alias: product_code
				Referenced table name (or alias): hash_testing
				target_id: 164
				Position: 1
				Column type: string
		[id: 732] expression: Alias [name = product_units]
			Child:
			[id: 896] expression: Reference
				Alias: product_units
				Referenced table name (or alias): hash_testing
				target_id: 164
				Position: 2
				Column type: boolean
		[id: 832] expression: Alias [name = sys_op]
			Child:
			[id: 996] expression: Reference
				Alias: sys_op
				Referenced table name (or alias): hash_testing
				target_id: 164
				Position: 3
				Column type: unsigned
		[id: 932] expression: Alias [name = bucket_id]
			Child:
			[id: 1096] expression: Reference
				Alias: bucket_id
				Referenced table name (or alias): hash_testing
				target_id: 164
				Position: 4
				Column type: unsigned
---------------------------------------------
---------------------------------------------
[id: 764] relation: Projection
	Children:
		Child_id = 364
	Output:	[id: 664] expression: Row [distribution = Any]
		List:
		[id: 1132] expression: Alias [name = gr_expr_1]
			Child:
			[id: 1296] expression: Reference
				Alias: product_code
				Referenced table name (or alias): hash_testing
				target_id: 364
				Position: 1
				Column type: string
---------------------------------------------
---------------------------------------------
[id: 0136] relation: Motion [policy = Full, alias = None]
	Children:
		Child_id = 764
	Output:	[id: 1064] expression: Row [distribution = Global]
		List:
		[id: 1332] expression: Alias [name = gr_expr_1]
			Child:
			[id: 1696] expression: Reference
				Alias: gr_expr_1
				Referenced table name (or alias): hash_testing
				target_id: 764
				Position: 0
				Column type: string
---------------------------------------------
---------------------------------------------
[id: 964] relation: GroupBy
	Gr_cols:
		[id: 1396] expression: Reference
			Alias: gr_expr_1
			target_id: 0136
			Position: 0
			Column type: string
	Children:
		Child_id = 0136
	Output:	[id: 864] expression: Row [distribution = Single]
		List:
		[id: 1232] expression: Alias [name = gr_expr_1]
			Child:
			[id: 1496] expression: Reference
				Alias: gr_expr_1
				target_id: 0136
				Position: 0
				Column type: string
---------------------------------------------
---------------------------------------------
[id: 564] relation: Projection
	Children:
		Child_id = 964
	Output:	[id: 464] expression: Row [distribution = Single]
		List:
		[id: 1032] expression: Alias [name = product_code]
			Child:
			[id: 1596] expression: Reference
				Alias: gr_expr_1
				target_id: 964
				Position: 0
				Column type: string
---------------------------------------------
"#);

    assert_eq!(expected_arena, actual_arena);
}
