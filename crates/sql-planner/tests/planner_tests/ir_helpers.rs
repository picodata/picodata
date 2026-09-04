use sql::{helpers::sql_to_optimized_ir, ir::node::ArenaType, ir::node::NodeId};

#[test]
fn simple_select() {
    let query = r#"SELECT "product_code" FROM "hash_testing""#;
    let plan = sql_to_optimized_ir(query, vec![]);

    insta::assert_snapshot!(plan.formatted_arena().unwrap(), @"
    ---------------------------------------------
    [id: 096] relation: ScanRelation
    	Relation: hash_testing
    	Distribution: Segment { keys: KeySet({Key { positions: [0, 1] }}) }
    	[No children]
    	Columns: [identification_number: int, product_code: string, product_units: bool, sys_op: int, bucket_id: int (system)]
    ---------------------------------------------
    ---------------------------------------------
    [id: 296] relation: Projection
    	Distribution: Any
    	Children:
    		Child_id = 096
    	Output:	[id: 064] expression: Row
    		List:
    		[id: 032] expression: Alias [name = product_code]
    			Child:
    			[id: 196] expression: Reference
    				Alias: product_code
    				Referenced table name (or alias): hash_testing
    				target_id: 096
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

    insta::assert_snapshot!(actual_arena, @"
    ---------------------------------------------
    [id: 096] relation: ScanRelation
    	Relation: test_space
    	Distribution: Segment { keys: KeySet({Key { positions: [0] }}) }
    	[No children]
    	Columns: [id: int, sysFrom: int, FIRST_NAME: string, sys_op: int, bucket_id: int (system)]
    ---------------------------------------------
    ---------------------------------------------
    [id: 296] relation: Projection
    	Distribution: Segment { keys: KeySet({Key { positions: [0] }}) }
    	Children:
    		Child_id = 096
    	Output:	[id: 064] expression: Row
    		List:
    		[id: 032] expression: Alias [name = id]
    			Child:
    			[id: 196] expression: Reference
    				Alias: id
    				Referenced table name (or alias): test_space
    				target_id: 096
    				Position: 0
    				Column type: int
    ---------------------------------------------
    ---------------------------------------------
    [id: 164] relation: ScanSubQuery
    	Alias: t1
    	Distribution: Segment { keys: KeySet({Key { positions: [0] }}) }
    	Children:
    		Child_id = 296
    	Columns: [id: int]
    ---------------------------------------------
    ---------------------------------------------
    [id: 396] relation: ScanRelation
    	Relation: hash_testing
    	Distribution: Segment { keys: KeySet({Key { positions: [0, 1] }}) }
    	[No children]
    	Columns: [identification_number: int, product_code: string, product_units: bool, sys_op: int, bucket_id: int (system)]
    ---------------------------------------------
    ---------------------------------------------
    [id: 596] relation: Projection
    	Distribution: Any
    	Children:
    		Child_id = 396
    	Output:	[id: 264] expression: Row
    		List:
    		[id: 132] expression: Alias [name = identification_number]
    			Child:
    			[id: 496] expression: Reference
    				Alias: identification_number
    				Referenced table name (or alias): hash_testing
    				target_id: 396
    				Position: 0
    				Column type: int
    ---------------------------------------------
    ---------------------------------------------
    [id: 364] relation: ScanSubQuery
    	Alias: t2
    	Distribution: Any
    	Children:
    		Child_id = 596
    	Columns: [identification_number: int]
    ---------------------------------------------
    ---------------------------------------------
    [id: 0136] relation: Motion [policy = Segment(MotionKey { targets: [Reference(0)] }), alias = t2]
    	Distribution: Segment { keys: KeySet({Key { positions: [0] }}) }
    	Children:
    		Child_id = 364
    	Output:	[id: 564] expression: Row
    		List:
    		[id: 432] expression: Alias [name = identification_number]
    			Child:
    			[id: 1196] expression: Reference
    				Alias: identification_number
    				Referenced table name (or alias): t2
    				target_id: 364
    				Position: 0
    				Column type: int
    ---------------------------------------------
    ---------------------------------------------
    [id: 896] relation: InnerJoin
    	Condition:
    		[id: 232] expression: Bool [op: =]
    			Left child
    			[id: 696] expression: Reference
    				Alias: id
    				Referenced table name (or alias): t1
    				target_id: 164
    				Position: 0
    				Column type: int
    			Right child
    			[id: 796] expression: Reference
    				Alias: identification_number
    				Referenced table name (or alias): t2
    				target_id: 0136
    				Position: 0
    				Column type: int
    	Distribution: Segment { keys: KeySet({Key { positions: [1] }, Key { positions: [0] }}) }
    	Children:
    		Child_id = 164
    		Child_id = 0136
    	Columns: [id: int, identification_number: int]
    ---------------------------------------------
    ---------------------------------------------
    [id: 1096] relation: Projection
    	Distribution: Segment { keys: KeySet({Key { positions: [0] }}) }
    	Children:
    		Child_id = 896
    	Output:	[id: 464] expression: Row
    		List:
    		[id: 332] expression: Alias [name = id]
    			Child:
    			[id: 996] expression: Reference
    				Alias: id
    				Referenced table name (or alias): t1
    				target_id: 896
    				Position: 0
    				Column type: int
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

    insta::assert_snapshot!(actual_arena_subtree, @"
    ---------------------------------------------
    [id: 396] relation: ScanRelation
    	Relation: hash_testing
    	Distribution: Segment { keys: KeySet({Key { positions: [0, 1] }}) }
    	[No children]
    	Columns: [identification_number: int, product_code: string, product_units: bool, sys_op: int, bucket_id: int (system)]
    ---------------------------------------------
    ---------------------------------------------
    [id: 596] relation: Projection
    	Distribution: Any
    	Children:
    		Child_id = 396
    	Output:	[id: 264] expression: Row
    		List:
    		[id: 132] expression: Alias [name = identification_number]
    			Child:
    			[id: 496] expression: Reference
    				Alias: identification_number
    				Referenced table name (or alias): hash_testing
    				target_id: 396
    				Position: 0
    				Column type: int
    ---------------------------------------------
    ---------------------------------------------
    [id: 364] relation: ScanSubQuery
    	Alias: t2
    	Distribution: Any
    	Children:
    		Child_id = 596
    	Columns: [identification_number: int]
    ---------------------------------------------
    ---------------------------------------------
    [id: 0136] relation: Motion [policy = Segment(MotionKey { targets: [Reference(0)] }), alias = t2]
    	Distribution: Segment { keys: KeySet({Key { positions: [0] }}) }
    	Children:
    		Child_id = 364
    	Output:	[id: 564] expression: Row
    		List:
    		[id: 432] expression: Alias [name = identification_number]
    			Child:
    			[id: 1196] expression: Reference
    				Alias: identification_number
    				Referenced table name (or alias): t2
    				target_id: 364
    				Position: 0
    				Column type: int
    ---------------------------------------------
    "
    );
}

#[test]
fn simple_aggregation_with_group_by() {
    let query = r#"SELECT "product_code" FROM "hash_testing" GROUP BY "product_code""#;
    let plan = sql_to_optimized_ir(query, vec![]);

    insta::assert_snapshot!(plan.formatted_arena().unwrap(), @"
    ---------------------------------------------
    [id: 096] relation: ScanRelation
    	Relation: hash_testing
    	Distribution: Segment { keys: KeySet({Key { positions: [0, 1] }}) }
    	[No children]
    	Columns: [identification_number: int, product_code: string, product_units: bool, sys_op: int, bucket_id: int (system)]
    ---------------------------------------------
    ---------------------------------------------
    [id: 296] relation: GroupBy
    	Gr_cols:
    		[id: 196] expression: Reference
    			Alias: product_code
    			Referenced table name (or alias): hash_testing
    			target_id: 096
    			Position: 1
    			Column type: string
    	Distribution: Segment { keys: KeySet({Key { positions: [0, 1] }}) }
    	Children:
    		Child_id = 096
    	Columns: [identification_number: int, product_code: string, product_units: bool, sys_op: int, bucket_id: int (system)]
    ---------------------------------------------
    ---------------------------------------------
    [id: 696] relation: Projection
    	Distribution: Any
    	Children:
    	Output:	[id: 164] expression: Row
    		List:
    		[id: 132] expression: Alias [name = gr_expr_1]
    			Child:
    			[id: 596] expression: Reference
    				Alias: product_code
    				Referenced table name (or alias): hash_testing
    				target_id: 296
    				Position: 1
    				Column type: string
    ---------------------------------------------
    ---------------------------------------------
    [id: 0136] relation: Motion [policy = Full, alias = None]
    	Distribution: Global
    	Children:
    		Child_id = 696
    	Output:	[id: 264] expression: Row
    		List:
    		[id: 232] expression: Alias [name = gr_expr_1]
    			Child:
    			[id: 1096] expression: Reference
    				Alias: gr_expr_1
    				Referenced table name (or alias): hash_testing
    				target_id: 696
    				Position: 0
    				Column type: string
    ---------------------------------------------
    ---------------------------------------------
    [id: 896] relation: GroupBy
    	Gr_cols:
    		[id: 796] expression: Reference
    			Alias: gr_expr_1
    			target_id: 0136
    			Position: 0
    			Column type: string
    	Distribution: Single
    	Children:
    		Child_id = 0136
    	Columns: [gr_expr_1: string]
    ---------------------------------------------
    ---------------------------------------------
    [id: 496] relation: Projection
    	Distribution: Single
    	Children:
    	Output:	[id: 064] expression: Row
    		List:
    		[id: 032] expression: Alias [name = product_code]
    			Child:
    			[id: 996] expression: Reference
    				Alias: gr_expr_1
    				target_id: 896
    				Position: 0
    				Column type: string
    ---------------------------------------------
    ");
}
