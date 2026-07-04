use sql::executor::engine::mock::RouterRuntimeMock;
use sql::executor::ExecutingQuery;
use sql::ir::bucket::Buckets;
use sql::ExecutingQueryExt;

#[test]
fn test_bool_folding1() {
    let query = r#"explain (logical) SELECT * from t WHERE 1 = 1 and 2 > -4"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (true::bool)
        scan t
    ");

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn test_bool_folding2() {
    let query = r#"explain (logical) SELECT * from t WHERE 1 = 0"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (false::bool)
        scan t
    ");

    assert_eq!(Buckets::new_empty(), buckets);
}

#[test]
fn test_bool_folding3() {
    let query = r#"explain (logical) SELECT * from t WHERE 1 = 0 or 2 > -4"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (true::bool)
        scan t
    ");

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn test_bool_folding4() {
    let query = r#"explain (logical) SELECT * from t WHERE 1 = 0 and 5 >= 4 and 6 < -8"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (false::bool)
        scan t
    ");

    assert_eq!(Buckets::new_empty(), buckets);
}

#[test]
fn test_bool_folding5() {
    let query = r#"explain (logical) SELECT * from t WHERE 1 = 0 and 5 >= 4 and 6 < -8 UNION ALL SELECT * FROM t where 1 = 9"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    union all
      projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
        selection (false::bool)
          scan t
      projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
        selection (false::bool)
          scan t
    ");

    assert_eq!(Buckets::new_empty(), buckets);
}

#[test]
fn test_bool_folding6() {
    let query = r#"explain (logical) SELECT * from t JOIN t on true WHERE 1 = 1 AND 56 <= 500"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (true::bool)
        join on (true::bool)
          scan t
          motion [policy: full, program: ReshardIfNeeded]
            projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t.bucket_id::int -> bucket_id)
              scan t
    ");

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn test_bool_folding7() {
    let query = r#"explain (logical) SELECT * FROM t WHERE true and not (null is null)"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (false::bool)
        scan t
    ");

    assert_eq!(Buckets::new_empty(), buckets);
}

#[test]
fn test_bool_folding8() {
    let query = r#"explain (logical) SELECT * FROM t WHERE null = null"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (NULL::unknown)
        scan t
    ");

    assert_eq!(Buckets::new_empty(), buckets);
}

#[test]
fn test_bool_folding9() {
    let query = r#"explain (logical) SELECT * FROM t WHERE 1.0 = 1"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (true::bool)
        scan t
    ");

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn test_bool_folding10() {
    let query = r#"explain (logical) SELECT * FROM t WHERE 'inf' = 'inf'::double"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (true::bool)
        scan t
    ");

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn test_bool_folding11() {
    let query = r#"explain (logical) SELECT * FROM t WHERE 'nan' = 'nan'::double"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (false::bool)
        scan t
    ");

    assert_eq!(Buckets::new_empty(), buckets);
}

#[test]
fn test_bool_folding12() {
    let query = r#"explain (logical) SELECT * from t WHERE true = true = true = (a = b)"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (t.a::int = t.b::int)
        scan t
    ");

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn test_bool_folding13() {
    let query = r#"explain (logical) SELECT * from t WHERE (a = b) = true = true = true"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (t.a::int = t.b::int)
        scan t
    ");

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn test_bool_folding14() {
    let query = r#"explain (logical) SELECT * from t WHERE true AND a = b"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (t.a::int = t.b::int)
        scan t
    ");

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn test_bool_folding15() {
    let query = r#"explain (logical) SELECT * from t WHERE false AND a = b"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (false::bool)
        scan t
    ");

    assert_eq!(Buckets::new_empty(), buckets);
}

#[test]
fn test_bool_folding16() {
    let query = r#"explain (logical) SELECT * from t WHERE true OR a = b"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (true::bool)
        scan t
    ");

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn test_bool_folding17() {
    let query = r#"explain (logical) SELECT * from t WHERE false OR a = b"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (t.a::int = t.b::int)
        scan t
    ");

    assert_eq!(Buckets::All, buckets);
}

// TODO: ideally the selection should fold to just `product_units::bool`
// (without `= true::bool`). Blocked by the same downstream gap as the
// guard in `Plan::fold_bool_op_to`.
#[test]
fn test_bool_folding18() {
    let query = r#"explain (logical) SELECT * from hash_testing WHERE product_units = true"#;

    let coordinator = RouterRuntimeMock::new();
    let query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let _top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (hash_testing.identification_number::int -> identification_number, hash_testing.product_code::string -> product_code, hash_testing.product_units::bool -> product_units, hash_testing.sys_op::int -> sys_op)
      selection (hash_testing.product_units::bool = true::bool)
        scan hash_testing
    ");
}

#[test]
fn test_bool_folding19() {
    let query = r#"explain (logical) SELECT * from t WHERE (a = b) <> false"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (t.a::int = t.b::int)
        scan t
    ");

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn test_bool_folding20() {
    let query = r#"explain (logical) SELECT * from t WHERE false <> (a = b)"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    let top = plan.get_top().unwrap();
    let query_explain = plan.explain_logical().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();

    insta::assert_snapshot!(query_explain, @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (t.a::int = t.b::int)
        scan t
    ");

    assert_eq!(Buckets::All, buckets);
}

#[test]
fn test_bool_folding21() {
    use sql::ir::node::relational::Relational;
    use sql::ir::node::Node;
    use sql::ir::tree::traversal::{LevelNode, PostOrderWithFilter, REL_CAPACITY};

    let query = r#"explain (logical) SELECT * from t WHERE true OR a IN (SELECT a FROM t)"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let top = query.get_exec_plan().get_ir_plan().get_top().unwrap();
    let buckets = query.bucket_discovery(top).unwrap();
    assert_eq!(Buckets::All, buckets);

    let plan = query.get_exec_plan().get_ir_plan();
    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection (true::bool)
        scan t
    ");

    let dfs = PostOrderWithFilter::new(
        |x| plan.nodes.rel_iter(x),
        |node| {
            matches!(
                plan.get_node(node),
                Ok(Node::Relational(Relational::Selection(_)))
            )
        },
        REL_CAPACITY,
    );
    let LevelNode(_, sel_id) = dfs.traverse_into_iter(top).next().expect("selection node");
    let Relational::Selection(sel) = plan.get_relation_node(sel_id).unwrap() else {
        unreachable!()
    };
    assert!(
        sel.subqueries.is_empty(),
        "expected orphan subquery to be detached, got {:?}",
        sel.subqueries
    );
}

#[test]
fn test_bool_folding22() {
    use sql::ir::node::relational::Relational;
    use sql::ir::node::Node;
    use sql::ir::tree::traversal::{LevelNode, PostOrderWithFilter, REL_CAPACITY};

    // The outer AND does not fold to a constant, but the (`false AND ...`)
    // branch collapses and takes its subquery with it. The remaining
    // `IN (sq)` branch must keep its subquery.
    let query = r#"explain (logical) SELECT * from t
        WHERE (a IN (SELECT a FROM t))
          AND (((b IN (SELECT b FROM t)) AND false) OR (c = d))"#;

    let coordinator = RouterRuntimeMock::new();
    let query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d)
      selection ((t.a::int in ROW($0) and t.c::int = t.d::int))
        scan t
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t.a::int -> a)
            scan t
    ");
    let top = plan.get_top().unwrap();

    let dfs = PostOrderWithFilter::new(
        |x| plan.nodes.rel_iter(x),
        |node| {
            matches!(
                plan.get_node(node),
                Ok(Node::Relational(Relational::Selection(_)))
            )
        },
        REL_CAPACITY,
    );
    let LevelNode(_, sel_id) = dfs.traverse_into_iter(top).next().expect("selection node");
    let Relational::Selection(sel) = plan.get_relation_node(sel_id).unwrap() else {
        unreachable!()
    };
    // Only the surviving `a IN (SELECT a FROM t)` subquery must remain;
    // the `b IN (SELECT b FROM t)` was wiped with the collapsed `false AND ...`.
    assert_eq!(
        sel.subqueries.len(),
        1,
        "expected only the surviving subquery in dependencies, got {:?}",
        sel.subqueries
    );
}

#[test]
fn test_bool_folding23() {
    use sql::ir::node::relational::Relational;
    use sql::ir::node::Node;
    use sql::ir::tree::traversal::{LevelNode, PostOrderWithFilter, REL_CAPACITY};

    // Same orphan-subquery scenario, but for a Join condition.
    let query = r#"explain (logical)
        SELECT * FROM t JOIN t AS t2 ON true OR t.a IN (SELECT a FROM t)"#;

    let coordinator = RouterRuntimeMock::new();
    let query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.a::int -> a, t2.b::int -> b, t2.c::int -> c, t2.d::int -> d)
      join on (true::bool)
        scan t
        motion [policy: full, program: ReshardIfNeeded]
          projection (t2.a::int -> a, t2.b::int -> b, t2.c::int -> c, t2.d::int -> d, t2.bucket_id::int -> bucket_id)
            scan t -> t2
    ");
    let top = plan.get_top().unwrap();

    let dfs = PostOrderWithFilter::new(
        |x| plan.nodes.rel_iter(x),
        |node| {
            matches!(
                plan.get_node(node),
                Ok(Node::Relational(Relational::Join(_)))
            )
        },
        REL_CAPACITY,
    );
    let LevelNode(_, join_id) = dfs.traverse_into_iter(top).next().expect("join node");
    let Relational::Join(join) = plan.get_relation_node(join_id).unwrap() else {
        unreachable!()
    };
    assert!(
        join.subqueries.is_empty(),
        "expected orphan subquery to be detached from join, got {:?}",
        join.subqueries
    );
}

#[test]
fn test_bool_folding24() {
    use sql::ir::node::relational::Relational;
    use sql::ir::node::Node;
    use sql::ir::tree::traversal::{LevelNode, PostOrderWithFilter, REL_CAPACITY};

    // The outer AND in the join condition does not fold, but the
    // `false AND (t.b IN (sq2))` branch collapses and takes its
    // subquery with it. The `t.a IN (sq1)` branch must keep its
    // subquery in the Join's dependency list.
    let query = r#"explain (logical) SELECT * FROM t JOIN t AS t2
        ON (t.a IN (SELECT a FROM t))
          AND (((t.b IN (SELECT b FROM t)) AND false) OR (t.c = t.d))"#;

    let coordinator = RouterRuntimeMock::new();
    let query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r"
    projection (t.a::int -> a, t.b::int -> b, t.c::int -> c, t.d::int -> d, t2.a::int -> a, t2.b::int -> b, t2.c::int -> c, t2.d::int -> d)
      join on ((t.c::int = t.d::int and t.a::int in ROW($0)))
        scan t
        motion [policy: full, program: ReshardIfNeeded]
          projection (t2.a::int -> a, t2.b::int -> b, t2.c::int -> c, t2.d::int -> d, t2.bucket_id::int -> bucket_id)
            scan t -> t2
    subquery $0:
      motion [policy: full, program: ReshardIfNeeded]
        scan
          projection (t.a::int -> a)
            scan t
    ");
    let top = plan.get_top().unwrap();

    let dfs = PostOrderWithFilter::new(
        |x| plan.nodes.rel_iter(x),
        |node| {
            matches!(
                plan.get_node(node),
                Ok(Node::Relational(Relational::Join(_)))
            )
        },
        REL_CAPACITY,
    );
    let LevelNode(_, join_id) = dfs.traverse_into_iter(top).next().expect("join node");
    let Relational::Join(join) = plan.get_relation_node(join_id).unwrap() else {
        unreachable!()
    };
    assert_eq!(
        join.subqueries.len(),
        1,
        "expected only the surviving subquery in join dependencies, got {:?}",
        join.subqueries
    );
}

#[test]
fn test_bool_folding25() {
    let query = r#"explain (logical)
    select * from (values(1)) where
    ((false OR (((true AND (( 3450.55 / ( 8783.41 ) ) <= 7624.91)) OR false)
    AND (false AND (NOT false)))) <> false)"#;

    let coordinator = RouterRuntimeMock::new();
    let query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (unnamed_subquery."COLUMN_1"::int -> "COLUMN_1")
      selection (false::bool)
        scan unnamed_subquery
          motion [policy: full, program: ReshardIfNeeded]
            values
              value ROW(1::int)
    "#);
}

#[test]
fn test_bool_folding26() {
    let query = r#"explain (logical)
    select * from (values(1)) where
    ((true = ((false AND
    ((9540 <= (CAST('4219.95' AS DOUBLE) / (901.13 - CAST('6583.56' AS DOUBLE))))
    OR true)) AND (CAST('3535.89' AS DOUBLE) <= 8312))) = true)"#;

    let coordinator = RouterRuntimeMock::new();
    let query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (unnamed_subquery."COLUMN_1"::int -> "COLUMN_1")
      selection (false::bool)
        scan unnamed_subquery
          motion [policy: full, program: ReshardIfNeeded]
            values
              value ROW(1::int)
    "#);
}

#[test]
fn test_bool_folding27() {
    let query = r#"explain (logical)
    select * from (values(1)) where
    (true = (((4577.04 <= CAST('668.77' AS DOUBLE))
    OR (7742.01 = CAST((CAST(4718.40 AS INT) * (6813 * 3465)) AS DOUBLE)))
    AND (false AND (true OR false))))"#;

    let coordinator = RouterRuntimeMock::new();
    let query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (unnamed_subquery."COLUMN_1"::int -> "COLUMN_1")
      selection (false::bool)
        scan unnamed_subquery
          motion [policy: full, program: ReshardIfNeeded]
            values
              value ROW(1::int)
    "#);
}

#[test]
fn test_bool_folding28() {
    let query = r#"explain (logical)
    select * from (values(1)) where
    (false <> (false AND ((((1963.26 / CAST('4591.54' AS DOUBLE)) < CAST(8633.18 AS NUMERIC))
    OR true) AND true)))"#;

    let coordinator = RouterRuntimeMock::new();
    let query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
    let plan = query.get_exec_plan().get_ir_plan();
    insta::assert_snapshot!(plan.explain_logical().unwrap(), @r#"
    projection (unnamed_subquery."COLUMN_1"::int -> "COLUMN_1")
      selection (false::bool)
        scan unnamed_subquery
          motion [policy: full, program: ReshardIfNeeded]
            values
              value ROW(1::int)
    "#);
}
