use crate::ir::node::{
    BlockEntries, BlockEntriesMut, BlockEntryKind, BlockStatement, Node136, Node232, Node32,
    Node64, Node96,
};

#[test]
fn test_node_size() {
    assert_eq!(std::mem::size_of::<Node32>(), 40);
    assert_eq!(std::mem::size_of::<Node64>(), 72);
    assert_eq!(std::mem::size_of::<Node96>(), 88);
    assert_eq!(std::mem::size_of::<Node136>(), 112);
    assert_eq!(std::mem::size_of::<Node232>(), 208);
}

/// A block whose IFs nest two levels deep.
fn nested_block() -> Vec<BlockStatement<u32>> {
    vec![
        BlockStatement::Let {
            var: ":v".into(),
            query: 0,
            is_used: true,
        },
        BlockStatement::If {
            cond: 1,
            body: vec![
                BlockStatement::ReturnQuery(2),
                BlockStatement::If {
                    cond: 3,
                    body: vec![BlockStatement::ReturnQuery(4), BlockStatement::Query(5)],
                },
                BlockStatement::Query(6),
            ],
        },
        BlockStatement::Query(7),
    ]
}

#[test]
fn block_entries_walk_nested_ifs_in_execution_order() {
    let stmts = nested_block();

    // Every query, once, in the order a VDBE would run them: an IF condition
    // then everything its body holds, innermost first.
    let queries: Vec<u32> = BlockEntries::new(&stmts).map(|e| *e.query).collect();
    assert_eq!(vec![0, 1, 2, 3, 4, 5, 6, 7], queries);

    // A statement reports what it does however deeply it is buried, which is
    // what the DQL-before-DML check and `n_res_column` rely on.
    let kinds: Vec<BlockEntryKind> = BlockEntries::new(&stmts).map(|e| e.location.kind).collect();
    assert_eq!(
        vec![
            BlockEntryKind::Let {
                var: ":v".into(),
                is_used: true,
            },
            BlockEntryKind::IfCondition,
            BlockEntryKind::ReturnQuery,
            BlockEntryKind::IfCondition,
            BlockEntryKind::ReturnQuery,
            BlockEntryKind::Query,
            BlockEntryKind::Query,
            BlockEntryKind::Query,
        ],
        kinds
    );

    // A location names a statement by the same dotted path EXPLAIN puts in its
    // stage header, and says what the statement is at any depth -- so an error
    // and the EXPLAIN of the same block point at a statement the same way.
    let locations: Vec<String> = BlockEntries::new(&stmts)
        .map(|e| e.location.to_string())
        .collect();
    assert_eq!(
        vec![
            r#"statement 1 (LET "v")"#,
            "statement 2.1 (IF condition)",
            "statement 2.2 (RETURN QUERY)",
            "statement 2.3.1 (IF condition)",
            "statement 2.3.2 (RETURN QUERY)",
            "statement 2.3.3 (DML)",
            "statement 2.4 (DML)",
            "statement 3 (DML)",
        ],
        locations
    );
}

/// EXPLAIN numbers an IF's condition `.1` and its body from `.2`, leaving the
/// IF's own slot free to prefix everything it holds. The block here is
/// `LET; IF (rq, IF (rq, dml), dml); dml`.
#[test]
fn explain_path_numbers_nested_ifs_by_their_position() {
    let stmts = nested_block();

    let paths: Vec<Vec<usize>> = BlockEntries::new(&stmts)
        .map(|e| e.location.explain_path())
        .collect();
    assert_eq!(
        vec![
            vec![1],       // LET
            vec![2, 1],    // IF's condition
            vec![2, 2],    // its first body item
            vec![2, 3, 1], // second body item is an IF: its condition
            vec![2, 3, 2], // that IF's own body
            vec![2, 3, 3],
            vec![2, 4], // back out to the third body item
            vec![3],    // and on to the next top-level statement
        ],
        paths
    );

    // One `If body: ` prefix per level is built from this.
    let depths: Vec<usize> = BlockEntries::new(&stmts)
        .map(|e| e.location.if_body_depth())
        .collect();
    assert_eq!(vec![0, 0, 1, 1, 2, 2, 1, 0], depths);
}

#[test]
fn block_entries_mut_reaches_every_nested_query() {
    let mut stmts = nested_block();
    for entry in BlockEntriesMut::new(&mut stmts) {
        *entry.query += 100;
    }
    let queries: Vec<u32> = BlockEntries::new(&stmts).map(|e| *e.query).collect();
    assert_eq!(vec![100, 101, 102, 103, 104, 105, 106, 107], queries);
}
