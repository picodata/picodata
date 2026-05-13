use crate::errors::{Entity, SbroadError};
use crate::ir::node::expression::{Expression, MutExpression};
use crate::ir::node::relational::{MutRelational, Relational};
use crate::ir::node::{
    ArenaType, BoolExpr, Constant, Join, Node, Node64, NodeId, Selection, SubQueryReference,
    UnaryExpr,
};
use crate::ir::operator::{Bool, Unary};
use crate::ir::tree::traversal::{LevelNode, PostOrderWithFilter, EXPR_CAPACITY};
use crate::ir::value::{Trivalent, TrivalentOrdering, Value};
use crate::ir::Plan;
use smol_str::format_smolstr;
use std::collections::{HashMap, HashSet};

type OldId = NodeId;
type NewId = NodeId;
type OldNewFoldingMap = HashMap<OldId, NewId>;

fn handle_gt(ordering: TrivalentOrdering) -> Value {
    match ordering {
        TrivalentOrdering::Greater => Value::from(true),
        TrivalentOrdering::Unknown => Value::Null,
        _ => Value::from(false),
    }
}

fn handle_gte(ordering: TrivalentOrdering) -> Value {
    match ordering {
        TrivalentOrdering::Greater | TrivalentOrdering::Equal => Value::from(true),
        TrivalentOrdering::Unknown => Value::Null,
        _ => Value::from(false),
    }
}

fn handle_lt(ordering: TrivalentOrdering) -> Value {
    match ordering {
        TrivalentOrdering::Less => Value::from(true),
        TrivalentOrdering::Unknown => Value::Null,
        _ => Value::from(false),
    }
}

fn handle_lte(ordering: TrivalentOrdering) -> Value {
    match ordering {
        TrivalentOrdering::Less | TrivalentOrdering::Equal => Value::from(true),
        TrivalentOrdering::Unknown => Value::Null,
        _ => Value::from(false),
    }
}

fn handle_ne(trivalent: Trivalent) -> Value {
    match trivalent {
        Trivalent::True => Value::from(false),
        Trivalent::Unknown => Value::Null,
        Trivalent::False => Value::from(true),
    }
}

fn handle_not(val: &Value) -> Result<Option<Value>, SbroadError> {
    let res = match val {
        Value::Boolean(bool_value) => Some(Value::Boolean(!*bool_value)),
        Value::Null => Some(Value::Null),
        _ => {
            return Err(SbroadError::Invalid(
                Entity::Plan,
                Some(format_smolstr!("{val} expected to be BOOLEAN or NULL")),
            ));
        }
    };

    Ok(res)
}

fn handle_isnull(val: &Value) -> Result<Option<Value>, SbroadError> {
    let res = match val {
        Value::Null => Some(Value::Boolean(true)),
        _ => Some(Value::Boolean(false)),
    };

    Ok(res)
}

fn collect_join_and_selection_nodes(plan: &Plan) -> Vec<NodeId> {
    plan.nodes
        .iter64()
        .enumerate()
        .filter(|(_, n)| matches!(n, Node64::Selection(_) | Node64::Join(_)))
        .map(|(i, _)| NodeId {
            offset: i.try_into().unwrap(),
            arena_type: ArenaType::Arena64,
        })
        .collect()
}

impl Plan {
    fn calculate_unary_op_res(&self, val: &Value, op: Unary) -> Result<Option<Value>, SbroadError> {
        let res = match op {
            Unary::Not => handle_not(val)?,
            Unary::IsNull => handle_isnull(val)?,
            _ => None,
        };

        Ok(res)
    }

    fn calculate_bool_op_res(
        &self,
        lhs: &Value,
        rhs: &Value,
        op: Bool,
    ) -> Result<Option<Value>, SbroadError> {
        let res = match op {
            Bool::Eq => Some(Value::from(lhs.eq(rhs))),
            Bool::Gt => {
                let ordering = lhs.partial_cmp(rhs).expect("type checked before");
                Some(handle_gt(ordering))
            }
            Bool::GtEq => {
                let ordering = lhs.partial_cmp(rhs).expect("type checked before");
                Some(handle_gte(ordering))
            }
            Bool::Lt => {
                let ordering = lhs.partial_cmp(rhs).expect("type checked before");
                Some(handle_lt(ordering))
            }
            Bool::LtEq => {
                let ordering = lhs.partial_cmp(rhs).expect("type checked before");
                Some(handle_lte(ordering))
            }
            Bool::NotEq => {
                let trivalent = lhs.eq(rhs);
                Some(handle_ne(trivalent))
            }
            Bool::And => match (lhs, rhs) {
                (Value::Boolean(val1), Value::Boolean(val2)) => {
                    Some(Value::Boolean(*val1 && *val2))
                }
                (Value::Boolean(false), Value::Null) | (Value::Null, Value::Boolean(false)) => {
                    Some(Value::Boolean(false))
                }
                (Value::Boolean(true), Value::Null) | (Value::Null, Value::Boolean(true)) => {
                    Some(Value::Null)
                }
                (Value::Null, Value::Null) => Some(Value::Null),
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Plan,
                        Some(format_smolstr!(
                            "{lhs} and {rhs} expected to be BOOLEAN or NULL"
                        )),
                    ));
                }
            },
            Bool::Or => match (lhs, rhs) {
                (Value::Boolean(val1), Value::Boolean(val2)) => {
                    Some(Value::Boolean(*val1 || *val2))
                }
                (Value::Boolean(false), Value::Null) | (Value::Null, Value::Boolean(false)) => {
                    Some(Value::Null)
                }
                (Value::Boolean(true), Value::Null) | (Value::Null, Value::Boolean(true)) => {
                    Some(Value::Boolean(true))
                }
                (Value::Null, Value::Null) => Some(Value::Null),
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Plan,
                        Some(format_smolstr!(
                            "{lhs} and {rhs} expected to be BOOLEAN or NULL"
                        )),
                    ));
                }
            },
            _ => None,
        };

        Ok(res)
    }

    fn fold_bool_op_to(
        &self,
        other: NodeId,
        op: Bool,
        bool_val: NodeId,
    ) -> Result<Option<NodeId>, SbroadError> {
        let Expression::Constant(Constant {
            value: Value::Boolean(const_val),
        }) = self.get_expression_node(bool_val)?
        else {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format_smolstr!("expected boolean value: {bool_val}")),
            ));
        };
        let other_is_ref = matches!(self.get_expression_node(other)?, Expression::Reference(_));
        let folded_to = match (op, *const_val) {
            // TODO: should fold `b = TRUE` -> `b` for bare references too,
            // once bucket_discovery learns to treat a bare boolean ref as
            // `ref = TRUE`. Without that, the fold breaks sharding-key
            // routing on bool columns.
            (Bool::Eq, true) | (Bool::NotEq, false)
                if self.is_trivalent(other)? && !other_is_ref =>
            {
                Some(other)
            }
            (Bool::And, true) => Some(other),
            (Bool::And, false) => Some(bool_val),
            (Bool::Or, true) => Some(bool_val),
            (Bool::Or, false) => Some(other),
            _ => None,
        };

        Ok(folded_to)
    }

    pub fn fold_boolean_tree(mut self) -> Result<Self, SbroadError> {
        let node_ids = collect_join_and_selection_nodes(&self);

        // Early return, there is no relational nodes with filters that benefit from folding.
        if node_ids.is_empty() {
            return Ok(self);
        }

        let mut const_map = OldNewFoldingMap::new();

        for id in node_ids {
            let rel_node = self.get_relation_node(id)?;
            let filter = match rel_node {
                Relational::Selection(Selection { filter, .. }) => *filter,
                Relational::Join(Join { condition, .. }) => *condition,
                _ => unreachable!("expected Selection or Join node"),
            };
            let dfs = PostOrderWithFilter::new(
                |node| self.nodes.expr_iter(node, false),
                |node| {
                    matches!(
                        self.get_node(node),
                        Ok(Node::Expression(Expression::Bool(_) | Expression::Unary(_)))
                    )
                },
                EXPR_CAPACITY,
            );
            let op_nodes = dfs.traverse_into_vec(filter);

            // TODO: there is a problem with folding other structures, like cast.
            // CAST(true AND a = 1 AS boolean) wouldn't be folded to CAST(a = 1 AS boolean)
            // because we don't reassign cast child
            for LevelNode(_, op_id) in op_nodes.iter() {
                match self.get_mut_expression_node(*op_id)? {
                    MutExpression::Bool(BoolExpr { left, op, right }) => {
                        *left = *const_map.get(left).unwrap_or(left);
                        *right = *const_map.get(right).unwrap_or(right);

                        let op = *op;
                        let left = *left;
                        let right = *right;

                        let left_node = self.get_expression_node(left)?;
                        let right_node = self.get_expression_node(right)?;

                        match (left_node, right_node) {
                            (
                                Expression::Constant(Constant { value: lhs }),
                                Expression::Constant(Constant { value: rhs }),
                            ) => {
                                let val = self.calculate_bool_op_res(lhs, rhs, op)?;
                                let Some(val) = val else {
                                    continue;
                                };
                                let const_id = self.nodes.next_id(ArenaType::Arena32);
                                const_map.insert(*op_id, const_id);
                                self.nodes.add_const(val);
                            }
                            (
                                Expression::Constant(Constant {
                                    value: Value::Boolean(_),
                                }),
                                _,
                            ) => {
                                if let Some(new_id) = self.fold_bool_op_to(right, op, left)? {
                                    const_map.insert(*op_id, new_id);
                                }
                            }
                            (
                                _,
                                Expression::Constant(Constant {
                                    value: Value::Boolean(_),
                                }),
                            ) => {
                                if let Some(new_id) = self.fold_bool_op_to(left, op, right)? {
                                    const_map.insert(*op_id, new_id);
                                }
                            }
                            _ => {}
                        }
                    }
                    MutExpression::Unary(UnaryExpr { op, child }) => {
                        *child = *const_map.get(child).unwrap_or(child);

                        let op = *op;
                        let child = *child;

                        let child_node = self.get_expression_node(child)?;
                        if let Expression::Constant(Constant { value }) = child_node {
                            let val = self.calculate_unary_op_res(value, op)?;
                            let Some(val) = val else {
                                continue;
                            };

                            let const_id = self.nodes.next_id(ArenaType::Arena32);
                            const_map.insert(*op_id, const_id);
                            self.nodes.add_const(val);
                        }
                    }
                    _ => unreachable!("expected Bool or Unary node"),
                }
            }

            if let Some(new_filter) = const_map.get(&filter) {
                match self.get_mut_relation_node(id)? {
                    MutRelational::Join(Join {
                        condition: mut_condition,
                        ..
                    }) => {
                        *mut_condition = *new_filter;
                    }
                    MutRelational::Selection(Selection {
                        filter: mut_filter, ..
                    }) => {
                        *mut_filter = *new_filter;
                    }
                    _ => unreachable!("expected Selection or Join node"),
                };
            }

            // If anything folded in this rel's filter, some `a IN (sq)` /
            // `EXISTS (sq)` branches may have been dropped (e.g.
            // `true OR a IN (sq)` -> `true`), leaving the subquery in the
            // node's `subqueries` field with no expression referencing it.
            // Such orphans would still be treated as dependencies by motion
            // planning and tree walks, so we prune them by retaining only
            // the subqueries actually reachable from the current filter.
            if !const_map.is_empty() {
                let (current_filter, has_subqueries) = match self.get_relation_node(id)? {
                    Relational::Selection(Selection {
                        filter, subqueries, ..
                    }) => (*filter, !subqueries.is_empty()),
                    Relational::Join(Join {
                        condition,
                        subqueries,
                        ..
                    }) => (*condition, !subqueries.is_empty()),
                    _ => unreachable!("expected Selection or Join node"),
                };
                if has_subqueries {
                    let mut referenced: HashSet<NodeId> = HashSet::new();
                    let sq_dfs = PostOrderWithFilter::new(
                        |node| self.nodes.expr_iter(node, false),
                        |node| {
                            matches!(
                                self.get_node(node),
                                Ok(Node::Expression(Expression::SubQueryReference(_)))
                            )
                        },
                        EXPR_CAPACITY,
                    );
                    for LevelNode(_, expr_id) in sq_dfs.traverse_into_vec(current_filter) {
                        if let Expression::SubQueryReference(SubQueryReference { rel_id, .. }) =
                            self.get_expression_node(expr_id)?
                        {
                            referenced.insert(*rel_id);
                        }
                    }
                    match self.get_mut_relation_node(id)? {
                        MutRelational::Join(Join { subqueries, .. })
                        | MutRelational::Selection(Selection { subqueries, .. }) => {
                            subqueries.retain(|sq_id| referenced.contains(sq_id));
                        }
                        _ => unreachable!("expected Selection or Join node"),
                    };
                }

                const_map.clear();
            }
        }

        Ok(self)
    }
}

#[cfg(test)]
#[cfg(feature = "mock")]
mod tests {
    use crate::executor::engine::mock::RouterRuntimeMock;
    use crate::executor::ExecutingQuery;
    use crate::ir::bucket::Buckets;

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
        use crate::ir::node::relational::Relational;
        use crate::ir::node::Node;
        use crate::ir::tree::traversal::{LevelNode, PostOrderWithFilter, REL_CAPACITY};

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
        use crate::ir::node::relational::Relational;
        use crate::ir::node::Node;
        use crate::ir::tree::traversal::{LevelNode, PostOrderWithFilter, REL_CAPACITY};

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
        use crate::ir::node::relational::Relational;
        use crate::ir::node::Node;
        use crate::ir::tree::traversal::{LevelNode, PostOrderWithFilter, REL_CAPACITY};

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
        use crate::ir::node::relational::Relational;
        use crate::ir::node::Node;
        use crate::ir::tree::traversal::{LevelNode, PostOrderWithFilter, REL_CAPACITY};

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
}
