use crate::errors::{Entity, SbroadError};
use crate::ir::node::expression::{Expression, MutExpression};
use crate::ir::node::relational::{MutRelational, Relational};
use crate::ir::node::{ArenaType, BoolExpr, Constant, Join, Node, NodeId, Selection, UnaryExpr};
use crate::ir::operator::{Bool, Unary};
use crate::ir::tree::traversal::{LevelNode, PostOrderWithFilter, EXPR_CAPACITY, REL_CAPACITY};
use crate::ir::tree::Snapshot;
use crate::ir::value::{Trivalent, TrivalentOrdering, Value};
use crate::ir::Plan;
use smol_str::format_smolstr;
use std::collections::HashMap;

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

    pub fn fold_boolean_tree(mut self) -> Result<Self, SbroadError> {
        let selection_filter = |id: NodeId| -> bool {
            matches!(
                self.get_node(id),
                Ok(Node::Relational(
                    Relational::Selection(_) | Relational::Join(_)
                ))
            )
        };

        let mut dfs = PostOrderWithFilter::with_capacity(
            |node_id| self.exec_plan_subtree_iter(node_id, Snapshot::Latest),
            REL_CAPACITY / 3,
            Box::new(selection_filter),
        );
        dfs.populate_nodes(self.get_top()?);
        let nodes = dfs.take_nodes();
        drop(dfs);

        let mut const_map = OldNewFoldingMap::new();

        for LevelNode(_, id) in nodes.iter() {
            let rel_node = self.get_relation_node(*id)?;
            let filter = match rel_node {
                Relational::Selection(Selection { filter, .. }) => *filter,
                Relational::Join(Join { condition, .. }) => *condition,
                _ => unreachable!("expected Selection or Join node"),
            };

            let bool_filter = |id: NodeId| -> bool {
                matches!(
                    self.get_node(id),
                    Ok(Node::Expression(Expression::Bool(_) | Expression::Unary(_)))
                )
            };
            let mut dfs = PostOrderWithFilter::with_capacity(
                |node_id| self.nodes.expr_iter(node_id, false),
                EXPR_CAPACITY,
                Box::new(bool_filter),
            );
            dfs.populate_nodes(filter);
            let op_nodes = dfs.take_nodes();
            drop(dfs);

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

                        if let (
                            Expression::Constant(Constant { value: lhs }),
                            Expression::Constant(Constant { value: rhs }),
                        ) = (left_node, right_node)
                        {
                            let val = self.calculate_bool_op_res(lhs, rhs, op)?;
                            let Some(val) = val else {
                                continue;
                            };

                            let const_id = self.nodes.next_id(ArenaType::Arena32);
                            const_map.insert(*op_id, const_id);
                            self.nodes.add_const(val);
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
                match self.get_mut_relation_node(*id)? {
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
        }

        Ok(self)
    }
}

#[cfg(test)]
#[cfg(feature = "mock")]
mod tests {
    use crate::executor::bucket::Buckets;
    use crate::executor::engine::mock::RouterRuntimeMock;
    use crate::executor::ExecutingQuery;

    #[test]
    fn test_bool_folding1() {
        let query = r#"SELECT * from t WHERE 1 = 1 and 2 > -4"#;

        let coordinator = RouterRuntimeMock::new();
        let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        let query_explain = plan.as_explain().unwrap();
        let buckets = query.bucket_discovery(top).unwrap();

        insta::assert_snapshot!(query_explain, @r#"
        projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
            selection true::bool
                scan "t"
        execution options:
            sql_vdbe_opcode_max = 45000
            sql_motion_row_max = 5000
        "#);

        assert_eq!(Buckets::All, buckets);
    }

    #[test]
    fn test_bool_folding2() {
        let query = r#"SELECT * from t WHERE 1 = 0"#;

        let coordinator = RouterRuntimeMock::new();
        let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        let query_explain = plan.as_explain().unwrap();
        let buckets = query.bucket_discovery(top).unwrap();

        insta::assert_snapshot!(query_explain, @r#"
        projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
            selection false::bool
                scan "t"
        execution options:
            sql_vdbe_opcode_max = 45000
            sql_motion_row_max = 5000
        "#);

        assert_eq!(Buckets::new_empty(), buckets);
    }

    #[test]
    fn test_bool_folding3() {
        let query = r#"SELECT * from t WHERE 1 = 0 or 2 > -4"#;

        let coordinator = RouterRuntimeMock::new();
        let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        let query_explain = plan.as_explain().unwrap();
        let buckets = query.bucket_discovery(top).unwrap();

        insta::assert_snapshot!(query_explain, @r#"
        projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
            selection true::bool
                scan "t"
        execution options:
            sql_vdbe_opcode_max = 45000
            sql_motion_row_max = 5000
        "#);

        assert_eq!(Buckets::All, buckets);
    }

    #[test]
    fn test_bool_folding4() {
        let query = r#"SELECT * from t WHERE 1 = 0 and 5 >= 4 and 6 < -8"#;

        let coordinator = RouterRuntimeMock::new();
        let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        let query_explain = plan.as_explain().unwrap();
        let buckets = query.bucket_discovery(top).unwrap();

        insta::assert_snapshot!(query_explain, @r#"
        projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
            selection false::bool
                scan "t"
        execution options:
            sql_vdbe_opcode_max = 45000
            sql_motion_row_max = 5000
        "#);

        assert_eq!(Buckets::new_empty(), buckets);
    }

    #[test]
    fn test_bool_folding5() {
        let query = r#"SELECT * from t WHERE 1 = 0 and 5 >= 4 and 6 < -8 UNION ALL SELECT * FROM t where 1 = 9"#;

        let coordinator = RouterRuntimeMock::new();
        let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        let query_explain = plan.as_explain().unwrap();
        let buckets = query.bucket_discovery(top).unwrap();

        insta::assert_snapshot!(query_explain, @r#"
        union all
            projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
                selection false::bool
                    scan "t"
            projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
                selection false::bool
                    scan "t"
        execution options:
            sql_vdbe_opcode_max = 45000
            sql_motion_row_max = 5000
        "#);

        assert_eq!(Buckets::new_empty(), buckets);
    }

    #[test]
    fn test_bool_folding6() {
        let query = r#"SELECT * from t JOIN t on true WHERE 1 = 1 AND 56 <= 500"#;

        let coordinator = RouterRuntimeMock::new();
        let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        let query_explain = plan.as_explain().unwrap();
        let buckets = query.bucket_discovery(top).unwrap();

        insta::assert_snapshot!(query_explain, @r#"
        projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
            selection true::bool
                join on true::bool
                    scan "t"
                    motion [policy: full]
                        projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d", "t"."bucket_id"::int -> "bucket_id")
                            scan "t"
        execution options:
            sql_vdbe_opcode_max = 45000
            sql_motion_row_max = 5000
        "#);

        assert_eq!(Buckets::All, buckets);
    }

    #[test]
    fn test_bool_folding7() {
        let query = r#"SELECT * FROM t WHERE true and not (null is null)"#;

        let coordinator = RouterRuntimeMock::new();
        let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        let query_explain = plan.as_explain().unwrap();
        let buckets = query.bucket_discovery(top).unwrap();

        insta::assert_snapshot!(query_explain, @r#"
        projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
            selection false::bool
                scan "t"
        execution options:
            sql_vdbe_opcode_max = 45000
            sql_motion_row_max = 5000
        "#);

        assert_eq!(Buckets::new_empty(), buckets);
    }

    #[test]
    fn test_bool_folding8() {
        let query = r#"SELECT * FROM t WHERE null = null"#;

        let coordinator = RouterRuntimeMock::new();
        let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        let query_explain = plan.as_explain().unwrap();
        let buckets = query.bucket_discovery(top).unwrap();

        insta::assert_snapshot!(query_explain, @r#"
        projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
            selection NULL::unknown
                scan "t"
        execution options:
            sql_vdbe_opcode_max = 45000
            sql_motion_row_max = 5000
        "#);

        assert_eq!(Buckets::new_empty(), buckets);
    }

    #[test]
    fn test_bool_folding9() {
        let query = r#"SELECT * FROM t WHERE 1.0 = 1"#;

        let coordinator = RouterRuntimeMock::new();
        let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        let query_explain = plan.as_explain().unwrap();
        let buckets = query.bucket_discovery(top).unwrap();

        insta::assert_snapshot!(query_explain, @r#"
        projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
            selection true::bool
                scan "t"
        execution options:
            sql_vdbe_opcode_max = 45000
            sql_motion_row_max = 5000
        "#);

        assert_eq!(Buckets::All, buckets);
    }

    #[test]
    fn test_bool_folding10() {
        let query = r#"SELECT * FROM t WHERE 'inf' = 'inf'::double"#;

        let coordinator = RouterRuntimeMock::new();
        let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        let query_explain = plan.as_explain().unwrap();
        let buckets = query.bucket_discovery(top).unwrap();

        insta::assert_snapshot!(query_explain, @r#"
        projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
            selection true::bool
                scan "t"
        execution options:
            sql_vdbe_opcode_max = 45000
            sql_motion_row_max = 5000
        "#);

        assert_eq!(Buckets::All, buckets);
    }

    #[test]
    fn test_bool_folding11() {
        let query = r#"SELECT * FROM t WHERE 'nan' = 'nan'::double"#;

        let coordinator = RouterRuntimeMock::new();
        let mut query = ExecutingQuery::from_text_and_params(&coordinator, query, vec![]).unwrap();
        let plan = query.get_exec_plan().get_ir_plan();
        let top = plan.get_top().unwrap();
        let query_explain = plan.as_explain().unwrap();
        let buckets = query.bucket_discovery(top).unwrap();

        insta::assert_snapshot!(query_explain, @r#"
        projection ("t"."a"::int -> "a", "t"."b"::int -> "b", "t"."c"::int -> "c", "t"."d"::int -> "d")
            selection false::bool
                scan "t"
        execution options:
            sql_vdbe_opcode_max = 45000
            sql_motion_row_max = 5000
        "#);

        assert_eq!(Buckets::new_empty(), buckets);
    }
}
