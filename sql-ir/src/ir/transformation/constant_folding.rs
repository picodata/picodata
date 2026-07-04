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
