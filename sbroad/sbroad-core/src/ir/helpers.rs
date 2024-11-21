//! Helper module with functions and structures for the IR.

use smol_str::{SmolStr, ToSmolStr};

use crate::backend::sql::tree::{SyntaxData, SyntaxPlan};
use crate::errors::{Action, Entity, SbroadError};
use crate::ir::node::{
    Alias, BoolExpr, Case, Constant, Delete, ExprInParentheses, GroupBy, Having, Join, Motion,
    NodeId, OrderBy, Reference, Row, ScanCte, ScanRelation, ScanSubQuery, Selection, UnaryExpr,
    Update, ValuesRow,
};
use crate::ir::operator::OrderByEntity;
use crate::ir::tree::traversal::{PostOrder, EXPR_CAPACITY};
use crate::ir::{Node, Plan};
use std::collections::hash_map::DefaultHasher;
use std::fmt::Write;
use std::hash::BuildHasher;

use super::node::expression::Expression;
use super::node::relational::Relational;
use super::node::{ArithmeticExpr, Like, Limit};

/// Helper macros to build a hash map or set
/// from the list of arguments.
#[macro_export]
macro_rules! collection {
    // map-like
    ($($k:expr => $v:expr),* $(,)?) => {{
        use std::iter::{Iterator, IntoIterator};
        Iterator::collect(IntoIterator::into_iter([$(($k, $v),)*]))
    }};
    // set-like
    ($($v:expr),* $(,)?) => {{
        use std::iter::{Iterator, IntoIterator};
        Iterator::collect(IntoIterator::into_iter([$($v,)*]))
    }};
}

/// By default, `HashSet` uses `RandomState` to initialize the hasher.
/// Because of that, we can't get repeatable order in the set between
/// test runs that can cause flaky test results in some cases.
#[derive(Clone)]
pub struct RepeatableState;

impl BuildHasher for RepeatableState {
    type Hasher = DefaultHasher;
    fn build_hasher(&self) -> DefaultHasher {
        DefaultHasher::new()
    }
}

impl Default for RepeatableState {
    fn default() -> Self {
        Self
    }
}

/// Helper formatting function for tabulation.
fn formatted_tabulate(buf: &mut String, n: i32) -> Result<(), std::fmt::Error> {
    for _ in 0..n {
        write!(buf, "\t")?;
    }
    Ok(())
}

/// Helper formatting function for writing with tabulation.
fn write_with_tabulation(buf: &mut String, n: i32, text: &str) -> Result<(), std::fmt::Error> {
    formatted_tabulate(buf, n)?;
    write!(buf, "{text}")
}

/// Helper formatting function for writing with tabulation and new line.
fn writeln_with_tabulation(buf: &mut String, n: i32, text: &str) -> Result<(), std::fmt::Error> {
    formatted_tabulate(buf, n)?;
    writeln!(buf, "{text}")
}

/// Formatting helper debug functions
impl Plan {
    /// Helper function for printing Expression node.
    #[allow(clippy::too_many_lines)]
    fn write_expr(
        &self,
        buf: &mut String,
        tabulation_number: i32,
        node_id: NodeId,
    ) -> Result<(), std::fmt::Error> {
        let expr_try = self.get_expression_node(node_id);
        if let Ok(expr) = expr_try {
            write!(buf, "expression: ")?;
            match expr {
                Expression::Alias(Alias { name, child }) => {
                    let child_node = self.get_node(*child).expect("Alias must have a child node");
                    let child = match child_node {
                        Node::Expression(child_expr) => format!("{child_expr:?}"),
                        Node::Parameter(..) => String::from("parameter"),
                        Node::Relational(rel) => format!("{rel:?}"),
                        // TODO: fix `fix_betweens` logic to cover SubQueries with References.
                        _ => unreachable!("unexpected Alias child node"),
                    };
                    writeln!(buf, "Alias [name = {name}, child = {child}]")?;
                }
                Expression::ExprInParentheses(ExprInParentheses { child }) => {
                    writeln!(buf, "Parentheses")?;
                    writeln_with_tabulation(buf, tabulation_number + 1, "Child")?;
                    self.formatted_arena_node(buf, tabulation_number + 1, *child)?;
                }
                Expression::Case(Case {
                    search_expr,
                    when_blocks,
                    else_expr,
                }) => {
                    writeln!(buf, "Case")?;
                    if let Some(search_expr) = search_expr {
                        writeln_with_tabulation(buf, tabulation_number + 1, "Search_expr")?;
                        self.formatted_arena_node(buf, tabulation_number + 1, *search_expr)?;
                    }
                    for (cond_expr, res_expr) in when_blocks {
                        writeln_with_tabulation(buf, tabulation_number + 1, "WHEN")?;
                        self.formatted_arena_node(buf, tabulation_number + 1, *cond_expr)?;
                        writeln_with_tabulation(buf, tabulation_number + 1, "THEN")?;
                        self.formatted_arena_node(buf, tabulation_number + 1, *res_expr)?;
                    }
                    if let Some(else_expr) = else_expr {
                        writeln_with_tabulation(buf, tabulation_number + 1, "Else expr")?;
                        self.formatted_arena_node(buf, tabulation_number + 1, *else_expr)?;
                    }
                }
                Expression::Bool(BoolExpr { op, left, right }) => {
                    writeln!(buf, "Bool [op: {op}]")?;
                    writeln_with_tabulation(buf, tabulation_number + 1, "Left child")?;
                    self.formatted_arena_node(buf, tabulation_number + 1, *left)?;
                    writeln_with_tabulation(buf, tabulation_number + 1, "Right child")?;
                    self.formatted_arena_node(buf, tabulation_number + 1, *right)?;
                }
                Expression::Constant(Constant { value }) => {
                    writeln!(buf, "Constant [value = {value}]")?;
                }
                Expression::CountAsterisk(_) => writeln!(buf, "CountAsterisk")?,
                Expression::Reference(Reference {
                    targets,
                    position,
                    parent,
                    col_type,
                    ..
                }) => {
                    let alias_name = self.get_alias_from_reference_node(&expr).unwrap();

                    writeln!(buf, "Reference")?;
                    writeln_with_tabulation(
                        buf,
                        tabulation_number + 1,
                        format!("Alias: {alias_name}").as_str(),
                    )?;

                    // See explain logic for Reference node
                    let rel_id = self.get_relational_from_reference_node(node_id);
                    if let Ok(rel_id) = rel_id {
                        let rel_node = self.get_relation_node(rel_id);
                        if rel_node.is_ok() {
                            if let Ok(Some(name)) = self.scan_name(rel_id, *position) {
                                writeln_with_tabulation(
                                    buf,
                                    tabulation_number + 1,
                                    format!("Referenced table name (or alias): {name}").as_str(),
                                )?;
                            }
                        }
                    }

                    writeln_with_tabulation(
                        buf,
                        tabulation_number + 1,
                        format!("Parent: {parent:?}").as_str(),
                    )?;

                    if let Some(targets) = targets {
                        for target_id in targets {
                            writeln_with_tabulation(
                                buf,
                                tabulation_number + 1,
                                format!("target_id: {target_id}").as_str(),
                            )?;
                        }
                    } else {
                        writeln!(buf, "NO TARGETS")?;
                    }

                    writeln_with_tabulation(
                        buf,
                        tabulation_number + 1,
                        format!("Column type: {col_type}").as_str(),
                    )?;
                }
                Expression::Row(Row { list, distribution }) => {
                    writeln!(buf, "Row [distribution = {distribution:?}]")?;
                    writeln_with_tabulation(buf, tabulation_number + 1, "List:")?;
                    for value in list {
                        self.formatted_arena_node(buf, tabulation_number + 2, *value)?;
                    }
                }
                Expression::Cast(_) => writeln!(buf, "Cast")?,
                Expression::Trim(_) => writeln!(buf, "Trim")?,
                Expression::Concat(_) => writeln!(buf, "Concat")?,
                Expression::Like(Like {
                    left,
                    right,
                    escape,
                }) => {
                    writeln!(buf, "Like")?;
                    writeln_with_tabulation(buf, tabulation_number + 1, "Left child")?;
                    self.formatted_arena_node(buf, tabulation_number + 1, *left)?;
                    writeln_with_tabulation(buf, tabulation_number + 1, "Right child")?;
                    self.formatted_arena_node(buf, tabulation_number + 1, *right)?;
                    writeln_with_tabulation(buf, tabulation_number + 1, "Escape")?;
                    self.formatted_arena_node(buf, tabulation_number + 1, *escape)?;
                }
                Expression::StableFunction(_) => writeln!(buf, "StableFunction")?,
                Expression::Unary(UnaryExpr { op, child }) => {
                    writeln!(buf, "Unary [op: {op}]")?;
                    writeln_with_tabulation(buf, tabulation_number + 1, "Child")?;
                    self.formatted_arena_node(buf, tabulation_number + 1, *child)?;
                }
                Expression::Arithmetic(ArithmeticExpr { left, right, op }) => {
                    writeln!(buf, "Arithmetic: [op: {op}]")?;
                    writeln_with_tabulation(buf, tabulation_number + 1, "Child")?;
                    writeln_with_tabulation(buf, tabulation_number + 1, "Left child")?;
                    self.formatted_arena_node(buf, tabulation_number + 1, *left)?;
                    writeln_with_tabulation(buf, tabulation_number + 1, "Right child")?;
                    self.formatted_arena_node(buf, tabulation_number + 1, *right)?;
                }
            };
        }
        Ok(())
    }

    /// Debug function for formatting working arena node.
    #[allow(clippy::too_many_lines)]
    fn formatted_arena_node(
        &self,
        buf: &mut String,
        tabulation_number: i32,
        node_id: NodeId,
    ) -> Result<(), std::fmt::Error> {
        if tabulation_number == 0 {
            writeln!(buf, "---------------------------------------------")?;
        }
        write_with_tabulation(buf, tabulation_number, format!("[id: {node_id}] ").as_str())?;
        let node = self.get_node(node_id).expect("Plan must be valid");
        match node {
            Node::Relational(relation) => {
                write!(buf, "relation: ")?;
                // Print relation name and specific info.
                match relation {
                    Relational::ScanRelation(ScanRelation {
                        alias, relation, ..
                    }) => {
                        writeln!(buf, "ScanRelation")?;
                        writeln_with_tabulation(
                            buf,
                            tabulation_number + 1,
                            format!("Relation: {relation}").as_str(),
                        )?;
                        if let Some(alias) = alias {
                            writeln_with_tabulation(
                                buf,
                                tabulation_number + 1,
                                format!("Alias: {alias}").as_str(),
                            )?;
                        }
                    }
                    Relational::Join(Join { condition, .. }) => {
                        writeln!(buf, "InnerJoin")?;
                        writeln_with_tabulation(buf, tabulation_number + 1, "Condition:")?;
                        self.formatted_arena_node(buf, tabulation_number + 2, *condition)?;
                    }
                    Relational::SelectWithoutScan(_) => {
                        writeln!(buf, "SelectWithoutScan")?;
                    }
                    Relational::Projection(_) => {
                        writeln!(buf, "Projection")?;
                    }
                    Relational::ScanCte(ScanCte { alias, .. }) => {
                        writeln!(buf, "ScanCte")?;
                        if !alias.is_empty() {
                            writeln_with_tabulation(
                                buf,
                                tabulation_number + 1,
                                format!("Alias: {alias}").as_str(),
                            )?;
                        }
                    }
                    Relational::ScanSubQuery(ScanSubQuery { alias, .. }) => {
                        writeln!(buf, "ScanSubQuery")?;
                        if let Some(alias) = alias {
                            if !alias.is_empty() {
                                writeln_with_tabulation(
                                    buf,
                                    tabulation_number + 1,
                                    format!("Alias: {alias}").as_str(),
                                )?;
                            }
                        }
                    }
                    Relational::Selection(Selection {
                        children: _,
                        filter,
                        output: _,
                    }) => {
                        writeln!(buf, "Selection")?;
                        writeln_with_tabulation(buf, tabulation_number + 1, "Filter")?;
                        self.formatted_arena_node(buf, tabulation_number + 1, *filter)?;
                    }
                    Relational::Having(Having { filter, .. }) => {
                        writeln!(buf, "Having")?;
                        writeln_with_tabulation(buf, tabulation_number + 1, "Filter")?;
                        self.formatted_arena_node(buf, tabulation_number + 1, *filter)?;
                    }
                    Relational::GroupBy(GroupBy {
                        gr_cols, is_final, ..
                    }) => {
                        writeln!(buf, "GroupBy [is_final = {is_final}]")?;
                        writeln_with_tabulation(buf, tabulation_number + 1, "Gr_cols:")?;
                        for gr_col in gr_cols {
                            let gl_col_expr = self.get_expression_node(*gr_col);
                            let text = if let Ok(gl_col_expr) = gl_col_expr {
                                format!("Gr_col: {gl_col_expr:?}")
                            } else {
                                format!("Gr_col: {gr_col}")
                            };
                            writeln_with_tabulation(buf, tabulation_number + 2, text.as_str())?;
                        }
                    }
                    Relational::OrderBy(OrderBy {
                        order_by_elements, ..
                    }) => {
                        writeln!(buf, "OrderBy")?;
                        writeln_with_tabulation(buf, tabulation_number + 1, "Order_by_elements:")?;
                        for element in order_by_elements {
                            let order_by_entity_str = match element.entity {
                                OrderByEntity::Expression { expr_id } => {
                                    let order_by_expr = self.get_expression_node(expr_id);
                                    if let Ok(order_by_expr) = order_by_expr {
                                        format!("{order_by_expr:?}")
                                    } else {
                                        "?".to_string()
                                    }
                                }
                                OrderByEntity::Index { value } => format!("{value}"),
                            };
                            let order_by_type = element.order_type.clone();
                            writeln_with_tabulation(buf, tabulation_number + 2, format!("Order_by_element: {order_by_entity_str} [order_type = {order_by_type:?}]").as_str())?;
                        }
                    }
                    Relational::Values { .. } => writeln!(buf, "Values")?,
                    Relational::ValuesRow(ValuesRow { data, .. }) => {
                        writeln!(buf, "ValuesRow")?;
                        writeln_with_tabulation(buf, tabulation_number + 1, "Data")?;
                        self.formatted_arena_node(buf, tabulation_number + 1, *data)?;
                    }
                    Relational::Motion(Motion { policy, alias, .. }) => {
                        write!(buf, "Motion [policy = {policy:?}, alias = ")?;
                        if let Some(alias) = alias {
                            write!(buf, "{alias}")?;
                        } else {
                            write!(buf, "None")?;
                        }
                        writeln!(buf, "]")?;
                    }
                    Relational::Union { .. } => writeln!(buf, "Union")?,
                    Relational::UnionAll { .. } => writeln!(buf, "UnionAll")?,
                    Relational::Update(Update {
                        relation,
                        update_columns_map,
                        ..
                    }) => {
                        writeln!(buf, "Update")?;
                        writeln_with_tabulation(buf, tabulation_number + 1, "Update columns map:")?;
                        for (rel_pos, proj_pos) in update_columns_map {
                            writeln_with_tabulation(buf, tabulation_number + 2, format!("Update {relation} column on pos {rel_pos} to child projection column on pos {proj_pos}").as_str())?;
                        }
                    }
                    Relational::Delete(_) => writeln!(buf, "Delete")?,
                    Relational::Insert(_) => writeln!(buf, "Insert")?,
                    Relational::Intersect(_) => writeln!(buf, "Intersect")?,
                    Relational::Except(_) => writeln!(buf, "Except")?,
                    Relational::Limit(Limit { limit, .. }) => writeln!(buf, "Limit {limit}")?,
                }
                // Print children.
                match relation {
                    Relational::Join(_)
                    | Relational::Projection(_)
                    | Relational::Except(_)
                    | Relational::Delete(_)
                    | Relational::Insert(_)
                    | Relational::Intersect(_)
                    | Relational::ScanSubQuery(_)
                    | Relational::Selection(_)
                    | Relational::SelectWithoutScan(_)
                    | Relational::Values(_)
                    | Relational::OrderBy(_)
                    | Relational::Limit(_)
                    | Relational::Motion(_)
                    | Relational::Union(_)
                    | Relational::UnionAll(_)
                    | Relational::Update(_)
                    | Relational::Having(_)
                    | Relational::GroupBy(_)
                    | Relational::ValuesRow(_) => {
                        writeln_with_tabulation(buf, tabulation_number + 1, "Children:")?;
                        for child in &relation.children() {
                            writeln_with_tabulation(
                                buf,
                                tabulation_number + 2,
                                format!("Child_id = {child}").as_str(),
                            )?;
                        }
                    }
                    Relational::ScanCte(ScanCte { child, .. }) => {
                        writeln_with_tabulation(buf, tabulation_number + 1, "Children:")?;
                        writeln_with_tabulation(
                            buf,
                            tabulation_number + 2,
                            format!("Child_id = {child}").as_str(),
                        )?;
                    }
                    Relational::ScanRelation { .. } => {
                        writeln_with_tabulation(buf, tabulation_number + 1, "[No children]")?;
                    }
                }
                // Print output.
                if let Relational::Delete(Delete { output: None, .. }) = relation {
                    writeln_with_tabulation(buf, tabulation_number + 1, "Output: NONE")?;
                } else {
                    let output = relation.output();
                    writeln_with_tabulation(
                        buf,
                        tabulation_number + 1,
                        format!("Output_id: {output}").as_str(),
                    )?;
                    self.formatted_arena_node(buf, tabulation_number + 2, output)?;
                }
                writeln!(buf, "---------------------------------------------")?;
            }
            Node::Expression(_) => {
                self.write_expr(buf, tabulation_number, node_id)?;
            }
            Node::Invalid(_) => {
                writeln!(buf, "INVALID")?;
                writeln!(buf, "---------------------------------------------")?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Pretty-printing plan `arena` subtree.
    ///
    /// # Errors
    /// Fail to format one of the relational node.
    pub fn formatted_arena_subtree(&self, node_id: NodeId) -> Result<String, SbroadError> {
        let mut ir_tree = PostOrder::with_capacity(|node| self.nodes.rel_iter(node), EXPR_CAPACITY);
        ir_tree.populate_nodes(node_id);
        let nodes = ir_tree.take_nodes();

        let mut buf = String::new();
        for level_node in &nodes {
            let id = level_node.1;
            if self.formatted_arena_node(&mut buf, 0, id).is_err() {
                return Err(SbroadError::FailedTo(
                    Action::Serialize,
                    Some(Entity::Plan),
                    "Unable to get formatted arena string".to_smolstr(),
                ));
            }
        }
        Ok(buf)
    }

    /// Pretty-printing plan `arena`.
    ///
    /// # Errors
    /// Fail to format one of the relational node.
    pub fn formatted_arena(&self) -> Result<String, SbroadError> {
        let top_id = self.get_top()?;
        self.formatted_arena_subtree(top_id)
    }
}

/// Formatting helper debug functions
impl SyntaxPlan<'_> {
    /// Pretty-printing plan `nodes.arena`.
    ///
    /// # Errors
    /// - Failed to retrieve top node.
    /// - Failed to format one of the nodes.
    pub fn formatted(&self, plan: &Plan) -> Result<String, SbroadError> {
        let mut buf = String::new();
        let top = self.top.ok_or_else(|| {
            SbroadError::Invalid(
                Entity::SyntaxPlan,
                Some(SmolStr::from(
                    "Top id should have already be set for SyntaxPlan",
                )),
            )
        })?;
        if self.formatted_inner(plan, &mut buf, 0, top).is_err() {
            return Err(SbroadError::FailedTo(
                Action::Serialize,
                Some(Entity::SyntaxPlan),
                "Unable to get formatted arena string".to_smolstr(),
            ));
        }
        Ok(buf)
    }

    fn formatted_inner(
        &self,
        plan: &Plan,
        buf: &mut String,
        tabulation_number: i32,
        node_id: usize,
    ) -> Result<(), std::fmt::Error> {
        let node = self.nodes.arena.get(node_id);
        if let Some(node) = node {
            formatted_tabulate(buf, tabulation_number)?;
            let data = &node.data;
            if let SyntaxData::PlanId(id) = data {
                let node = plan.get_node(*id);
                if let Ok(node) = node {
                    writeln!(buf, "{node:?} [id={id:?}]")?;
                }
            } else {
                writeln!(buf, "{data:?}")?;
            }

            if let Some(left_id) = node.left {
                writeln_with_tabulation(buf, tabulation_number + 1, "Left:")?;
                self.formatted_inner(plan, buf, tabulation_number + 2, left_id)?;
            }
            if !node.right.is_empty() {
                writeln_with_tabulation(buf, tabulation_number + 1, "Right:")?;
            }
            for right in &node.right {
                self.formatted_inner(plan, buf, tabulation_number + 2, *right)?;
            }
        } else {
            writeln_with_tabulation(buf, tabulation_number, "MISSING")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
