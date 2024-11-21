use crate::debug;
use crate::executor::protocol::VTablesMeta;
use crate::ir::node::expression::Expression;
use crate::ir::node::relational::Relational;
use crate::ir::node::{
    Constant, Delete, Join, Node, NodeId, Reference, ScanRelation, StableFunction,
};
use crate::ir::relation::Column;
use crate::ir::tree::traversal::{LevelNode, PostOrderWithFilter};
use ahash::AHashSet;
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;
use std::collections::HashMap;
use std::fmt::Write as _;
use tarantool::tlua::{self, Push};
use tarantool::tuple::{FunctionArgs, Tuple};

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::engine::helpers::table_name;
use crate::executor::ir::ExecutionPlan;
use crate::ir::operator::OrderByType;
use crate::ir::value::{LuaValue, Value};

use super::space::{create_table, TableGuard};
use super::tree::SyntaxData;

#[derive(Debug, Eq, Deserialize, Serialize, Push, Clone)]
pub struct PatternWithParams {
    pub pattern: String,
    pub params: Vec<Value>,
}

impl PatternWithParams {
    #[must_use]
    pub fn into_parts(self) -> (String, Vec<Value>) {
        (self.pattern, self.params)
    }
}

impl PartialEq for PatternWithParams {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern && self.params == other.params
    }
}
impl TryFrom<FunctionArgs> for PatternWithParams {
    type Error = SbroadError;

    fn try_from(value: FunctionArgs) -> Result<Self, Self::Error> {
        (&Tuple::from(value)).try_into()
    }
}

impl TryFrom<&Tuple> for PatternWithParams {
    type Error = SbroadError;

    fn try_from(value: &Tuple) -> Result<Self, Self::Error> {
        debug!(
            Option::from("argument parsing"),
            &format!("Query parameters: {value:?}"),
        );
        match value.decode::<EncodedPatternWithParams>() {
            Ok(encoded) => Ok(PatternWithParams::try_from(encoded)?),
            Err(e) => Err(SbroadError::ParsingError(
                Entity::PatternWithParams,
                format_smolstr!("{e:?}"),
            )),
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct EncodedPatternWithParams(String, Option<Vec<LuaValue>>);

impl From<PatternWithParams> for EncodedPatternWithParams {
    fn from(mut value: PatternWithParams) -> Self {
        let encoded_params: Vec<LuaValue> = value.params.drain(..).map(LuaValue::from).collect();
        EncodedPatternWithParams(value.pattern, Some(encoded_params))
    }
}

impl TryFrom<EncodedPatternWithParams> for PatternWithParams {
    type Error = SbroadError;
    fn try_from(mut value: EncodedPatternWithParams) -> Result<Self, Self::Error> {
        let params: Vec<Value> = match value.1.take() {
            Some(mut encoded_params) => encoded_params
                .drain(..)
                .map(Value::from)
                .collect::<Vec<Value>>(),
            None => Vec::new(),
        };
        let res = PatternWithParams {
            pattern: value.0,
            params,
        };
        Ok(res)
    }
}

impl PatternWithParams {
    #[must_use]
    pub fn new(pattern: String, params: Vec<Value>) -> Self {
        PatternWithParams { pattern, params }
    }
}

impl From<PatternWithParams> for String {
    fn from(p: PatternWithParams) -> Self {
        format!("pattern: {}, parameters: {:?}", p.pattern, p.params)
    }
}

impl ExecutionPlan {
    /// # Errors
    /// - IR plan is invalid
    pub fn to_params(&self) -> Result<Vec<Value>, SbroadError> {
        let plan = self.get_ir_plan();
        let capacity = plan.nodes.len();
        let mut was: AHashSet<NodeId> = AHashSet::with_capacity(plan.constants.len());
        let filter = |id: NodeId| -> bool {
            if matches!(plan.get_node(id), Ok(Node::Parameter(_))) && !was.contains(&id) {
                was.insert(id);
                true
            } else {
                false
            }
        };
        let mut tree = PostOrderWithFilter::with_capacity(
            |node| plan.flashback_subtree_iter(node),
            capacity,
            Box::new(filter),
        );
        let top_id = plan.get_top()?;
        tree.populate_nodes(top_id);
        let nodes = tree.take_nodes();
        let mut params: Vec<Value> = Vec::with_capacity(nodes.len());
        for LevelNode(_, param_id) in nodes {
            let Expression::Constant(Constant { value }) = plan.get_expression_node(param_id)?
            else {
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!(
                        "expected parameter constant on id={param_id}"
                    )),
                ));
            };
            params.push(value.clone());
        }
        Ok(params)
    }

    /// Remove vtables from execution plan.
    ///
    /// Because vtables are transfered in required data,
    /// no need to store them in optional data (where
    /// execution plan is stored).
    /// One exception currently is vtable under DML
    /// node, such query is not cacheable (because
    /// we execute dml using tarantool api) and this
    /// vtable is not removed from plan.
    ///
    /// # Errors
    /// - Invalid plan
    pub fn remove_vtables(&mut self) -> Result<VTablesMeta, SbroadError> {
        let mut reserved_motion_id = None;
        {
            let ir = self.get_ir_plan();
            let top_id = ir.get_top()?;
            let top = ir.get_relation_node(top_id)?;

            if top.is_dml() {
                let top_children = ir.children(top_id);

                // DELETE without WHERE clause don't have children.
                if !top_children.is_empty() {
                    reserved_motion_id = Some(top_children[0]);
                }
            }
        }

        let Some(vtables) = self.get_mut_vtables() else {
            return Ok(VTablesMeta::default());
        };
        let mut vtables_meta = VTablesMeta::with_capacity(vtables.len());

        for (id, vtable) in vtables.iter() {
            vtables_meta.insert(*id, vtable.metadata());
        }

        let reserved_entry = if let Some(id) = reserved_motion_id {
            vtables.remove_entry(&id)
        } else {
            None
        };
        vtables.clear();
        if let Some((key, value)) = reserved_entry {
            vtables.insert(key, value);
        }

        Ok(vtables_meta)
    }

    /// Transform plan sub-tree (pointed by top) to sql string pattern with parameters.
    ///
    /// # Errors
    /// - plan is invalid and can't be transformed
    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    pub fn to_sql(
        &self,
        nodes: &[&SyntaxData],
        plan_id: &str,
        vtables_meta: Option<&VTablesMeta>,
    ) -> Result<(PatternWithParams, Vec<TableGuard>), SbroadError> {
        let vtable_engine = self.vtable_engine()?;
        let capacity = self.get_vtables().map_or(1, HashMap::len);
        // In our data structures, we store plan identifiers without
        // quotes in normalized form, but tarantool local sql has
        // different normalizing rules (to uppercase), so we need
        // to wrap all names in quotes.
        let push_identifier = |sql: &mut String, identifier: &str| {
            sql.push('\"');
            sql.push_str(identifier);
            sql.push('\"');
        };
        let mut guard = Vec::with_capacity(capacity);
        let mut params: Vec<Value> = Vec::new();

        let mut sql = String::new();
        let delim = " ";

        let need_delim_after = |id: usize| -> bool {
            let mut result: bool = true;
            if id > 0 {
                if let Some(SyntaxData::OpenParenthesis) = nodes.get(id - 1) {
                    result = false;
                }
            }
            if let Some(SyntaxData::Comma | SyntaxData::CloseParenthesis) = nodes.get(id) {
                result = false;
            }
            if id == 0 {
                result = false;
            }
            result
        };

        let ir_plan = self.get_ir_plan();

        // The starting value of the counter for generating anonymous column names.
        // It emulates Tarantool parser (generated by lemon parser generator).
        // It traverses the parse tree bottom-up from left to right and increments
        // the global counter with the columns that need an auto-generated name.
        // As a result each such column would be named like "COLUMN_%d", where "%d"
        // is the new global counter value.
        for (id, data) in nodes.iter().enumerate() {
            if let Some(' ' | '(') = sql.chars().last() {
            } else if need_delim_after(id) {
                sql.push_str(delim);
            }

            match data {
                // TODO: should we care about plans without projections?
                // Or they should be treated as invalid?
                SyntaxData::Asterisk(relation_name) => {
                    if let Some(relation_name) = relation_name {
                        push_identifier(&mut sql, relation_name.as_str());
                        sql.push('.')
                    }
                    sql.push('*')
                }
                SyntaxData::Alias(s) => {
                    sql.push_str("as ");
                    push_identifier(&mut sql, s);
                }
                SyntaxData::UnquotedAlias(s) => {
                    sql.push_str("as ");
                    sql.push_str(s);
                }
                SyntaxData::Cast => sql.push_str("CAST"),
                SyntaxData::Case => sql.push_str("CASE"),
                SyntaxData::When => sql.push_str("WHEN"),
                SyntaxData::Then => sql.push_str("THEN"),
                SyntaxData::Else => sql.push_str("ELSE"),
                SyntaxData::End => sql.push_str("END"),
                SyntaxData::CloseParenthesis => sql.push(')'),
                SyntaxData::Concat => sql.push_str("||"),
                SyntaxData::Comma => sql.push(','),
                SyntaxData::Condition => sql.push_str("ON"),
                SyntaxData::Escape => sql.push_str("ESCAPE"),
                SyntaxData::Like => sql.push_str("LIKE"),
                SyntaxData::Distinct => sql.push_str("DISTINCT"),
                SyntaxData::OrderByPosition(index) => sql.push_str(format!("{index}").as_str()),
                SyntaxData::OrderByType(order_type) => match order_type {
                    OrderByType::Asc => sql.push_str("ASC"),
                    OrderByType::Desc => sql.push_str("DESC"),
                },
                SyntaxData::Inline(content) => sql.push_str(content),
                SyntaxData::From => sql.push_str("FROM"),
                SyntaxData::Leading => sql.push_str("LEADING"),
                SyntaxData::Limit(limit) => sql.push_str(&format_smolstr!("LIMIT {limit}")),
                SyntaxData::Both => sql.push_str("BOTH"),
                SyntaxData::Trailing => sql.push_str("TRAILING"),
                SyntaxData::Operator(s) => sql.push_str(s.as_str()),
                SyntaxData::OpenParenthesis => sql.push('('),
                SyntaxData::Trim => sql.push_str("TRIM"),
                SyntaxData::PlanId(id) => {
                    let node = ir_plan.get_node(*id)?;
                    match node {
                        Node::Ddl(_) => {
                            return Err(SbroadError::Unsupported(
                                Entity::Node,
                                Some("DDL nodes are not supported in the generated SQL".into()),
                            ));
                        }
                        Node::Acl(_) => {
                            return Err(SbroadError::Unsupported(
                                Entity::Node,
                                Some("ACL nodes are not supported in the generated SQL".into()),
                            ));
                        }
                        Node::Block(_) => {
                            return Err(SbroadError::Unsupported(
                                Entity::Node,
                                Some(
                                    "Code block nodes are not supported in the generated SQL"
                                        .into(),
                                ),
                            ));
                        }
                        Node::Parameter(..) => {
                            return Err(SbroadError::Unsupported(
                                Entity::Node,
                                Some("Parameters are not supported in the generated SQL".into()),
                            ));
                        }
                        Node::Invalid(..) => {
                            return Err(SbroadError::Unsupported(
                                Entity::Node,
                                Some("Invalid nodes are not supported in the generated SQL".into()),
                            ));
                        }
                        Node::Plugin(_) => {
                            return Err(SbroadError::Unsupported(
                                Entity::Node,
                                Some("Plugin are not supported in the generated SQL".into()),
                            ));
                        }
                        Node::Relational(rel) => match rel {
                            Relational::Except { .. } => sql.push_str("EXCEPT"),
                            Relational::GroupBy { .. } => sql.push_str("GROUP BY"),
                            Relational::Intersect { .. } => sql.push_str("INTERSECT"),
                            Relational::Having { .. } => sql.push_str("HAVING"),
                            Relational::OrderBy { .. } => sql.push_str("ORDER BY"),
                            Relational::Delete(Delete { relation, .. }) => {
                                sql.push_str("DELETE FROM ");
                                push_identifier(&mut sql, relation)
                            }
                            Relational::Join(Join { kind, .. }) => sql.push_str(
                                format!("{} JOIN", kind.to_string().to_uppercase()).as_str(),
                            ),
                            Relational::Projection { .. }
                            | Relational::SelectWithoutScan { .. } => sql.push_str("SELECT"),
                            Relational::ScanRelation(ScanRelation { relation, .. }) => {
                                push_identifier(&mut sql, relation);
                            }
                            Relational::ScanSubQuery { .. }
                            | Relational::ScanCte { .. }
                            | Relational::Motion { .. }
                            | Relational::ValuesRow { .. }
                            | Relational::Limit { .. } => {}
                            Relational::Selection { .. } => sql.push_str("WHERE"),
                            Relational::Union { .. } => sql.push_str("UNION"),
                            Relational::UnionAll { .. } => sql.push_str("UNION ALL"),
                            Relational::Values { .. } => sql.push_str("VALUES"),
                            Relational::Insert { .. } | Relational::Update { .. } => {
                                return Err(SbroadError::Invalid(
                                    Entity::Node,
                                    Some("DML nodes are not supported in local SQL".into()),
                                ));
                            }
                        },
                        Node::Expression(expr) => {
                            match expr {
                                Expression::Alias { .. }
                                | Expression::ExprInParentheses { .. }
                                | Expression::Bool { .. }
                                | Expression::Arithmetic { .. }
                                | Expression::Cast { .. }
                                | Expression::Case { .. }
                                | Expression::Concat { .. }
                                | Expression::Like { .. }
                                | Expression::Row { .. }
                                | Expression::Trim { .. }
                                | Expression::Unary { .. } => {}
                                Expression::Constant(Constant { value, .. }) => {
                                    write!(sql, "{value}").map_err(|e| {
                                        SbroadError::FailedTo(
                                            Action::Put,
                                            Some(Entity::Value),
                                            format_smolstr!("constant value to SQL: {e}"),
                                        )
                                    })?;
                                }
                                Expression::Reference(Reference { position, .. }) => {
                                    let rel_id = ir_plan.get_relational_from_reference_node(*id)?;
                                    let rel_node = ir_plan.get_relation_node(rel_id)?;

                                    if rel_node.is_motion() {
                                        if let Ok(vt) = self.get_motion_vtable(*id) {
                                            let alias = (*vt)
                                                .get_columns()
                                                .get(*position)
                                                .map(|column| &column.name)
                                                .ok_or_else(|| {
                                                    SbroadError::NotFound(
                                                        Entity::Name,
                                                        format_smolstr!(
                                                            "for column at position {position}"
                                                        ),
                                                    )
                                                })?;
                                            if let Some(name) = (*vt).get_alias() {
                                                push_identifier(&mut sql, name);
                                                sql.push('.');
                                                push_identifier(&mut sql, alias);
                                                continue;
                                            }
                                        }
                                    }

                                    let alias = &ir_plan.get_alias_from_reference_node(&expr)?;
                                    if rel_node.is_insert() {
                                        // We expect `INSERT INTO t(a, b) VALUES(1, 2)`
                                        // rather then `INSERT INTO t(t.a, t.b) VALUES(1, 2)`.
                                        push_identifier(&mut sql, alias);
                                        continue;
                                    }
                                    if let Some(name) = ir_plan.scan_name(rel_id, *position)? {
                                        push_identifier(&mut sql, name);
                                        sql.push('.');
                                        push_identifier(&mut sql, alias);

                                        continue;
                                    }
                                    push_identifier(&mut sql, alias);
                                }
                                Expression::StableFunction(StableFunction {
                                    name,
                                    is_system,
                                    ..
                                }) => {
                                    if *is_system {
                                        sql.push_str(name);
                                    } else {
                                        push_identifier(&mut sql, name);
                                    }
                                }
                                Expression::CountAsterisk { .. } => {
                                    sql.push('*');
                                }
                            }
                        }
                    }
                }
                SyntaxData::Parameter(id) => {
                    sql.push('?');
                    let value = ir_plan.get_expression_node(*id)?;
                    if let Expression::Constant(Constant { value, .. }) = value {
                        params.push(value.clone());
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some(format_smolstr!("parameter {value:?} is not a constant")),
                        ));
                    }
                }
                SyntaxData::VTable(motion_id) => {
                    // Note that in case Motion is a top of the plan it won't be transformed
                    // into local SQL. Rather `VirtualTable::to_output` method would be called
                    // in order to transform it into result, directly returned to user.
                    // See `Query::dispatch` method for usage.
                    let name = table_name(plan_id, *motion_id);
                    let build_names = |columns: &[Column]| -> String {
                        let cp: usize = columns.iter().map(|c| c.name.len() + 3).sum();
                        let mut col_names = String::with_capacity(cp.saturating_sub(1));
                        if let Some((last, other)) = columns.split_last() {
                            for col in other {
                                push_identifier(&mut col_names, &col.name);
                                col_names.push(',');
                            }
                            push_identifier(&mut col_names, &last.name);
                        }
                        col_names
                    };
                    let col_names = if let Some(meta) = vtables_meta {
                        let meta = meta.get(motion_id).ok_or_else(|| {
                            SbroadError::Invalid(
                                Entity::RequiredData,
                                Some(format_smolstr!(
                                    "no vtable meta for motion with id={motion_id}"
                                )),
                            )
                        })?;
                        build_names(&meta.columns)
                    } else {
                        let vtable = self.get_motion_vtable(*motion_id)?;
                        build_names(vtable.get_columns())
                    };
                    write!(sql, "SELECT {col_names} FROM \"{name}\"").map_err(|e| {
                        SbroadError::FailedTo(
                            Action::Serialize,
                            Some(Entity::VirtualTable),
                            format_smolstr!("SQL builder: {e}"),
                        )
                    })?;
                    // BETWEEN can refer to the same virtual table multiple times.
                    let table_guard =
                        create_table(self, plan_id, *motion_id, &vtable_engine, vtables_meta)?;
                    guard.push(table_guard);
                }
            }
        }
        // MUST be constructed out of the `syntax.ordered.sql` context scope.
        Ok((PatternWithParams::new(sql, params), guard))
    }

    /// Checks if the given query subtree modifies data or not.
    ///
    /// # Errors
    /// - If the subtree top is not a relational node.
    pub fn subtree_modifies_data(&self, top_id: NodeId) -> Result<bool, SbroadError> {
        // Tarantool doesn't support `INSERT`, `UPDATE` and `DELETE` statements
        // with `RETURNING` clause. That is why it is enough to check if the top
        // node is a data modification statement or not.
        let top = self.get_ir_plan().get_relation_node(top_id)?;
        Ok(top.is_dml())
    }
}

#[cfg(test)]
mod tests;
