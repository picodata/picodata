use serde::Serialize;

use crate::{
    errors::{Entity, SbroadError},
    ir::{aggregates::AggregateKind, distribution::Distribution},
};

use super::{
    Alias, ArithmeticExpr, BoolExpr, Case, Cast, Concat, Constant, CountAsterisk,
    ExprInParentheses, Like, NodeAligned, NodeId, Reference, Row, StableFunction, Trim, UnaryExpr,
};

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum ExprOwned {
    Alias(Alias),
    Bool(BoolExpr),
    Arithmetic(ArithmeticExpr),
    Cast(Cast),
    Concat(Concat),
    Constant(Constant),
    Like(Like),
    Reference(Reference),
    Row(Row),
    StableFunction(StableFunction),
    Trim(Trim),
    Unary(UnaryExpr),
    CountAsterisk(CountAsterisk),
    Case(Case),
    ExprInParentheses(ExprInParentheses),
}

impl From<ExprOwned> for NodeAligned {
    fn from(value: ExprOwned) -> Self {
        match value {
            ExprOwned::Alias(alias) => alias.into(),
            ExprOwned::Arithmetic(arithm) => arithm.into(),
            ExprOwned::Bool(bool) => bool.into(),
            ExprOwned::Case(case) => case.into(),
            ExprOwned::Cast(cast) => cast.into(),
            ExprOwned::Concat(concat) => concat.into(),
            ExprOwned::Constant(constant) => constant.into(),
            ExprOwned::CountAsterisk(count) => count.into(),
            ExprOwned::Like(like) => like.into(),
            ExprOwned::ExprInParentheses(expr) => expr.into(),
            ExprOwned::Reference(reference) => reference.into(),
            ExprOwned::Row(row) => row.into(),
            ExprOwned::StableFunction(stable_func) => stable_func.into(),
            ExprOwned::Trim(trim) => trim.into(),
            ExprOwned::Unary(unary) => unary.into(),
        }
    }
}

/// Tuple tree build blocks.
///
/// A tuple describes a single portion of data moved among cluster nodes.
/// It consists of the ordered, strictly typed expressions with names
/// (columns) and additional information about data distribution policy.
///
/// Tuple is a tree with a `Row` top (level 0) and a list of the named
/// `Alias` columns (level 1). This convention is used across the code
/// and should not be changed. It ensures that we always know the
/// name of any column in the tuple and therefore simplifies AST
/// deserialization.
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum Expression<'a> {
    Alias(&'a Alias),
    Bool(&'a BoolExpr),
    Arithmetic(&'a ArithmeticExpr),
    Cast(&'a Cast),
    Concat(&'a Concat),
    Constant(&'a Constant),
    Like(&'a Like),
    Reference(&'a Reference),
    Row(&'a Row),
    StableFunction(&'a StableFunction),
    Trim(&'a Trim),
    Unary(&'a UnaryExpr),
    CountAsterisk(&'a CountAsterisk),
    Case(&'a Case),
    ExprInParentheses(&'a ExprInParentheses),
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, PartialEq, Eq)]
pub enum MutExpression<'a> {
    Alias(&'a mut Alias),
    Bool(&'a mut BoolExpr),
    Arithmetic(&'a mut ArithmeticExpr),
    Cast(&'a mut Cast),
    Concat(&'a mut Concat),
    Constant(&'a mut Constant),
    Like(&'a mut Like),
    Reference(&'a mut Reference),
    Row(&'a mut Row),
    StableFunction(&'a mut StableFunction),
    Trim(&'a mut Trim),
    Unary(&'a mut UnaryExpr),
    CountAsterisk(&'a mut CountAsterisk),
    Case(&'a mut Case),
    ExprInParentheses(&'a mut ExprInParentheses),
}

#[allow(dead_code)]
impl Expression<'_> {
    /// Gets current row distribution.
    ///
    /// # Errors
    /// Returns `SbroadError` when the function is called on expression
    /// other than `Row` or a node doesn't know its distribution yet.
    pub fn distribution(&self) -> Result<&Distribution, SbroadError> {
        if let Expression::Row(Row { distribution, .. }) = self {
            let Some(dist) = distribution else {
                return Err(SbroadError::Invalid(
                    Entity::Distribution,
                    Some("distribution is uninitialized".into()),
                ));
            };
            return Ok(dist);
        }
        Err(SbroadError::Invalid(Entity::Expression, None))
    }

    /// Clone the row children list.
    ///
    /// # Errors
    /// - node isn't `Row`
    pub fn clone_row_list(&self) -> Result<Vec<NodeId>, SbroadError> {
        match self {
            Expression::Row(Row { list, .. }) => Ok(list.clone()),
            _ => Err(SbroadError::Invalid(
                Entity::Expression,
                Some("node isn't Row type".into()),
            )),
        }
    }

    #[must_use]
    pub fn is_aggregate_name(name: &str) -> bool {
        // currently we support only simple aggregates
        AggregateKind::new(name).is_some()
    }

    #[must_use]
    pub fn is_aggregate_fun(&self) -> bool {
        match self {
            Expression::StableFunction(StableFunction { name, .. }) => {
                Expression::is_aggregate_name(name)
            }
            _ => false,
        }
    }

    /// Gets relational node id containing the reference.
    ///
    /// # Errors
    /// - node isn't reference type
    /// - reference doesn't have a parent
    pub fn get_parent(&self) -> Result<NodeId, SbroadError> {
        if let Expression::Reference(Reference { parent, .. }) = self {
            return parent.ok_or_else(|| {
                SbroadError::Invalid(Entity::Expression, Some("Reference has no parent".into()))
            });
        }
        Err(SbroadError::Invalid(
            Entity::Expression,
            Some("node is not Reference type".into()),
        ))
    }

    /// The node is a row expression.
    #[must_use]
    pub fn is_row(&self) -> bool {
        matches!(self, Expression::Row(_))
    }
    #[must_use]
    pub fn is_arithmetic(&self) -> bool {
        matches!(self, Expression::Arithmetic(_))
    }

    #[must_use]
    pub fn get_expr_owned(&self) -> ExprOwned {
        match self {
            Expression::Alias(alias) => ExprOwned::Alias((*alias).clone()),
            Expression::Arithmetic(arithm) => ExprOwned::Arithmetic((*arithm).clone()),
            Expression::Bool(bool) => ExprOwned::Bool((*bool).clone()),
            Expression::Case(case) => ExprOwned::Case((*case).clone()),
            Expression::Cast(cast) => ExprOwned::Cast((*cast).clone()),
            Expression::Concat(con) => ExprOwned::Concat((*con).clone()),
            Expression::Constant(constant) => ExprOwned::Constant((*constant).clone()),
            Expression::Like(like) => ExprOwned::Like((*like).clone()),
            Expression::CountAsterisk(count) => ExprOwned::CountAsterisk((*count).clone()),
            Expression::ExprInParentheses(expr_par) => {
                ExprOwned::ExprInParentheses((*expr_par).clone())
            }
            Expression::Reference(reference) => ExprOwned::Reference((*reference).clone()),
            Expression::Row(row) => ExprOwned::Row((*row).clone()),
            Expression::StableFunction(sfunc) => ExprOwned::StableFunction((*sfunc).clone()),
            Expression::Trim(trim) => ExprOwned::Trim((*trim).clone()),
            Expression::Unary(unary) => ExprOwned::Unary((*unary).clone()),
        }
    }
}

impl MutExpression<'_> {
    /// Get a mutable reference to the row children list.
    ///
    /// # Errors
    /// - node isn't `Row`
    pub fn get_row_list_mut(&mut self) -> Result<&mut Vec<NodeId>, SbroadError> {
        match self {
            MutExpression::Row(Row { ref mut list, .. }) => Ok(list),
            _ => Err(SbroadError::Invalid(
                Entity::Expression,
                Some("node isn't Row type".into()),
            )),
        }
    }

    /// Replaces parent in the reference node with the new one.
    pub fn replace_parent_in_reference(&mut self, from_id: Option<NodeId>, to_id: Option<NodeId>) {
        if let MutExpression::Reference(Reference { parent, .. }) = self {
            if *parent == from_id {
                *parent = to_id;
            }
        }
    }

    /// Flushes parent in the reference node.
    pub fn flush_parent_in_reference(&mut self) {
        if let MutExpression::Reference(Reference { parent, .. }) = self {
            *parent = None;
        }
    }
}
