use serde::Serialize;

use crate::{
    errors::{Entity, SbroadError},
    ir::aggregates::AggregateKind,
};

use super::{
    Alias, ArithmeticExpr, BoolExpr, Case, Cast, Concat, Constant, CountAsterisk, Like,
    NodeAligned, NodeId, Over, Parameter, Reference, Row, ScalarFunction, SubQueryReference,
    Timestamp, Trim, UnaryExpr, Window,
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
    SubQueryReference(SubQueryReference),
    Row(Row),
    ScalarFunction(ScalarFunction),
    Trim(Trim),
    Unary(UnaryExpr),
    CountAsterisk(CountAsterisk),
    Case(Case),
    Timestamp(Timestamp),
    Over(Over),
    Window(Window),
    Parameter(Parameter),
}

impl From<ExprOwned> for NodeAligned {
    fn from(value: ExprOwned) -> Self {
        match value {
            ExprOwned::Window(window) => window.into(),
            ExprOwned::Over(over) => over.into(),
            ExprOwned::Alias(alias) => alias.into(),
            ExprOwned::Arithmetic(arithm) => arithm.into(),
            ExprOwned::Bool(bool) => bool.into(),
            ExprOwned::Case(case) => case.into(),
            ExprOwned::Cast(cast) => cast.into(),
            ExprOwned::Concat(concat) => concat.into(),
            ExprOwned::Constant(constant) => constant.into(),
            ExprOwned::CountAsterisk(count) => count.into(),
            ExprOwned::Like(like) => like.into(),
            ExprOwned::Reference(reference) => reference.into(),
            ExprOwned::SubQueryReference(sq_reference) => sq_reference.into(),
            ExprOwned::Row(row) => row.into(),
            ExprOwned::ScalarFunction(stable_func) => stable_func.into(),
            ExprOwned::Trim(trim) => trim.into(),
            ExprOwned::Unary(unary) => unary.into(),
            ExprOwned::Timestamp(lt) => lt.into(),
            ExprOwned::Parameter(param) => param.into(),
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
    SubQueryReference(&'a SubQueryReference),
    Row(&'a Row),
    ScalarFunction(&'a ScalarFunction),
    Trim(&'a Trim),
    Unary(&'a UnaryExpr),
    CountAsterisk(&'a CountAsterisk),
    Case(&'a Case),
    Timestamp(&'a Timestamp),
    Over(&'a Over),
    Window(&'a Window),
    Parameter(&'a Parameter),
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
    SubQueryReference(&'a mut SubQueryReference),
    Row(&'a mut Row),
    ScalarFunction(&'a mut ScalarFunction),
    Trim(&'a mut Trim),
    Unary(&'a mut UnaryExpr),
    CountAsterisk(&'a mut CountAsterisk),
    Case(&'a mut Case),
    Timestamp(&'a mut Timestamp),
    Over(&'a mut Over),
    Window(&'a mut Window),
    Parameter(&'a mut Parameter),
}

#[allow(dead_code)]
impl Expression<'_> {
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
        AggregateKind::from_name(name).is_some()
    }

    #[must_use]
    pub fn is_aggregate_fun(&self) -> bool {
        match self {
            Expression::ScalarFunction(ScalarFunction { name, .. }) => {
                Expression::is_aggregate_name(name)
            }
            _ => false,
        }
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
            Expression::Window(window) => ExprOwned::Window((*window).clone()),
            Expression::Over(over) => ExprOwned::Over((*over).clone()),
            Expression::Alias(alias) => ExprOwned::Alias((*alias).clone()),
            Expression::Arithmetic(arithm) => ExprOwned::Arithmetic((*arithm).clone()),
            Expression::Bool(bool) => ExprOwned::Bool((*bool).clone()),
            Expression::Case(case) => ExprOwned::Case((*case).clone()),
            Expression::Cast(cast) => ExprOwned::Cast((*cast).clone()),
            Expression::Concat(con) => ExprOwned::Concat((*con).clone()),
            Expression::Constant(constant) => ExprOwned::Constant((*constant).clone()),
            Expression::Like(like) => ExprOwned::Like((*like).clone()),
            Expression::CountAsterisk(count) => ExprOwned::CountAsterisk((*count).clone()),
            Expression::Reference(reference) => ExprOwned::Reference((*reference).clone()),
            Expression::SubQueryReference(sq_reference) => {
                ExprOwned::SubQueryReference((*sq_reference).clone())
            }
            Expression::Row(row) => ExprOwned::Row((*row).clone()),
            Expression::ScalarFunction(sfunc) => ExprOwned::ScalarFunction((*sfunc).clone()),
            Expression::Trim(trim) => ExprOwned::Trim((*trim).clone()),
            Expression::Unary(unary) => ExprOwned::Unary((*unary).clone()),
            Expression::Timestamp(lt) => ExprOwned::Timestamp((*lt).clone()),
            Expression::Parameter(param) => ExprOwned::Parameter((*param).clone()),
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
}
