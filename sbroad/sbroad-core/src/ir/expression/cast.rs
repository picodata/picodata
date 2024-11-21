use std::fmt::{Display, Formatter};

use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::ast::Rule;
use crate::ir::node::Cast;
use crate::ir::relation::Type as RelationType;
use crate::ir::Plan;
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};

use super::{MutNode, NodeId};

#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum Type {
    Any,
    Map,
    Boolean,
    Datetime,
    Decimal,
    Double,
    Integer,
    Number,
    Scalar,
    String,
    Text,
    Uuid,
    Unsigned,
    Varchar(usize),
}

impl TryFrom<&Rule> for Type {
    type Error = SbroadError;

    /// Pay attention that we can't build `Type::Varchar(length)` from string
    /// because it has an additional length parameter. It should be constructed
    /// separately.
    fn try_from(ast_type: &Rule) -> Result<Self, Self::Error> {
        match ast_type {
            Rule::TypeAny => Ok(Type::Any),
            Rule::TypeBool => Ok(Type::Boolean),
            Rule::TypeDatetime => Ok(Type::Datetime),
            Rule::TypeDecimal => Ok(Type::Decimal),
            Rule::TypeDouble => Ok(Type::Double),
            Rule::TypeInt => Ok(Type::Integer),
            Rule::TypeNumber => Ok(Type::Number),
            Rule::TypeScalar => Ok(Type::Scalar),
            Rule::TypeString => Ok(Type::String),
            Rule::TypeText => Ok(Type::Text),
            Rule::TypeUuid => Ok(Type::Uuid),
            Rule::TypeUnsigned => Ok(Type::Unsigned),
            _ => Err(SbroadError::Unsupported(
                Entity::Type,
                Some(format_smolstr!("{ast_type:?}")),
            )),
        }
    }
}

impl TryFrom<&RelationType> for Type {
    type Error = SbroadError;

    fn try_from(relational_type: &RelationType) -> Result<Self, Self::Error> {
        match relational_type {
            RelationType::Boolean => Ok(Type::Boolean),
            RelationType::Datetime => Ok(Type::Datetime),
            RelationType::Decimal => Ok(Type::Decimal),
            RelationType::Double => Ok(Type::Double),
            RelationType::Integer => Ok(Type::Integer),
            RelationType::Scalar => Ok(Type::Scalar),
            RelationType::String => Ok(Type::String),
            RelationType::Number => Ok(Type::Number),
            RelationType::Unsigned => Ok(Type::Unsigned),
            RelationType::Map => Ok(Type::Map),
            RelationType::Any => Ok(Type::Any),
            RelationType::Uuid => Ok(Type::Uuid),
            RelationType::Array => Err(SbroadError::Unsupported(
                Entity::Type,
                Some("array in the cast operation".to_smolstr()),
            )),
        }
    }
}

impl From<&Type> for SmolStr {
    fn from(t: &Type) -> Self {
        match t {
            Type::Any => "any".to_smolstr(),
            Type::Map => "map".to_smolstr(),
            Type::Boolean => "bool".to_smolstr(),
            Type::Datetime => "datetime".to_smolstr(),
            Type::Decimal => "decimal".to_smolstr(),
            Type::Double => "double".to_smolstr(),
            Type::Integer => "int".to_smolstr(),
            Type::Number => "number".to_smolstr(),
            Type::Scalar => "scalar".to_smolstr(),
            Type::String => "string".to_smolstr(),
            Type::Text => "text".to_smolstr(),
            Type::Uuid => "uuid".to_smolstr(),
            Type::Unsigned => "unsigned".to_smolstr(),
            Type::Varchar(length) => format_smolstr!("varchar({length})"),
        }
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", SmolStr::from(self))
    }
}

impl Type {
    #[must_use]
    pub fn as_relation_type(&self) -> RelationType {
        match self {
            Type::Any | Type::Scalar => RelationType::Scalar,
            Type::Map => RelationType::Map,
            Type::Boolean => RelationType::Boolean,
            Type::Datetime => RelationType::Datetime,
            Type::Decimal => RelationType::Decimal,
            Type::Double => RelationType::Double,
            Type::Integer => RelationType::Integer,
            Type::Number => RelationType::Number,
            Type::Uuid => RelationType::Uuid,
            Type::String | Type::Text | Type::Varchar(_) => RelationType::String,
            Type::Unsigned => RelationType::Unsigned,
        }
    }
}

impl Plan {
    /// Adds a cast expression to the plan.
    ///
    /// # Errors
    /// - Child node is not of the expression type.
    pub fn add_cast(&mut self, expr_id: NodeId, to_type: Type) -> Result<NodeId, SbroadError> {
        let cast_expr = Cast {
            child: expr_id,
            to: to_type,
        };
        let cast_id = self.nodes.push(cast_expr.into());

        let child_plan_node = self.get_mut_node(expr_id)?;
        if let MutNode::Parameter(ty) = child_plan_node {
            ty.param_type = Some(to_type.as_relation_type());
        }

        Ok(cast_id)
    }
}
