use crate::ir::value::Value;
use crate::{
    errors::{Entity, SbroadError},
    ir::node::{MutNode, NodeId},
    ir::{relation::Type as RelationType, Node, Plan},
};
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr};

use super::node::ddl::{Ddl, MutDdl};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ColumnDef {
    pub name: SmolStr,
    pub data_type: RelationType,
    pub is_nullable: bool,
}

impl Default for ColumnDef {
    fn default() -> Self {
        Self {
            name: SmolStr::default(),
            data_type: RelationType::default(),
            is_nullable: true,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub struct ParamDef {
    pub data_type: RelationType,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub enum Language {
    #[default]
    SQL,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum SetParamScopeType {
    Local,
    Session,
}

// TODO: Fill with actual values.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum SetParamValue {
    NamedParam { name: SmolStr },
    TimeZone,
}

impl SetParamValue {
    #[must_use]
    pub fn param_name(&self) -> SmolStr {
        match self {
            SetParamValue::NamedParam { name } => name.clone(),
            SetParamValue::TimeZone => SmolStr::from("TimeZone"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum AlterSystemType {
    AlterSystemSet {
        param_name: SmolStr,
        param_value: Value,
    },
    AlterSystemReset {
        /// In case of None, all params are supposed to be reset.
        param_name: Option<SmolStr>,
    },
}

impl Plan {
    /// Get DDL node from the plan arena.
    ///
    /// # Errors
    /// - the node index is absent in arena
    /// - current node is not of DDL type
    pub fn get_ddl_node(&self, node_id: NodeId) -> Result<Ddl, SbroadError> {
        let node = self.get_node(node_id)?;
        match node {
            Node::Ddl(ddl) => Ok(ddl),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("node is not DDL type: {node:?}")),
            )),
        }
    }

    /// Get a mutable DDL node from the plan arena.
    ///
    /// # Errors
    /// - the node index is absent in arena
    /// - current node is not of DDL type
    pub fn get_mut_ddl_node(&mut self, node_id: NodeId) -> Result<MutDdl, SbroadError> {
        let node = self.get_mut_node(node_id)?;
        match node {
            MutNode::Ddl(ddl) => Ok(ddl),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("node is not DDL type: {node:?}")),
            )),
        }
    }
}
