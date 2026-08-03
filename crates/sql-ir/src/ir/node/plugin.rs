use crate::errors::{Entity, SbroadError};
use crate::ir::node::{Node136, Node232, Node96, NodeAligned};
use crate::ir::options::Timeout;
use crate::ir::{Node, NodeId, Plan};
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr};

#[must_use]
pub fn get_default_timeout() -> Timeout {
    Timeout::default_ddl()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrateToOpts {
    pub timeout: Timeout,
    pub rollback_timeout: Timeout,
}

impl Default for MigrateToOpts {
    fn default() -> Self {
        MigrateToOpts {
            timeout: get_default_timeout(),
            rollback_timeout: get_default_timeout(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, PartialOrd, Ord)]
pub struct SettingsPair {
    pub key: SmolStr,
    pub value: SmolStr,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, PartialOrd, Ord)]
pub struct ServiceSettings {
    pub name: SmolStr,
    pub pairs: Vec<SettingsPair>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CreatePlugin {
    pub name: SmolStr,
    pub version: SmolStr,
    pub if_not_exists: bool,
    pub timeout: Timeout,
}

impl From<CreatePlugin> for NodeAligned {
    fn from(value: CreatePlugin) -> Self {
        Self::Node96(Node96::CreatePlugin(value))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EnablePlugin {
    pub name: SmolStr,
    pub version: SmolStr,
    pub timeout: Timeout,
}

impl From<EnablePlugin> for NodeAligned {
    fn from(value: EnablePlugin) -> Self {
        Self::Node96(Node96::EnablePlugin(value))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DisablePlugin {
    pub name: SmolStr,
    pub version: SmolStr,
    pub timeout: Timeout,
}

impl From<DisablePlugin> for NodeAligned {
    fn from(value: DisablePlugin) -> Self {
        Self::Node96(Node96::DisablePlugin(value))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DropPlugin {
    pub name: SmolStr,
    pub version: SmolStr,
    pub if_exists: bool,
    pub with_data: bool,
    pub timeout: Timeout,
}

impl From<DropPlugin> for NodeAligned {
    fn from(value: DropPlugin) -> Self {
        Self::Node96(Node96::DropPlugin(value))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MigrateTo {
    pub name: SmolStr,
    pub version: SmolStr,
    pub opts: MigrateToOpts,
}

impl From<MigrateTo> for NodeAligned {
    fn from(value: MigrateTo) -> Self {
        Self::Node136(Node136::MigrateTo(value))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AppendServiceToTier {
    pub plugin_name: SmolStr,
    pub version: SmolStr,
    pub service_name: SmolStr,
    pub tier: SmolStr,
    pub timeout: Timeout,
}

impl From<AppendServiceToTier> for NodeAligned {
    fn from(value: AppendServiceToTier) -> Self {
        Self::Node232(Node232::AppendServiceToTier(value))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RemoveServiceFromTier {
    pub plugin_name: SmolStr,
    pub version: SmolStr,
    pub service_name: SmolStr,
    pub tier: SmolStr,
    pub timeout: Timeout,
}

impl From<RemoveServiceFromTier> for NodeAligned {
    fn from(value: RemoveServiceFromTier) -> Self {
        Self::Node232(Node232::RemoveServiceFromTier(value))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChangeConfig {
    pub plugin_name: SmolStr,
    pub version: SmolStr,
    pub key_value_grouped: Vec<ServiceSettings>,
    pub timeout: Timeout,
}

impl From<ChangeConfig> for NodeAligned {
    fn from(value: ChangeConfig) -> Self {
        Self::Node136(Node136::ChangeConfig(value))
    }
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub enum MutPlugin<'a> {
    Create(&'a mut CreatePlugin),
    Enable(&'a mut EnablePlugin),
    Disable(&'a mut DisablePlugin),
    Drop(&'a mut DropPlugin),
    MigrateTo(&'a mut MigrateTo),
    AppendServiceToTier(&'a mut AppendServiceToTier),
    RemoveServiceFromTier(&'a mut RemoveServiceFromTier),
    ChangeConfig(&'a mut ChangeConfig),
}

impl MutPlugin<'_> {
    /// Return a mutable reference to the timeout, if present.
    pub fn timeout_mut(&mut self) -> Option<&mut Timeout> {
        match self {
            MutPlugin::Create(n) => Some(&mut n.timeout),
            MutPlugin::Enable(n) => Some(&mut n.timeout),
            MutPlugin::Disable(n) => Some(&mut n.timeout),
            MutPlugin::Drop(n) => Some(&mut n.timeout),
            MutPlugin::MigrateTo(n) => Some(&mut n.opts.timeout),
            MutPlugin::AppendServiceToTier(n) => Some(&mut n.timeout),
            MutPlugin::RemoveServiceFromTier(n) => Some(&mut n.timeout),
            MutPlugin::ChangeConfig(n) => Some(&mut n.timeout),
        }
    }
}

/// Represent a plugin query.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum Plugin<'a> {
    /// Create a new plugin.
    Create(&'a CreatePlugin),
    /// Enable plugin.
    Enable(&'a EnablePlugin),
    /// Disable plugin.
    Disable(&'a DisablePlugin),
    /// Remove plugin from system.
    Drop(&'a DropPlugin),
    /// Run installed plugin migrations.
    MigrateTo(&'a MigrateTo),
    /// Append plugin service to a tier.
    AppendServiceToTier(&'a AppendServiceToTier),
    /// Remove plugin service from tier.
    RemoveServiceFromTier(&'a RemoveServiceFromTier),
    /// Change plugin service configuration.
    ChangeConfig(&'a ChangeConfig),
}

impl Plugin<'_> {
    #[must_use]
    pub fn get_plugin_owned(&self) -> PluginOwned {
        match self {
            Plugin::Create(create) => PluginOwned::Create((*create).clone()),
            Plugin::Enable(enable) => PluginOwned::Enable((*enable).clone()),
            Plugin::Disable(disable) => PluginOwned::Disable((*disable).clone()),
            Plugin::Drop(drop) => PluginOwned::Drop((*drop).clone()),
            Plugin::MigrateTo(migrate_to) => PluginOwned::MigrateTo((*migrate_to).clone()),
            Plugin::AppendServiceToTier(add_to_tier) => {
                PluginOwned::AppendServiceToTier((*add_to_tier).clone())
            }
            Plugin::RemoveServiceFromTier(rm_from_tier) => {
                PluginOwned::RemoveServiceFromTier((*rm_from_tier).clone())
            }
            Plugin::ChangeConfig(change_config) => {
                PluginOwned::ChangeConfig((*change_config).clone())
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PluginOwned {
    /// Create a new plugin.
    Create(CreatePlugin),
    /// Enable plugin.
    Enable(EnablePlugin),
    /// Disable plugin.
    Disable(DisablePlugin),
    /// Remove plugin from system.
    Drop(DropPlugin),
    /// Run installed plugin migrations.
    MigrateTo(MigrateTo),
    /// Append plugin service to a tier.
    AppendServiceToTier(AppendServiceToTier),
    /// Remove plugin service from tier.
    RemoveServiceFromTier(RemoveServiceFromTier),
    /// Change plugin service configuration.
    ChangeConfig(ChangeConfig),
}

impl From<PluginOwned> for NodeAligned {
    fn from(value: PluginOwned) -> Self {
        match value {
            PluginOwned::Create(create) => create.into(),
            PluginOwned::Enable(enable) => enable.into(),
            PluginOwned::Disable(disable) => disable.into(),
            PluginOwned::Drop(drop) => drop.into(),
            PluginOwned::MigrateTo(migrate) => migrate.into(),
            PluginOwned::AppendServiceToTier(add) => add.into(),
            PluginOwned::RemoveServiceFromTier(rm) => rm.into(),
            PluginOwned::ChangeConfig(change_config) => change_config.into(),
        }
    }
}

impl Plan {
    /// Get a reference to a plugin node.
    ///
    /// # Errors
    /// - the node is not a block node.
    pub fn get_plugin_node(&self, node_id: NodeId) -> Result<Plugin<'_>, SbroadError> {
        let node = self.get_node(node_id)?;
        match node {
            Node::Plugin(plugin) => Ok(plugin),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!(
                    "node {node:?} (id {node_id}) is not Block type"
                )),
            )),
        }
    }
}
