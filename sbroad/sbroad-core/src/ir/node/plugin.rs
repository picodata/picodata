use crate::errors::{Entity, SbroadError};
use crate::ir::node::{Node136, Node232, Node96, NodeAligned};
use crate::ir::{Node, NodeId, Plan};
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr};
use tarantool::decimal::Decimal;

#[must_use]
pub fn get_default_timeout() -> Decimal {
    Decimal::from(10)
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MigrateToOpts {
    pub timeout: Decimal,
    pub rollback_timeout: Decimal,
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
    pub timeout: Decimal,
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
    pub timeout: Decimal,
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
    pub timeout: Decimal,
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
    pub timeout: Decimal,
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
    pub timeout: Decimal,
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
    pub timeout: Decimal,
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
    pub timeout: Decimal,
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

impl<'a> Plugin<'a> {
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
    pub fn get_plugin_node(&self, node_id: NodeId) -> Result<Plugin, SbroadError> {
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

#[cfg(test)]
mod test {
    use super::{
        get_default_timeout, AppendServiceToTier, ChangeConfig, CreatePlugin, DisablePlugin,
        DropPlugin, EnablePlugin, MigrateTo, MigrateToOpts, PluginOwned, RemoveServiceFromTier,
        ServiceSettings, SettingsPair,
    };
    use crate::executor::engine::mock::RouterConfigurationMock;
    use crate::frontend::sql::ast::AbstractSyntaxTree;
    use crate::frontend::Ast;
    use crate::ir::node::{ArenaType, NodeId};
    use crate::ir::Plan;
    use smol_str::SmolStr;
    use tarantool::decimal::Decimal;

    #[test]
    fn test_plugin_parsing() {
        struct TestCase {
            sql: &'static str,
            arena_type: ArenaType,
            expected: PluginOwned,
        }

        let test_cases = &[
            TestCase {
                sql: r#"CREATE PLUGIN "abc" 0.1.1"#,
                arena_type: ArenaType::Arena96,
                expected: PluginOwned::Create(CreatePlugin {
                    name: SmolStr::from("abc"),
                    version: SmolStr::from("0.1.1"),
                    if_not_exists: false,
                    timeout: get_default_timeout(),
                }),
            },
            TestCase {
                sql: r#"CREATE PLUGIN "test_plugin" 0.0.1"#,
                arena_type: ArenaType::Arena96,
                expected: PluginOwned::Create(CreatePlugin {
                    name: SmolStr::from("test_plugin"),
                    version: SmolStr::from("0.0.1"),
                    if_not_exists: false,
                    timeout: get_default_timeout(),
                }),
            },
            TestCase {
                sql: r#"CREATE PLUGIN IF NOT EXISTS "abc" 0.1.1"#,
                arena_type: ArenaType::Arena96,
                expected: PluginOwned::Create(CreatePlugin {
                    name: SmolStr::from("abc"),
                    version: SmolStr::from("0.1.1"),
                    if_not_exists: true,
                    timeout: get_default_timeout(),
                }),
            },
            TestCase {
                sql: r#"CREATE PLUGIN IF NOT EXISTS "abcde" 0.1.2 option(timeout=1)"#,
                arena_type: ArenaType::Arena96,
                expected: PluginOwned::Create(CreatePlugin {
                    name: SmolStr::from("abcde"),
                    version: SmolStr::from("0.1.2"),
                    if_not_exists: true,
                    timeout: Decimal::from(1),
                }),
            },
            TestCase {
                sql: r#"ALTER PLUGIN "abc" 1.1.1 ENABLE"#,
                arena_type: ArenaType::Arena96,
                expected: PluginOwned::Enable(EnablePlugin {
                    name: SmolStr::from("abc"),
                    version: SmolStr::from("1.1.1"),
                    timeout: Decimal::from(10),
                }),
            },
            TestCase {
                sql: r#"ALTER PLUGIN "abc" 1.1.1 ENABLE option(timeout=1)"#,
                arena_type: ArenaType::Arena96,
                expected: PluginOwned::Enable(EnablePlugin {
                    name: SmolStr::from("abc"),
                    version: SmolStr::from("1.1.1"),
                    timeout: Decimal::from(1),
                }),
            },
            TestCase {
                sql: r#"ALTER PLUGIN "abc" 1.1.1 DISABLE option(timeout=1)"#,
                arena_type: ArenaType::Arena96,
                expected: PluginOwned::Disable(DisablePlugin {
                    name: SmolStr::from("abc"),
                    version: SmolStr::from("1.1.1"),
                    timeout: Decimal::from(1),
                }),
            },
            TestCase {
                sql: r#"DROP PLUGIN "abc" 1.1.1"#,
                arena_type: ArenaType::Arena96,
                expected: PluginOwned::Drop(DropPlugin {
                    name: SmolStr::from("abc"),
                    version: SmolStr::from("1.1.1"),
                    if_exists: false,
                    with_data: false,
                    timeout: Decimal::from(10),
                }),
            },
            TestCase {
                sql: r#"DROP PLUGIN "abc" 1.1.1 option(timeout=1)"#,
                arena_type: ArenaType::Arena96,
                expected: PluginOwned::Drop(DropPlugin {
                    name: SmolStr::from("abc"),
                    version: SmolStr::from("1.1.1"),
                    if_exists: false,
                    with_data: false,
                    timeout: Decimal::from(1),
                }),
            },
            TestCase {
                sql: r#"DROP PLUGIN IF EXISTS "abcde" 1.1.1 WITH DATA option(timeout=11)"#,
                arena_type: ArenaType::Arena96,
                expected: PluginOwned::Drop(DropPlugin {
                    name: SmolStr::from("abcde"),
                    version: SmolStr::from("1.1.1"),
                    if_exists: true,
                    with_data: true,
                    timeout: Decimal::from(11),
                }),
            },
            TestCase {
                sql: r#"DROP PLUGIN IF EXISTS "abcde" 1.1.1 WITH DATA"#,
                arena_type: ArenaType::Arena96,
                expected: PluginOwned::Drop(DropPlugin {
                    name: SmolStr::from("abcde"),
                    version: SmolStr::from("1.1.1"),
                    if_exists: true,
                    with_data: true,
                    timeout: Decimal::from(10),
                }),
            },
            TestCase {
                sql: r#"ALTER PLUGIN "abc" MIGRATE TO 0.1.0"#,
                arena_type: ArenaType::Arena136,
                expected: PluginOwned::MigrateTo(MigrateTo {
                    name: SmolStr::from("abc"),
                    version: SmolStr::from("0.1.0"),
                    opts: MigrateToOpts {
                        timeout: get_default_timeout(),
                        rollback_timeout: get_default_timeout(),
                    },
                }),
            },
            TestCase {
                sql: r#"ALTER PLUGIN "abc" MIGRATE TO 0.1.0 option(timeout=11, rollback_timeout=12)"#,
                arena_type: ArenaType::Arena136,
                expected: PluginOwned::MigrateTo(MigrateTo {
                    name: SmolStr::from("abc"),
                    version: SmolStr::from("0.1.0"),
                    opts: MigrateToOpts {
                        timeout: Decimal::from(11),
                        rollback_timeout: Decimal::from(12),
                    },
                }),
            },
            TestCase {
                sql: r#"ALTER PLUGIN "abc" 0.1.0 ADD SERVICE "svc1" TO TIER "tier1" option(timeout=1)"#,
                arena_type: ArenaType::Arena232,
                expected: PluginOwned::AppendServiceToTier(AppendServiceToTier {
                    service_name: SmolStr::from("svc1"),
                    plugin_name: SmolStr::from("abc"),
                    version: SmolStr::from("0.1.0"),
                    tier: SmolStr::from("tier1"),
                    timeout: Decimal::from(1),
                }),
            },
            TestCase {
                sql: r#"ALTER PLUGIN "abc" 0.1.0 REMOVE SERVICE "svc1" FROM TIER "tier1" option(timeout=11)"#,
                arena_type: ArenaType::Arena232,
                expected: PluginOwned::RemoveServiceFromTier(RemoveServiceFromTier {
                    service_name: SmolStr::from("svc1"),
                    plugin_name: SmolStr::from("abc"),
                    version: SmolStr::from("0.1.0"),
                    tier: SmolStr::from("tier1"),
                    timeout: Decimal::from(11),
                }),
            },
            TestCase {
                sql: r#"ALTER PLUGIN "abc" 0.1.0 SET "svc1"."key1" = '{"a": 1, "b": 2}' option(timeout=12)"#,
                arena_type: ArenaType::Arena136,
                expected: PluginOwned::ChangeConfig(ChangeConfig {
                    plugin_name: SmolStr::from("abc"),
                    version: SmolStr::from("0.1.0"),
                    key_value_grouped: vec![ServiceSettings {
                        name: SmolStr::from("svc1"),
                        pairs: vec![SettingsPair {
                            key: SmolStr::from("key1"),
                            value: SmolStr::from("{\"a\": 1, \"b\": 2}"),
                        }],
                    }],
                    timeout: Decimal::from(12),
                }),
            },
            TestCase {
                sql: r#"ALTER PLUGIN "abc" 0.1.0 SET "svc1"."key1" = 'a', "svc2"."key2" = 'b', "svc3"."key3" = 'c' option(timeout=11)"#,
                arena_type: ArenaType::Arena136,
                expected: PluginOwned::ChangeConfig(ChangeConfig {
                    plugin_name: SmolStr::from("abc"),
                    version: SmolStr::from("0.1.0"),
                    key_value_grouped: vec![
                        ServiceSettings {
                            name: SmolStr::from("svc1"),
                            pairs: vec![SettingsPair {
                                key: SmolStr::from("key1"),
                                value: SmolStr::from("a"),
                            }],
                        },
                        ServiceSettings {
                            name: SmolStr::from("svc2"),
                            pairs: vec![SettingsPair {
                                key: SmolStr::from("key2"),
                                value: SmolStr::from("b"),
                            }],
                        },
                        ServiceSettings {
                            name: SmolStr::from("svc3"),
                            pairs: vec![SettingsPair {
                                key: SmolStr::from("key3"),
                                value: SmolStr::from("c"),
                            }],
                        },
                    ],
                    timeout: Decimal::from(11),
                }),
            },
            TestCase {
                sql: r#"ALTER PLUGIN "abc" 0.1.0 SET "svc1"."key1" = 'a', "svc1"."key2" = 'b'"#,
                arena_type: ArenaType::Arena136,
                expected: PluginOwned::ChangeConfig(ChangeConfig {
                    plugin_name: SmolStr::from("abc"),
                    version: SmolStr::from("0.1.0"),
                    key_value_grouped: vec![ServiceSettings {
                        name: SmolStr::from("svc1"),
                        pairs: vec![
                            SettingsPair {
                                key: SmolStr::from("key1"),
                                value: SmolStr::from("a"),
                            },
                            SettingsPair {
                                key: SmolStr::from("key2"),
                                value: SmolStr::from("b"),
                            },
                        ],
                    }],
                    timeout: Decimal::from(10),
                }),
            },
        ];

        for tc in test_cases {
            let metadata = &RouterConfigurationMock::new();
            let plan: Plan = AbstractSyntaxTree::transform_into_plan(tc.sql, metadata).unwrap();
            let node = plan
                .get_plugin_node(NodeId {
                    offset: 0,
                    arena_type: tc.arena_type,
                })
                .unwrap()
                .get_plugin_owned();
            let node = if let PluginOwned::ChangeConfig(ChangeConfig {
                plugin_name,
                version,
                mut key_value_grouped,
                timeout,
            }) = node
            {
                key_value_grouped.sort();
                PluginOwned::ChangeConfig(ChangeConfig {
                    plugin_name,
                    version,
                    key_value_grouped,
                    timeout,
                })
            } else {
                node
            };

            assert_eq!(node, tc.expected, "from sql: `{}`", tc.sql);
        }
    }
}
