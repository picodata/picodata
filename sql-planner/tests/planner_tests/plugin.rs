use smol_str::SmolStr;
use sql::executor::engine::mock::RouterConfigurationMock;
use sql::frontend::sql::transform_into_plan;
use sql::ir::node::plugin::{
    get_default_timeout, AppendServiceToTier, ChangeConfig, CreatePlugin, DisablePlugin,
    DropPlugin, EnablePlugin, MigrateTo, MigrateToOpts, PluginOwned, RemoveServiceFromTier,
    ServiceSettings, SettingsPair,
};
use sql::ir::node::{ArenaType, NodeId};
use sql::ir::options::Timeout;
use sql::ir::Plan;

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
                timeout: Timeout::from_secs(1),
            }),
        },
        TestCase {
            sql: r#"ALTER PLUGIN "abc" 1.1.1 ENABLE"#,
            arena_type: ArenaType::Arena96,
            expected: PluginOwned::Enable(EnablePlugin {
                name: SmolStr::from("abc"),
                version: SmolStr::from("1.1.1"),
                timeout: get_default_timeout(),
            }),
        },
        TestCase {
            sql: r#"ALTER PLUGIN "abc" 1.1.1 ENABLE option(timeout=1)"#,
            arena_type: ArenaType::Arena96,
            expected: PluginOwned::Enable(EnablePlugin {
                name: SmolStr::from("abc"),
                version: SmolStr::from("1.1.1"),
                timeout: Timeout::from_secs(1),
            }),
        },
        TestCase {
            sql: r#"ALTER PLUGIN "abc" 1.1.1 DISABLE option(timeout=1)"#,
            arena_type: ArenaType::Arena96,
            expected: PluginOwned::Disable(DisablePlugin {
                name: SmolStr::from("abc"),
                version: SmolStr::from("1.1.1"),
                timeout: Timeout::from_secs(1),
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
                timeout: get_default_timeout(),
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
                timeout: Timeout::from_secs(1),
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
                timeout: Timeout::from_secs(11),
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
                timeout: get_default_timeout(),
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
                    timeout: Timeout::from_secs(11),
                    rollback_timeout: Timeout::from_secs(12),
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
                timeout: Timeout::from_secs(1),
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
                timeout: Timeout::from_secs(11),
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
                timeout: Timeout::from_secs(12),
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
                timeout: Timeout::from_secs(11),
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
                timeout: get_default_timeout(),
            }),
        },
    ];

    for tc in test_cases {
        let metadata = &RouterConfigurationMock::new();
        let plan: Plan = transform_into_plan(tc.sql, &[], metadata).unwrap();
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
