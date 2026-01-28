use ::raft::prelude as raft;
use protobuf::Message;

use ::tarantool::msgpack;
use smol_str::SmolStr;

use crate::config::PicodataConfig;
use crate::config::{self};
use crate::info::PICODATA_VERSION;
use crate::instance::Instance;
use crate::replicaset::Replicaset;
use crate::schema;
use crate::schema::{ADMIN_ID, GUEST_ID, INITIAL_SCHEMA_VERSION, PUBLIC_ID};
use crate::storage::PropertyName;
use crate::storage::SystemTable;
use crate::storage::{self};
use crate::tier::Tier;
use crate::tlog;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::op;
use std::collections::HashMap;
use tarantool::auth::{AuthData, AuthDef, AuthMethod};

pub(super) fn prepare(
    config: &PicodataConfig,
    instance: &Instance,
    tiers: &HashMap<&str, Tier>,
    admin_auth: Option<AuthDef>,
) -> Result<Vec<raft::Entry>, Error> {
    let mut init_entries = Vec::new();
    let mut ops = vec![];
    let mut schema_version = INITIAL_SCHEMA_VERSION;

    //
    // Populate "_pico_address" and "_pico_instance" with info about the first instance
    //
    ops.push(
        op::Dml::replace(
            storage::PeerAddresses::TABLE_ID,
            &traft::PeerAddress {
                raft_id: instance.raft_id,
                address: config.instance.iproto_advertise().to_host_port(),
                connection_type: traft::ConnectionType::Iproto,
            },
            ADMIN_ID,
        )
        .expect("serialization cannot fail"),
    );
    ops.push(
        op::Dml::replace(
            storage::PeerAddresses::TABLE_ID,
            &traft::PeerAddress {
                raft_id: instance.raft_id,
                address: config.instance.pgproto_advertise().to_host_port(),
                connection_type: traft::ConnectionType::Pgproto,
            },
            ADMIN_ID,
        )
        .expect("serialization cannot fail"),
    );
    ops.push(
        op::Dml::insert(storage::Instances::TABLE_ID, &instance, ADMIN_ID)
            .expect("serialization cannot fail"),
    );
    ops.push(
        op::Dml::insert(
            storage::Replicasets::TABLE_ID,
            &Replicaset::with_one_instance(instance),
            ADMIN_ID,
        )
        .expect("serialization cannot fail"),
    );
    let context = traft::EntryContext::Op(op::Op::BatchDml { ops });
    init_entries.push(
        traft::Entry {
            entry_type: raft::EntryType::EntryNormal,
            index: (init_entries.len() + 1) as _,
            term: traft::INIT_RAFT_TERM,
            data: vec![],
            context,
        }
        .into(),
    );

    //
    // Populate "_pico_tier" with initial tiers
    //
    let mut ops = vec![];
    for tier in tiers.values() {
        ops.push(
            op::Dml::insert(storage::Tiers::TABLE_ID, &tier, ADMIN_ID)
                .expect("serialization cannot fail"),
        );
    }
    let context = traft::EntryContext::Op(op::Op::BatchDml { ops });
    init_entries.push(
        traft::Entry {
            entry_type: raft::EntryType::EntryNormal,
            index: (init_entries.len() + 1) as _,
            term: traft::INIT_RAFT_TERM,
            data: vec![],
            context,
        }
        .into(),
    );

    #[allow(unused_mut)]
    let mut version = SmolStr::new_static(PICODATA_VERSION);
    #[cfg(feature = "error_injection")]
    crate::error_injection!("BOOT_PICODATA_VERSION" => |v| {
        version = v.into();
    });
    //
    // Populate "_pico_property" with initial values for cluster-wide properties
    //
    let mut ops = vec![
        op::Dml::insert(
            storage::Properties::TABLE_ID,
            &(PropertyName::GlobalSchemaVersion, INITIAL_SCHEMA_VERSION),
            ADMIN_ID,
        )
        .expect("serialization cannot fail"),
        op::Dml::insert(
            storage::Properties::TABLE_ID,
            &(PropertyName::NextSchemaVersion, INITIAL_SCHEMA_VERSION + 1),
            ADMIN_ID,
        )
        .expect("serialization cannot fail"),
        op::Dml::insert(
            storage::Properties::TABLE_ID,
            &(
                PropertyName::SystemCatalogVersion,
                storage::LATEST_SYSTEM_CATALOG_VERSION,
            ),
            ADMIN_ID,
        )
        .expect("serialization cannot fail"),
        op::Dml::insert(
            storage::Properties::TABLE_ID,
            &(PropertyName::ClusterVersion, version.to_string()),
            ADMIN_ID,
        )
        .expect("serialization cannot fail"),
    ];

    let tier_names = tiers.keys().map(AsRef::as_ref).collect::<Vec<_>>();

    //
    // Populate "_pico_db_config" with initial values for cluster-wide properties
    //
    let db_config_entries = config::get_defaults_for_all_alter_system_parameters(&tier_names)?;

    ops.extend(db_config_entries);

    let context = traft::EntryContext::Op(op::Op::BatchDml { ops });
    init_entries.push(
        traft::Entry {
            entry_type: raft::EntryType::EntryNormal,
            index: (init_entries.len() + 1) as _,
            term: traft::INIT_RAFT_TERM,
            data: vec![],
            context,
        }
        .into(),
    );

    //
    // Unchangeable configs with cluster scope
    //
    {
        let mut ops = vec![];
        for config_tuple in config.cluster.bootstrap_configs() {
            ops.push(
                op::Dml::insert(storage::DbConfig::TABLE_ID, &config_tuple, ADMIN_ID)
                    .expect("serialization can't fail"),
            );
        }
        let context = traft::EntryContext::Op(op::Op::BatchDml { ops });
        init_entries.push(
            traft::Entry {
                entry_type: raft::EntryType::EntryNormal,
                index: (init_entries.len() + 1) as _,
                term: traft::INIT_RAFT_TERM,
                data: vec![],
                context,
            }
            .into(),
        );
    }

    // Populate "_pico_user" and "_pico_priv" to match tarantool ones
    //
    // Note: op::Dml is used instead of op::Acl because with Acl
    // replicas will attempt to apply these records to corresponding
    // tarantool spaces which is not needed
    let mut ops = vec![];
    for (user_def, privilege_defs) in &schema::system_user_definitions() {
        ops.push(
            op::Dml::insert(storage::Users::TABLE_ID, user_def, ADMIN_ID)
                .expect("serialization cannot fail"),
        );

        for priv_def in privilege_defs {
            ops.push(
                op::Dml::insert(storage::Privileges::TABLE_ID, priv_def, ADMIN_ID)
                    .expect("serialization cannot fail"),
            );
        }
    }

    //
    // Populate "_pico_user" and "_pico_priv" to match tarantool ones
    //
    for (role_def, privilege_defs) in &schema::system_role_definitions() {
        ops.push(
            op::Dml::insert(storage::Users::TABLE_ID, role_def, ADMIN_ID)
                .expect("serialization cannot fail"),
        );

        for priv_def in privilege_defs {
            ops.push(
                op::Dml::insert(storage::Privileges::TABLE_ID, priv_def, ADMIN_ID)
                    .expect("serialization cannot fail"),
            );
        }
    }
    let context = traft::EntryContext::Op(op::Op::BatchDml { ops });
    init_entries.push(
        traft::Entry {
            entry_type: raft::EntryType::EntryNormal,
            index: (init_entries.len() + 1) as _,
            term: traft::INIT_RAFT_TERM,
            data: vec![],
            context,
        }
        .into(),
    );

    //
    // Set up password for admin (see config::get_admin_auth_def_from_env for details)
    //
    if let Some(admin_auth) = admin_auth {
        schema_version += 1;
        let op_elem = op::Op::Acl(op::Acl::ChangeAuth {
            user_id: ADMIN_ID,
            auth: admin_auth,
            initiator: ADMIN_ID,
            schema_version,
        });

        let context = traft::EntryContext::Op(op_elem);
        init_entries.push(
            traft::Entry {
                entry_type: raft::EntryType::EntryNormal,
                index: (init_entries.len() + 1) as _,
                term: traft::INIT_RAFT_TERM,
                data: vec![],
                context,
            }
            .into(),
        );
        tlog!(Info, "Password for user=admin has been set successfully");
    }

    schema_version += 1;
    let op_elem = op::Op::Acl(op::Acl::ChangeAuth {
        user_id: GUEST_ID,
        auth: AuthDef::new(
            AuthMethod::Md5,
            AuthData::new(&AuthMethod::Md5, "guest", "").into_string(),
        ),
        initiator: ADMIN_ID,
        schema_version,
    });

    let context = traft::EntryContext::Op(op_elem);
    init_entries.push(
        traft::Entry {
            entry_type: raft::EntryType::EntryNormal,
            index: (init_entries.len() + 1) as _,
            term: traft::INIT_RAFT_TERM,
            data: vec![],
            context,
        }
        .into(),
    );

    //
    // Populate "_pico_table" & "_pico_index" with definitions of builtins
    //
    let mut ops = vec![];
    for (table_def, index_defs) in schema::system_table_definitions() {
        ops.push(
            op::Dml::insert_raw(
                crate::catalog::pico_table::PicoTable::TABLE_ID,
                msgpack::encode(&table_def),
                ADMIN_ID,
            )
            .expect("serialization cannot fail"),
        );
        for index_def in index_defs {
            ops.push(
                op::Dml::insert(storage::Indexes::TABLE_ID, &index_def, ADMIN_ID)
                    .expect("serialization cannot fail"),
            );
        }
    }
    let context = traft::EntryContext::Op(op::Op::BatchDml { ops });
    init_entries.push(
        traft::Entry {
            entry_type: raft::EntryType::EntryNormal,
            index: (init_entries.len() + 1) as _,
            term: traft::INIT_RAFT_TERM,
            data: vec![],
            context,
        }
        .into(),
    );

    // Grant bootstrap privileges for role PUBLIC
    let privileges = [
        (
            schema::PrivilegeType::Read,
            storage::Instances::TABLE_ID as i64,
        ),
        (
            schema::PrivilegeType::Read,
            storage::PeerAddresses::TABLE_ID as i64,
        ),
    ];
    for (privilege, object_id) in privileges.iter() {
        schema_version += 1;
        let priv_def = schema::PrivilegeDef::new(
            ADMIN_ID,
            PUBLIC_ID,
            *privilege,
            schema::SchemaObjectType::Table,
            *object_id,
            schema_version,
        )
        .expect("privilege definition should be valid");

        let acl_query = op::Acl::GrantPrivilege { priv_def };
        let context = traft::EntryContext::Op(op::Op::Acl(acl_query));
        init_entries.push(
            traft::Entry {
                entry_type: raft::EntryType::EntryNormal,
                index: (init_entries.len() + 1) as _,
                term: traft::INIT_RAFT_TERM,
                data: vec![],
                context,
            }
            .into(),
        );
    }

    //
    // Initial raft configuration
    //
    init_entries.push({
        let conf_change = raft::ConfChange {
            change_type: raft::ConfChangeType::AddNode.into(),
            node_id: instance.raft_id,
            ..Default::default()
        };
        let e = traft::Entry {
            entry_type: raft::EntryType::EntryConfChange,
            index: (init_entries.len() + 1) as _,
            term: traft::INIT_RAFT_TERM,
            data: conf_change.write_to_bytes().unwrap(),
            context: traft::EntryContext::None,
        };

        e.into()
    });

    Ok(init_entries)
}
