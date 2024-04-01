use ::raft::prelude as raft;
use protobuf::Message;

use crate::config::PicodataConfig;
use crate::instance::Instance;
use crate::replicaset::Replicaset;
use crate::schema;
use crate::schema::ADMIN_ID;
use crate::sql::pgproto;
use crate::storage;
use crate::storage::ClusterwideTable;
use crate::storage::PropertyName;
use crate::tier::Tier;
use crate::traft;
use crate::traft::op;
use std::collections::HashMap;

pub(super) fn prepare(
    config: &PicodataConfig,
    instance: &Instance,
    tiers: &HashMap<String, Tier>,
) -> Vec<raft::Entry> {
    let mut init_entries = Vec::new();
    let mut ops = vec![];

    //
    // Populate "_pico_address" and "_pico_instance" with info about the first instance
    //
    ops.push(
        op::Dml::replace(
            ClusterwideTable::Address,
            &traft::PeerAddress {
                raft_id: instance.raft_id,
                address: config.instance.advertise_address().to_host_port(),
            },
            ADMIN_ID,
        )
        .expect("serialization cannot fail"),
    );
    ops.push(
        op::Dml::insert(ClusterwideTable::Instance, &instance, ADMIN_ID)
            .expect("serialization cannot fail"),
    );
    ops.push(
        op::Dml::insert(
            ClusterwideTable::Replicaset,
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
            op::Dml::insert(ClusterwideTable::Tier, &tier, ADMIN_ID)
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

    //
    // Populate "_pico_property" with initial values for cluster-wide properties
    //
    let mut ops = vec![];
    ops.push(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::GlobalSchemaVersion, 0),
            ADMIN_ID,
        )
        .expect("serialization cannot fail"),
    );
    ops.push(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::NextSchemaVersion, 1),
            ADMIN_ID,
        )
        .expect("serialization cannot fail"),
    );

    #[rustfmt::skip]
    ops.push(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::PasswordMinLength, storage::DEFAULT_PASSWORD_MIN_LENGTH),
            ADMIN_ID,
        )
    .expect("serialization cannot fail"));

    #[rustfmt::skip]
    ops.push(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::PasswordEnforceUppercase, storage::DEFAULT_PASSWORD_ENFORCE_UPPERCASE),
            ADMIN_ID,
        )
    .expect("serialization cannot fail"));

    #[rustfmt::skip]
    ops.push(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::PasswordEnforceLowercase, storage::DEFAULT_PASSWORD_ENFORCE_LOWERCASE),
            ADMIN_ID,
        )
    .expect("serialization cannot fail"));

    #[rustfmt::skip]
    ops.push(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::PasswordEnforceDigits, storage::DEFAULT_PASSWORD_ENFORCE_DIGITS),
            ADMIN_ID,
        )
    .expect("serialization cannot fail"));

    #[rustfmt::skip]
    ops.push(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::PasswordEnforceSpecialchars, storage::DEFAULT_PASSWORD_ENFORCE_SPECIALCHARS),
            ADMIN_ID,
        )
    .expect("serialization cannot fail"));

    #[rustfmt::skip]
    ops.push(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::AutoOfflineTimeout, storage::DEFAULT_AUTO_OFFLINE_TIMEOUT),
            ADMIN_ID,
        )
    .expect("serialization cannot fail"));

    #[rustfmt::skip]
    ops.push(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::MaxHeartbeatPeriod, storage::DEFAULT_MAX_HEARTBEAT_PERIOD),
            ADMIN_ID,
        )
    .expect("serialization cannot fail"));

    #[rustfmt::skip]
    ops.push(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::MaxPgStatements, pgproto::DEFAULT_MAX_PG_STATEMENTS),
            ADMIN_ID,
        )
    .expect("serialization cannot fail"));

    #[rustfmt::skip]
    ops.push(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::MaxPgPortals, pgproto::DEFAULT_MAX_PG_PORTALS),
            ADMIN_ID,
        )
    .expect("serialization cannot fail"));

    #[rustfmt::skip]
    ops.push(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::SnapshotChunkMaxSize, storage::DEFAULT_SNAPSHOT_CHUNK_MAX_SIZE),
            ADMIN_ID,
        )
    .expect("serialization cannot fail"));

    #[rustfmt::skip]
    ops.push(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::SnapshotReadViewCloseTimeout, storage::DEFAULT_SNAPSHOT_READ_VIEW_CLOSE_TIMEOUT),
            ADMIN_ID,
        )
    .expect("serialization cannot fail"));

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
    // Populate "_pico_user" and "_pico_priv" to match tarantool ones
    //
    // Note: op::Dml is used instead of op::Acl because with Acl
    // replicas will attempt to apply these records to coresponding
    // tarantool spaces which is not needed
    let mut ops = vec![];
    for (user_def, privilege_defs) in &schema::system_user_definitions() {
        ops.push(
            op::Dml::insert(ClusterwideTable::User, user_def, ADMIN_ID)
                .expect("serialization cannot fail"),
        );

        for priv_def in privilege_defs {
            ops.push(
                op::Dml::insert(ClusterwideTable::Privilege, priv_def, ADMIN_ID)
                    .expect("serialization cannot fail"),
            );
        }
    }

    //
    // Populate "_pico_user" and "_pico_priv" to match tarantool ones
    //
    for (role_def, privilege_defs) in &schema::system_role_definitions() {
        ops.push(
            op::Dml::insert(ClusterwideTable::User, role_def, ADMIN_ID)
                .expect("serialization cannot fail"),
        );

        for priv_def in privilege_defs {
            ops.push(
                op::Dml::insert(ClusterwideTable::Privilege, priv_def, ADMIN_ID)
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
    // Populate "_pico_table" & "_pico_index" with defintions of builtins
    //
    let mut ops = vec![];
    for (table_def, index_defs) in schema::system_table_definitions() {
        ops.push(
            op::Dml::insert(ClusterwideTable::Table, &table_def, ADMIN_ID)
                .expect("serialization cannot fail"),
        );
        for index_def in index_defs {
            ops.push(
                op::Dml::insert(ClusterwideTable::Index, &index_def, ADMIN_ID)
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
    // Initial raft configuration
    //
    init_entries.push({
        let conf_change = raft::ConfChange {
            change_type: raft::ConfChangeType::AddNode,
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

    init_entries
}
