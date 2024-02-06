use ::raft::prelude as raft;
use protobuf::Message;

use crate::cli::args;
use crate::instance::Instance;
use crate::schema;
use crate::schema::TableDef;
use crate::schema::ADMIN_ID;
use crate::sql::pgproto;
use crate::storage;
use crate::storage::ClusterwideTable;
use crate::storage::PropertyName;
use crate::tier::Tier;
use crate::traft;
use crate::traft::op;

pub(super) fn prepare(args: &args::Run, instance: &Instance, tiers: &[Tier]) -> Vec<raft::Entry> {
    let mut init_entries = Vec::new();

    let mut init_entries_push_op = |dml: tarantool::Result<op::Dml>| {
        let dml = dml.expect("serialization cannot fail");

        let context = traft::EntryContext::Op(op::Op::from(dml));
        let e = traft::Entry {
            entry_type: raft::EntryType::EntryNormal,
            index: (init_entries.len() + 1) as _,
            term: traft::INIT_RAFT_TERM,
            data: vec![],
            context,
        };

        init_entries.push(e.into());
    };

    //
    // Populate "_pico_address" and "_pico_instance" with info about the first instance
    //
    init_entries_push_op(op::Dml::insert(
        ClusterwideTable::Address,
        &traft::PeerAddress {
            raft_id: instance.raft_id,
            address: args.advertise_address(),
        },
        ADMIN_ID,
    ));
    init_entries_push_op(op::Dml::insert(
        ClusterwideTable::Instance,
        &instance,
        ADMIN_ID,
    ));

    //
    // Populate "_pico_tier" with initial tiers
    //
    for tier in tiers {
        init_entries_push_op(op::Dml::insert(ClusterwideTable::Tier, &tier, ADMIN_ID));
    }

    //
    // Populate "_pico_property" with initial values for cluster-wide properties
    //
    init_entries_push_op(op::Dml::insert(
        ClusterwideTable::Property,
        &(PropertyName::GlobalSchemaVersion, 0),
        ADMIN_ID,
    ));
    init_entries_push_op(op::Dml::insert(
        ClusterwideTable::Property,
        &(PropertyName::NextSchemaVersion, 1),
        ADMIN_ID,
    ));

    #[rustfmt::skip]
    init_entries_push_op(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::PasswordMinLength, storage::DEFAULT_PASSWORD_MIN_LENGTH),
            ADMIN_ID,
        )
    );

    #[rustfmt::skip]
    init_entries_push_op(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::AutoOfflineTimeout, storage::DEFAULT_AUTO_OFFLINE_TIMEOUT),
            ADMIN_ID,
        )
    );

    #[rustfmt::skip]
    init_entries_push_op(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::MaxHeartbeatPeriod, storage::DEFAULT_MAX_HEARTBEAT_PERIOD),
            ADMIN_ID,
        )
    );

    #[rustfmt::skip]
    init_entries_push_op(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::MaxPgPortals, pgproto::DEFAULT_MAX_PG_PORTALS),
            ADMIN_ID,
        )
    );

    #[rustfmt::skip]
    init_entries_push_op(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::SnapshotChunkMaxSize, storage::DEFAULT_SNAPSHOT_CHUNK_MAX_SIZE),
            ADMIN_ID,
        )
    );

    #[rustfmt::skip]
    init_entries_push_op(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::SnapshotReadViewCloseTimeout, storage::DEFAULT_SNAPSHOT_READ_VIEW_CLOSE_TIMEOUT),
            ADMIN_ID,
        )
    );

    //
    // Populate "_pico_user" and "_pico_priv" to match tarantool ones
    //
    // Note: op::Dml is used instead of op::Acl because with Acl
    // replicas will attempt to apply these records to coresponding
    // tarantool spaces which is not needed
    for (user_def, privilege_defs) in &schema::system_user_definitions() {
        init_entries_push_op(op::Dml::insert(ClusterwideTable::User, user_def, ADMIN_ID));

        for priv_def in privilege_defs {
            init_entries_push_op(op::Dml::insert(
                ClusterwideTable::Privilege,
                priv_def,
                ADMIN_ID,
            ));
        }
    }

    //
    // Populate "_pico_role" and "_pico_priv" to match tarantool ones
    //
    for (role_def, privilege_defs) in &schema::system_role_definitions() {
        init_entries_push_op(op::Dml::insert(ClusterwideTable::Role, &role_def, ADMIN_ID));

        for priv_def in privilege_defs {
            init_entries_push_op(op::Dml::insert(
                ClusterwideTable::Privilege,
                priv_def,
                ADMIN_ID,
            ));
        }
    }

    // Builtin global table definitions
    for table_def in TableDef::system_tables() {
        init_entries_push_op(op::Dml::insert(
            ClusterwideTable::Table,
            &table_def,
            ADMIN_ID,
        ));
    }

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
