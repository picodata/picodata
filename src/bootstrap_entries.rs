use ::raft::prelude as raft;
use protobuf::Message;
use tarantool::auth::AuthData;
use tarantool::auth::AuthDef;
use tarantool::auth::AuthMethod;

use crate::cli::args;
use crate::instance::Instance;
use crate::schema::PrivilegeDef;
use crate::schema::RoleDef;
use crate::schema::UserDef;
use crate::schema::{ADMIN_ID, GUEST_ID, PUBLIC_ID, SUPER_ID};
use crate::sql::pgproto;
use crate::storage;
use crate::storage::ClusterwideTable;
use crate::storage::PropertyName;
use crate::tier::Tier;
use crate::traft;
use crate::traft::op;
use crate::traft::LogicalClock;

pub(super) fn prepare(args: &args::Run, instance: &Instance, tiers: &[Tier]) -> Vec<raft::Entry> {
    let mut lc = LogicalClock::new(instance.raft_id, 0);
    let mut init_entries = Vec::new();

    let mut init_entries_push_op = |dml: tarantool::Result<op::Dml>| {
        let dml = dml.expect("serialization cannot fail");

        lc.inc();

        let ctx = traft::EntryContextNormal {
            op: op::Op::from(dml),
            lc,
        };
        let e = traft::Entry {
            entry_type: raft::EntryType::EntryNormal,
            index: (init_entries.len() + 1) as _,
            term: traft::INIT_RAFT_TERM,
            data: vec![],
            context: Some(traft::EntryContext::Normal(ctx)),
        };

        init_entries.push(raft::Entry::try_from(e).unwrap());
    };

    // insert ourselves into global _pico_address and _pico_instance spaces
    init_entries_push_op(op::Dml::insert(
        ClusterwideTable::Address,
        &traft::PeerAddress {
            raft_id: instance.raft_id,
            address: args.advertise_address(),
        },
    ));
    init_entries_push_op(op::Dml::insert(ClusterwideTable::Instance, &instance));

    for tier in tiers {
        init_entries_push_op(op::Dml::insert(ClusterwideTable::Tier, &tier));
    }

    // populate initial values for cluster-wide properties
    // stored in _pico_property
    init_entries_push_op(op::Dml::insert(
        ClusterwideTable::Property,
        &(PropertyName::GlobalSchemaVersion, 0),
    ));
    init_entries_push_op(op::Dml::insert(
        ClusterwideTable::Property,
        &(PropertyName::NextSchemaVersion, 1),
    ));

    #[rustfmt::skip]
    init_entries_push_op(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::PasswordMinLength, storage::DEFAULT_PASSWORD_MIN_LENGTH),
        )
    );

    #[rustfmt::skip]
    init_entries_push_op(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::AutoOfflineTimeout, storage::DEFAULT_AUTO_OFFLINE_TIMEOUT),
        )
    );

    #[rustfmt::skip]
    init_entries_push_op(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::MaxHeartbeatPeriod, storage::DEFAULT_MAX_HEARTBEAT_PERIOD),
        )
    );

    #[rustfmt::skip]
    init_entries_push_op(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::MaxPgPortals, pgproto::DEFAULT_MAX_PG_PORTALS),
        )
    );

    #[rustfmt::skip]
    init_entries_push_op(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::SnapshotChunkMaxSize, storage::DEFAULT_SNAPSHOT_CHUNK_MAX_SIZE),
        )
    );

    #[rustfmt::skip]
    init_entries_push_op(
        op::Dml::insert(
            ClusterwideTable::Property,
            &(PropertyName::SnapshotReadViewCloseTimeout, storage::DEFAULT_SNAPSHOT_READ_VIEW_CLOSE_TIMEOUT),
        )
    );
    // Populate system roles and their privileges to match tarantool ones
    // Note: op::Dml is used instead of op::Acl because with Acl
    // replicas will attempt to apply these records to coresponding
    // tarantool spaces which is not needed
    // equivalent SQL expression: CREATE USER 'guest' WITH PASSWORD '' USING chap-sha1
    init_entries_push_op(op::Dml::insert(
        ClusterwideTable::User,
        &UserDef {
            id: GUEST_ID,
            name: String::from("guest"),
            schema_version: 0,
            auth: AuthDef::new(
                AuthMethod::ChapSha1,
                AuthData::new(&AuthMethod::ChapSha1, "guest", "").into_string(),
            ),
        },
    ));

    // equivalent SQL expression: CREATE USER 'admin' with PASSWORD 'no password, see below for more details' USING chap-sha1
    init_entries_push_op(op::Dml::insert(
        ClusterwideTable::User,
        &UserDef {
            id: ADMIN_ID,
            name: String::from("admin"),
            schema_version: 0,
            // this is a bit different from vanilla tnt
            // in vanilla tnt auth def is empty. Here for simplicity give navailable module api
            // we use ChapSha with invalid password
            // (its impossible to get empty string as output of sha1)
            auth: AuthDef::new(AuthMethod::ChapSha1, String::from("")),
        },
    ));

    // equivalent SQL expression: CREATE ROLE 'public'
    init_entries_push_op(op::Dml::insert(
        ClusterwideTable::Role,
        &RoleDef {
            id: PUBLIC_ID,
            name: String::from("public"),
            schema_version: 0,
        },
    ));

    // equivalent SQL expression: CREATE ROLE 'super'
    init_entries_push_op(op::Dml::insert(
        ClusterwideTable::Role,
        &RoleDef {
            id: SUPER_ID,
            name: String::from("super"),
            schema_version: 0,
        },
    ));

    // equivalent SQL expressions under 'admin' user:
    // GRANT <'usage', 'session'> ON 'universe' TO 'guest'
    // GRANT <'public', 'super'> TO 'guest'
    // GRANT 'all privileges' ON 'universe' TO 'admin'
    // GRANT 'all privileges' ON 'universe' TO 'super'
    for priv_def in PrivilegeDef::get_default_privileges() {
        init_entries_push_op(op::Dml::insert(ClusterwideTable::Privilege, priv_def));
    }

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
            context: None,
        };

        raft::Entry::try_from(e).unwrap()
    });

    init_entries
}
