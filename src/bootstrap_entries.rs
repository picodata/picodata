use ::raft::prelude as raft;
use protobuf::Message;

use crate::cli::args;
use crate::instance::Instance;
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
