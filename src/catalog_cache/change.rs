use super::index::index_change;
use super::table::table_change;
use crate::catalog::pico_bucket::{BucketRecord, PicoBucket};
use crate::catalog::pico_resharding_state::{PicoReshardingState, ReshardingStateRecord};
use crate::catalog::pico_table::PicoTable;
use crate::instance::Instance;
use crate::replicaset::Replicaset;
use crate::schema::{IndexDef, ServiceRouteItem, TableDef};
use crate::storage::{Indexes, Instances, Replicasets, ServiceRouteTable, SystemTable as _, Tiers};
use crate::tier::Tier;
use crate::traft::op::{Dml, Op};
use crate::traft::Result;
use crate::traft::{self, EntryContext};
use tarantool::index::IndexId;
use tarantool::space::{SpaceId, SystemSpace};
use tarantool::tuple::{DecodeOwned, Tuple};

////////////////////////////////////////////////////////////////////////////////
// CatalogChange
////////////////////////////////////////////////////////////////////////////////

/// The `old`/`new` pair of an applied DML operation to one of the topology
/// system tables.
///
/// Used in [`CatalogCache::update`](super::CatalogCache::update).
pub(crate) enum CatalogChange {
    Instance {
        old: Option<Instance>,
        new: Option<Instance>,
    },
    Replicaset {
        old: Option<Replicaset>,
        new: Option<Replicaset>,
    },
    Tier {
        old: Option<Tier>,
        new: Option<Tier>,
    },
    ServiceRoute {
        old: Option<ServiceRouteItem>,
        new: Option<ServiceRouteItem>,
    },
    BucketRecord {
        old: Option<BucketRecord>,
        new: Option<BucketRecord>,
    },
    ReshardingStateRecord {
        old: Option<ReshardingStateRecord>,
        new: Option<ReshardingStateRecord>,
    },
    /// The definition of table `id` is now `new`.
    Table { id: SpaceId, new: Option<TableDef> },
    /// The definition of index `key` is now `new`.
    Index {
        key: (SpaceId, IndexId),
        new: Option<IndexDef>,
    },
}

impl CatalogChange {
    pub(crate) fn decode(
        table_id: SpaceId,
        old: Option<&Tuple>,
        new: Option<&Tuple>,
    ) -> Result<Option<Self>> {
        match table_id {
            Instances::TABLE_ID => {
                let (old, new) = decode_pair(old, new)?;
                Ok(Some(Self::Instance { old, new }))
            }
            Replicasets::TABLE_ID => {
                let (old, new) = decode_pair(old, new)?;
                Ok(Some(Self::Replicaset { old, new }))
            }
            Tiers::TABLE_ID => {
                let (old, new) = decode_pair(old, new)?;
                Ok(Some(Self::Tier { old, new }))
            }
            ServiceRouteTable::TABLE_ID => {
                let (old, new) = decode_pair(old, new)?;
                Ok(Some(Self::ServiceRoute { old, new }))
            }
            PicoBucket::TABLE_ID => {
                let (old, new) = decode_pair(old, new)?;
                Ok(Some(Self::BucketRecord { old, new }))
            }
            PicoReshardingState::TABLE_ID => {
                let (old, new) = decode_pair(old, new)?;
                Ok(Some(Self::ReshardingStateRecord { old, new }))
            }
            // Unlike the tables above, a row which can't be decoded panics with
            // the table and the key: `NodeImpl::handle_dml_entry` would turn an
            // error into retrying the entry forever.
            PicoTable::TABLE_ID => {
                let change = table_change(old, new);
                Ok(change.map(|(id, new)| Self::Table { id, new }))
            }
            Indexes::TABLE_ID => {
                let change = index_change(old, new);
                Ok(change.map(|(key, new)| Self::Index { key, new }))
            }
            _ => return Ok(None),
        }
    }
}

fn decode_pair<T: DecodeOwned>(
    old: Option<&Tuple>,
    new: Option<&Tuple>,
) -> Result<(Option<T>, Option<T>)> {
    let old = old.map(|t| t.decode()).transpose()?;
    let new = new.map(|t| t.decode()).transpose()?;
    Ok((old, new))
}

////////////////////////////////////////////////////////////////////////////////
// debug check
////////////////////////////////////////////////////////////////////////////////

/// Whether applying `entry` may change `_pico_table` or `_pico_index`: any
/// entry except empty ones and DML into other tables. Doesn't depend on
/// whether the cache noticed a change, as a write bypassing it would bypass
/// such a flag as well.
pub fn may_change_catalog(entry: &traft::Entry) -> bool {
    let is_catalog = |dml: &Dml| {
        let id = dml.table_id();
        id == PicoTable::TABLE_ID
            || id == Indexes::TABLE_ID
            || id == SystemSpace::Truncate as SpaceId
    };
    match &entry.context {
        EntryContext::None => entry.entry_type != ::raft::prelude::EntryType::EntryNormal,
        EntryContext::Op(Op::Nop) => false,
        EntryContext::Op(Op::Dml(dml)) => is_catalog(dml),
        EntryContext::Op(Op::BatchDml { ops }) => ops.iter().any(is_catalog),
        EntryContext::Op(_) => true,
    }
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

mod tests {
    use super::*;

    #[::tarantool::test]
    fn entries_which_may_change_catalog() {
        let entry = |context| traft::Entry {
            entry_type: ::raft::prelude::EntryType::EntryNormal,
            index: 1,
            term: 1,
            data: vec![],
            context,
        };
        let op = |op| entry(EntryContext::Op(op));
        let dml = |table: SpaceId| Dml::delete(table, &[1], crate::schema::ADMIN_ID, None).unwrap();

        assert!(!may_change_catalog(&entry(EntryContext::None)));
        assert!(!may_change_catalog(&op(Op::Nop)));
        assert!(!may_change_catalog(&op(Op::Dml(dml(516)))));
        let batch = Op::BatchDml {
            ops: vec![dml(516), dml(515)],
        };
        assert!(!may_change_catalog(&op(batch)));

        assert!(may_change_catalog(&op(Op::DdlCommit)));
        for table in [512, 513, 330] {
            assert!(may_change_catalog(&op(Op::Dml(dml(table)))));
            let batch = Op::BatchDml {
                ops: vec![dml(516), dml(table)],
            };
            assert!(may_change_catalog(&op(batch)));
        }
        let conf_change = traft::Entry {
            entry_type: ::raft::prelude::EntryType::EntryConfChange,
            ..entry(EntryContext::None)
        };
        assert!(may_change_catalog(&conf_change));
    }
}
