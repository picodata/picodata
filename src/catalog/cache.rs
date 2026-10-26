//! Table and index definitions from `_pico_table` and `_pico_index`, kept in
//! [`CatalogCache`] next to the topology for the hot paths of SQL and CAS.
//! Other code reads the tables through `PicoTable` and `Indexes`.
//!
//! The definitions are loaded with the rest of the cache when the node starts.
//! After that the code which writes the tables changes them: the write methods
//! of `PicoTable` and `Indexes` take the cache and pass it the row tarantool
//! returned (or the deleted key) right after writing it, DML from the raft log
//! goes through [`CatalogChange`], truncation of the tables and the raft
//! snapshot change the cache explicitly. Within a transaction nothing yields
//! between writing a row and changing the cache, so other fibers always see
//! the cache equal to the tables, including the rows of a transaction waiting
//! for WAL. Every raft entry up to `applied` is reflected in the cache, so no
//! separate index is needed.
//!
//! There is no rollback handling. A failed commit of a raft entry or a snapshot
//! crashes the instance, and the cache is loaded again on restart. Code which
//! writes the catalog in a transaction which can roll back without a crash must
//! call [`CatalogCache::full_reload`] on error.
//!
//! Writes bypassing picodata (from the console, `pico.cas` into tarantool's
//! system spaces) aren't reflected until restart. Debug builds compare the
//! cache with the tables after raft entries ([`may_change_catalog`]).
//!
//! Rules for readers holding a reference returned by [`CatalogCache::get`]:
//! - don't yield while holding it (debug builds catch this);
//! - don't write to `_pico_table` and `_pico_index` while holding it, even if it
//!   was taken for the topology: the write methods change the cache, and the
//!   instance will panic.
//!
//! [`CatalogCache`]: crate::catalog_cache::CatalogCache
//! [`CatalogCache::get`]: crate::catalog_cache::CatalogCache::get
//! [`CatalogCache::full_reload`]: crate::catalog_cache::CatalogCache::full_reload
//! [`CatalogChange`]: crate::catalog_cache::CatalogChange

use crate::catalog::pico_table::PicoTable;
use crate::schema::{IndexDef, TableDef};
use crate::storage::{Catalog, Indexes, SystemTable as _};
use crate::traft::op::{Dml, Op};
use crate::traft::{self, EntryContext};
use ahash::AHashMap;
use smol_str::SmolStr;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use tarantool::index::{IndexId, IteratorType};
use tarantool::space::{SpaceId, SystemSpace};
use tarantool::tuple::Tuple;

////////////////////////////////////////////////////////////////////////////////
// TableCache, IndexCache
////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct TableCache {
    by_id: AHashMap<SpaceId, TableDef>,
    by_name: AHashMap<SmolStr, SpaceId>,
}

#[derive(Default)]
pub struct IndexCache {
    /// Ordered like the primary key of `_pico_index`.
    by_key: BTreeMap<(SpaceId, IndexId), IndexDef>,
    by_name: AHashMap<SmolStr, (SpaceId, IndexId)>,
}

impl TableCache {
    /// Replaces the definition with id `id`. A name is removed only if it
    /// still points to this key, so changes may be applied in any order.
    pub(crate) fn set(&mut self, id: SpaceId, def: Option<TableDef>) {
        if let Some(old) = self.by_id.remove(&id) {
            if self.by_name.get(&old.name) == Some(&id) {
                self.by_name.remove(&old.name);
            }
        }
        if let Some(def) = def {
            self.by_name.insert(def.name.clone(), id);
            self.by_id.insert(id, def);
        }
    }

    /// A row of `_pico_table` returned by tarantool after a write.
    pub(crate) fn put(&mut self, tuple: &Tuple) {
        let def = decode_table(tuple);
        self.set(def.id, Some(def));
    }

    #[inline]
    pub fn get(&self, id: SpaceId) -> Option<&TableDef> {
        self.by_id.get(&id)
    }

    #[inline]
    pub fn get_by_name(&self, name: &str) -> Option<&TableDef> {
        let id = self.by_name.get(name)?;
        self.by_id.get(id)
    }

    /// Reads `_pico_table` of `storage` in full.
    pub(crate) fn load(storage: &Catalog) -> tarantool::Result<Self> {
        let mut tables = Self::default();
        for tuple in storage.pico_table.space.select_v2(IteratorType::All, &())? {
            tables.put(&tuple?);
        }
        Ok(tables)
    }

    /// Returns a description of the first difference from `actual`.
    pub(crate) fn diff(&self, actual: &Self) -> Option<String> {
        let table = PicoTable::TABLE_NAME;
        first_diff(table, &self.by_id, &actual.by_id)
            .or_else(|| first_diff(table, &self.by_name, &actual.by_name))
    }
}

impl IndexCache {
    /// Replaces the definition with key `key`, see [`TableCache::set`].
    pub(crate) fn set(&mut self, key: (SpaceId, IndexId), def: Option<IndexDef>) {
        if let Some(old) = self.by_key.remove(&key) {
            if self.by_name.get(&old.name) == Some(&key) {
                self.by_name.remove(&old.name);
            }
        }
        if let Some(def) = def {
            self.by_name.insert(def.name.clone(), key);
            self.by_key.insert(key, def);
        }
    }

    /// A row of `_pico_index` returned by tarantool after a write.
    pub(crate) fn put(&mut self, tuple: &Tuple) {
        let def = decode_index(tuple);
        self.set((def.table_id, def.id), Some(def));
    }

    #[inline]
    pub fn get(&self, table_id: SpaceId, id: IndexId) -> Option<&IndexDef> {
        self.by_key.get(&(table_id, id))
    }

    #[inline]
    pub fn get_by_name(&self, name: &str) -> Option<&IndexDef> {
        let key = self.by_name.get(name)?;
        self.by_key.get(key)
    }

    /// Indexes of table `table_id` ordered by index id.
    #[inline]
    pub fn of_table(&self, table_id: SpaceId) -> impl Iterator<Item = &IndexDef> {
        self.by_key
            .range((table_id, 0)..=(table_id, IndexId::MAX))
            .map(|(_, def)| def)
    }

    /// Reads `_pico_index` of `storage` in full.
    pub(crate) fn load(storage: &Catalog) -> tarantool::Result<Self> {
        let mut indexes = Self::default();
        for tuple in storage.indexes.space.select_v2(IteratorType::All, &())? {
            indexes.put(&tuple?);
        }
        Ok(indexes)
    }

    /// Returns a description of the first difference from `actual`.
    pub(crate) fn diff(&self, actual: &Self) -> Option<String> {
        let table = Indexes::TABLE_NAME;
        first_diff(table, &self.by_key, &actual.by_key)
            .or_else(|| first_diff(table, &self.by_name, &actual.by_name))
    }
}

fn first_diff<'a, K, V>(
    table: &str,
    cached: impl IntoIterator<Item = (&'a K, &'a V)>,
    actual: impl IntoIterator<Item = (&'a K, &'a V)>,
) -> Option<String>
where
    K: Ord + Debug + 'a,
    V: PartialEq + 'a,
{
    let cached: BTreeMap<_, _> = cached.into_iter().collect();
    let actual: BTreeMap<_, _> = actual.into_iter().collect();
    let keys: BTreeSet<_> = cached.keys().chain(actual.keys()).collect();
    let key = keys
        .into_iter()
        .find(|k| cached.get(*k) != actual.get(*k))?;
    Some(format!("catalog cache differs from {table} at key {key:?}"))
}

////////////////////////////////////////////////////////////////////////////////
// decoding
////////////////////////////////////////////////////////////////////////////////

fn decode_table(tuple: &Tuple) -> TableDef {
    tarantool::msgpack::decode(&tuple.to_vec()).unwrap_or_else(|e| {
        let id = table_key(tuple);
        panic!(
            "failed to decode {} row with id {id}: {e}",
            PicoTable::TABLE_NAME
        )
    })
}

fn decode_index(tuple: &Tuple) -> IndexDef {
    tuple.decode().unwrap_or_else(|e| {
        let key = index_key(tuple);
        panic!(
            "failed to decode {} row with key {key:?}: {e}",
            Indexes::TABLE_NAME
        )
    })
}

/// What DML into `_pico_table` did: the definition with the returned id is now
/// the returned one. `new` is the row tarantool returned, `old` the row before
/// the DML. `None` if nothing changed: an update of a missing key, an insert
/// skipped on conflict.
pub(crate) fn table_change(
    old: Option<&Tuple>,
    new: Option<&Tuple>,
) -> Option<(SpaceId, Option<TableDef>)> {
    match (new, old) {
        (Some(new), _) => {
            let def = decode_table(new);
            Some((def.id, Some(def)))
        }
        // A key out of `u32` isn't in the cache.
        (None, Some(old)) => Some((table_key(old).try_into().ok()?, None)),
        (None, None) => None,
    }
}

/// Same as [`table_change`] for `_pico_index`.
pub(crate) fn index_change(
    old: Option<&Tuple>,
    new: Option<&Tuple>,
) -> Option<((SpaceId, IndexId), Option<IndexDef>)> {
    match (new, old) {
        (Some(new), _) => {
            let def = decode_index(new);
            Some(((def.table_id, def.id), Some(def)))
        }
        // A key out of `u32` isn't in the cache.
        (None, Some(old)) => {
            let (table_id, id) = index_key(old);
            Some(((table_id.try_into().ok()?, id.try_into().ok()?), None))
        }
        (None, None) => None,
    }
}

fn table_key(tuple: &Tuple) -> u64 {
    tuple
        .field(0)
        .ok()
        .flatten()
        .expect("primary key of _pico_table should be an unsigned integer")
}

fn index_key(tuple: &Tuple) -> (u64, u64) {
    let field = |i| tuple.field(i).ok().flatten();
    field(0)
        .zip(field(1))
        .expect("primary key of _pico_index should be a pair of unsigned integers")
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
    use crate::catalog_cache::CatalogCache;
    use tarantool::tuple::RawBytes;

    fn table(id: SpaceId, name: &str, schema_version: u64) -> TableDef {
        TableDef {
            id,
            name: name.into(),
            schema_version,
            ..TableDef::for_tests()
        }
    }

    fn index(table_id: SpaceId, id: IndexId, name: &str) -> IndexDef {
        IndexDef {
            table_id,
            id,
            name: name.into(),
            ..IndexDef::for_tests()
        }
    }

    fn table_names(tables: &TableCache) -> Vec<(&str, SpaceId)> {
        let mut names: Vec<_> = (tables.by_name.iter())
            .map(|(name, id)| (name.as_str(), *id))
            .collect();
        names.sort();
        names
    }

    fn index_names(indexes: &IndexCache) -> Vec<(&str, (SpaceId, IndexId))> {
        let mut names: Vec<_> = (indexes.by_name.iter())
            .map(|(name, key)| (name.as_str(), *key))
            .collect();
        names.sort();
        names
    }

    #[::tarantool::test]
    fn apply_table_changes() {
        let mut tables = TableCache::default();

        tables.set(1, Some(table(1, "a", 1)));
        tables.set(2, Some(table(2, "b", 1)));
        assert_eq!(tables.get(1), Some(&table(1, "a", 1)));
        assert_eq!(tables.get_by_name("b"), Some(&table(2, "b", 1)));
        assert_eq!(table_names(&tables), [("a", 1), ("b", 2)]);

        // Rename.
        tables.set(1, Some(table(1, "c", 2)));
        assert_eq!(tables.get_by_name("a"), None);
        assert_eq!(tables.get_by_name("c"), Some(&table(1, "c", 2)));
        assert_eq!(table_names(&tables), [("b", 2), ("c", 1)]);

        // Delete, then rename into the freed name.
        tables.set(2, None);
        assert_eq!(tables.get(2), None);
        assert_eq!(tables.get_by_name("b"), None);
        tables.set(1, Some(table(1, "b", 3)));
        assert_eq!(tables.get_by_name("b"), Some(&table(1, "b", 3)));
        assert_eq!(table_names(&tables), [("b", 1)]);
    }

    #[::tarantool::test]
    fn apply_index_changes() {
        let mut indexes = IndexCache::default();

        indexes.set((1, 0), Some(index(1, 0, "a")));
        indexes.set((1, 1), Some(index(1, 1, "b")));
        assert_eq!(indexes.get(1, 0), Some(&index(1, 0, "a")));
        assert_eq!(indexes.get_by_name("b"), Some(&index(1, 1, "b")));
        assert_eq!(index_names(&indexes), [("a", (1, 0)), ("b", (1, 1))]);

        // Rename.
        indexes.set((1, 0), Some(index(1, 0, "c")));
        assert_eq!(indexes.get_by_name("a"), None);
        assert_eq!(index_names(&indexes), [("b", (1, 1)), ("c", (1, 0))]);

        // Delete, then rename into the freed name.
        indexes.set((1, 1), None);
        assert_eq!(indexes.get(1, 1), None);
        assert_eq!(indexes.get_by_name("b"), None);
        indexes.set((1, 0), Some(index(1, 0, "b")));
        assert_eq!(index_names(&indexes), [("b", (1, 0))]);
    }

    /// The final rows of all changed keys give the right maps in any order.
    #[::tarantool::test]
    fn final_rows_in_any_order() {
        // (before, after) for keys 1 and 2: a swap of names and a chain.
        let cases = [
            ([(1, "a"), (2, "b")], [(1, "b"), (2, "a")]),
            ([(1, "a"), (2, "b")], [(1, "c"), (2, "a")]),
        ];
        for (before, after) in cases {
            for order in [[0, 1], [1, 0]] {
                let mut tables = TableCache::default();
                let mut indexes = IndexCache::default();
                for (id, name) in before {
                    tables.set(id, Some(table(id, name, 1)));
                    indexes.set((1, id), Some(index(1, id, name)));
                }
                for i in order {
                    let (id, name) = after[i];
                    tables.set(id, Some(table(id, name, 1)));
                    indexes.set((1, id), Some(index(1, id, name)));
                }
                for (id, name) in after {
                    assert_eq!(tables.by_name.get(name), Some(&id), "{after:?} {order:?}");
                    assert_eq!(indexes.by_name.get(name), Some(&(1, id)));
                }
                assert_eq!(tables.by_name.len(), 2, "{after:?} {order:?}");
                assert_eq!(indexes.by_name.len(), 2);
            }
        }
    }

    #[::tarantool::test]
    fn load_contains_system_tables() {
        let storage = Catalog::for_tests();
        let cache = CatalogCache::for_tests_from(&storage);

        let cached = cache.get();
        for (table, indexes) in crate::schema::system_table_definitions() {
            assert_eq!(cached.table(table.id), Some(&table));
            assert_eq!(cached.table_by_name(&table.name), Some(&table));
            for index in indexes {
                assert_eq!(cached.index(index.table_id, index.id), Some(&index));
                assert_eq!(cached.index_by_name(&index.name), Some(&index));
            }
        }
        drop(cached);
        assert_eq!(cache.find_mismatch(&storage).unwrap(), None);
    }

    #[::tarantool::test]
    fn index_order_matches_pico_index() {
        let storage = Catalog::for_tests();
        let indexes = IndexCache::load(&storage).unwrap();

        let space = &storage.indexes.space;
        let expected: Vec<(SpaceId, IndexId)> = (space.select_v2(IteratorType::All, &()).unwrap())
            .map(|t| decode_index(&t.unwrap()))
            .map(|i| (i.table_id, i.id))
            .collect();
        let cached: Vec<_> = indexes.by_key.keys().copied().collect();
        assert_eq!(cached, expected);

        let expected: Vec<IndexId> = (space.select_v2(IteratorType::Eq, &[PicoTable::TABLE_ID]))
            .unwrap()
            .map(|t| decode_index(&t.unwrap()).id)
            .collect();
        assert_eq!(expected, [0, 1, 2]);
        let cached: Vec<_> = (indexes.of_table(PicoTable::TABLE_ID))
            .map(|i| i.id)
            .collect();
        assert_eq!(cached, expected);
    }

    #[::tarantool::test]
    fn diff_finds_mismatch() {
        let storage = Catalog::for_tests();
        let tables = TableCache::load(&storage).unwrap();
        let indexes = IndexCache::load(&storage).unwrap();
        let table_mismatch = |corrupt: fn(&mut TableCache)| {
            let mut cached = TableCache::load(&storage).unwrap();
            corrupt(&mut cached);
            cached.diff(&tables).unwrap()
        };
        let index_mismatch = |corrupt: fn(&mut IndexCache)| {
            let mut cached = IndexCache::load(&storage).unwrap();
            corrupt(&mut cached);
            cached.diff(&indexes).unwrap()
        };

        // Extra record.
        let msg = table_mismatch(|c| c.set(9999, Some(table(9999, "extra", 1))));
        assert!(msg.contains("_pico_table at key 9999"), "{msg}");

        // Missing record.
        let msg = index_mismatch(|c| _ = c.by_key.remove(&(Indexes::TABLE_ID, 1)));
        assert!(msg.contains("_pico_index at key (513, 1)"), "{msg}");

        // Changed record.
        let msg = table_mismatch(|c| {
            let def = c.by_id.get_mut(&PicoTable::TABLE_ID).unwrap();
            def.schema_version += 1;
        });
        assert!(msg.contains("_pico_table at key 512"), "{msg}");

        // Wrong name map.
        let msg = index_mismatch(|c| _ = c.by_name.insert("bogus".into(), (512, 0)));
        assert!(msg.contains(r#"_pico_index at key "bogus""#), "{msg}");
    }

    /// Each instance of the cache is changed only by the writes passed to it,
    /// e.g. each pretend instance of the simulation has its own.
    #[::tarantool::test]
    fn caches_are_independent() {
        let storage = Catalog::for_tests();
        let written = CatalogCache::for_tests_from(&storage);
        let other = CatalogCache::for_tests_from(&storage);
        let id = 70006;

        let encoded = tarantool::msgpack::encode(&table(id, "catalog_cache_own", 1));
        let tuple = storage
            .pico_table
            .space
            .insert(RawBytes::new(&encoded))
            .unwrap();
        written.put_table(&tuple);
        assert!(written.get().table(id).is_some());
        assert!(other.get().table(id).is_none());
        assert_eq!(written.find_mismatch(&storage).unwrap(), None);

        storage.pico_table.space.delete(&[id]).unwrap();
        written.remove_table(id);
        assert_eq!(written.find_mismatch(&storage).unwrap(), None);
        assert_eq!(other.find_mismatch(&storage).unwrap(), None);
    }

    #[::tarantool::test]
    fn write_methods_update_cache() {
        use crate::traft::op::RenameMappingBuilder;
        use tarantool::index::{Part, SortOrder};
        use tarantool::space::{Field, FieldType};

        let storage = Catalog::for_tests();
        let cache = CatalogCache::for_tests_from(&storage);
        let cached_table = |id| cache.get().table(id).cloned();
        let id = 70001;
        let def = table(id, "catalog_cache_methods", 1);

        storage.pico_table.insert(&def, &cache).unwrap();
        assert_eq!(cached_table(id), Some(def.clone()));
        cache.assert_consistent(&storage);

        storage
            .pico_table
            .update_operable(id, false, &cache)
            .unwrap();
        assert!(!cached_table(id).unwrap().operable);
        cache.assert_consistent(&storage);

        (storage.pico_table.update_schema_version(id, 2, &cache)).unwrap();
        assert_eq!(cached_table(id).unwrap().schema_version, 2);
        cache.assert_consistent(&storage);

        (storage
            .pico_table
            .rename(id, "catalog_cache_renamed", &cache))
        .unwrap();
        assert!(cache.get().table_by_name("catalog_cache_methods").is_none());
        let renamed = cache
            .get()
            .table_by_name("catalog_cache_renamed")
            .map(|t| t.id);
        assert_eq!(renamed, Some(id));
        cache.assert_consistent(&storage);

        let format = vec![Field::from(("a", FieldType::Unsigned))];
        let renames = RenameMappingBuilder::new().build();
        (storage
            .pico_table
            .update_format(id, &format, &renames, &cache))
        .unwrap();
        assert_eq!(cached_table(id).unwrap().format, format);
        cache.assert_consistent(&storage);

        storage.pico_table.put(&def, &cache).unwrap();
        assert_eq!(cached_table(id), Some(def));
        cache.assert_consistent(&storage);

        // The ascending order is dropped when the row is encoded, so the stored
        // definition differs from the one passed in.
        let mut pk = index(id, 0, "catalog_cache_methods_pk");
        pk.parts = vec![Part {
            sort_order: Some(SortOrder::Asc),
            ..Part::field("a")
        }];
        storage.indexes.insert(&pk, &cache).unwrap();
        cache.assert_consistent(&storage);

        (storage.indexes.update_operable(id, 0, false, &cache)).unwrap();
        assert!(!cache.get().index(id, 0).unwrap().operable);
        cache.assert_consistent(&storage);

        (storage.indexes.update_schema_version(id, 0, 2, &cache)).unwrap();
        assert_eq!(cache.get().index(id, 0).unwrap().schema_version, 2);
        cache.assert_consistent(&storage);

        (storage
            .indexes
            .rename(id, 0, "catalog_cache_pk_renamed", &cache))
        .unwrap();
        assert!(cache
            .get()
            .index_by_name("catalog_cache_methods_pk")
            .is_none());
        assert!(cache
            .get()
            .index_by_name("catalog_cache_pk_renamed")
            .is_some());
        cache.assert_consistent(&storage);

        storage.indexes.put(&pk, &cache).unwrap();
        assert!(cache
            .get()
            .index_by_name("catalog_cache_methods_pk")
            .is_some());
        cache.assert_consistent(&storage);

        storage.indexes.delete(id, 0, &cache).unwrap();
        assert!(cache.get().index(id, 0).is_none());
        storage.pico_table.delete(id, &cache).unwrap();
        assert_eq!(cached_table(id), None);
        cache.assert_consistent(&storage);
    }

    #[::tarantool::test]
    fn writes_in_transaction() {
        use tarantool::transaction::transaction;

        let storage = Catalog::for_tests();
        let cache = CatalogCache::for_tests_from(&storage);
        let cached_table = |id| cache.get().table(id).cloned();
        let id = 70002;
        let taken_id = 70003;
        let missing_id = 70004;
        let def = table(id, "catalog_cache_txn", 1);
        storage.pico_table.insert(&def, &cache).unwrap();

        // The cache changes together with the table, before the commit.
        transaction(|| -> tarantool::Result<()> {
            storage.pico_table.update_operable(id, false, &cache)?;
            assert!(!cached_table(id).unwrap().operable);
            Ok(())
        })
        .unwrap();
        cache.assert_consistent(&storage);

        // A rejected write changes nothing, an earlier one stays.
        transaction(|| -> tarantool::Result<()> {
            storage.pico_table.update_schema_version(id, 7, &cache)?;
            let taken = table(taken_id, PicoTable::TABLE_NAME, 1);
            assert!(storage.pico_table.insert(&taken, &cache).is_err());
            Ok(())
        })
        .unwrap();
        assert_eq!(cached_table(id).unwrap().schema_version, 7);
        assert_eq!(cached_table(taken_id), None);
        cache.assert_consistent(&storage);

        // An update of a missing row changes nothing.
        (storage
            .pico_table
            .update_operable(missing_id, false, &cache))
        .unwrap();
        assert_eq!(cached_table(missing_id), None);

        storage.pico_table.delete(id, &cache).unwrap();
        cache.assert_consistent(&storage);
    }

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
