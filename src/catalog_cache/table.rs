use super::first_diff;
use crate::catalog::pico_table::PicoTable;
use crate::schema::TableDef;
use crate::storage::{Catalog, SystemTable as _};
use ahash::AHashMap;
use smol_str::SmolStr;
use tarantool::index::IteratorType;
use tarantool::space::SpaceId;
use tarantool::tuple::Tuple;

////////////////////////////////////////////////////////////////////////////////
// TableCache
////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct TableCache {
    by_id: AHashMap<SpaceId, TableDef>,
    by_name: AHashMap<SmolStr, SpaceId>,
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

    /// Compares the cache with `actual`, the definitions freshly loaded from
    /// `_pico_table`, for `CatalogCache::find_mismatch`. Used to ensure that
    /// cache is consistent with storage.
    pub(crate) fn diff(&self, actual: &Self) -> Option<String> {
        let table = PicoTable::TABLE_NAME;
        first_diff(table, &self.by_id, &actual.by_id)
            .or_else(|| first_diff(table, &self.by_name, &actual.by_name))
    }
}

////////////////////////////////////////////////////////////////////////////////
// decoding
////////////////////////////////////////////////////////////////////////////////

fn decode_table(tuple: &Tuple) -> TableDef {
    tarantool::msgpack::decode(&tuple.to_vec()).unwrap_or_else(|e| {
        panic!(
            "failed to decode {} row {tuple:?}: {e}",
            PicoTable::TABLE_NAME
        )
    })
}

/// Turns a DML into `_pico_table` into a change of the cache. `old` is the row
/// before the DML, `new` is the row tarantool returned from storage `replace`.
///
/// Returns the table id with its new definition, which is `None` if the row
/// was deleted. Returns `None` if the DML changed nothing, e.g. an update of a
/// missing key or an insert skipped on conflict.
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

fn table_key(tuple: &Tuple) -> u64 {
    tuple
        .field(0)
        .ok()
        .flatten()
        .expect("primary key of _pico_table should be an unsigned integer")
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

mod tests {
    use super::*;

    fn table(id: SpaceId, name: &str, schema_version: u64) -> TableDef {
        TableDef {
            id,
            name: name.into(),
            schema_version,
            ..TableDef::for_tests()
        }
    }

    fn table_names(tables: &TableCache) -> Vec<(&str, SpaceId)> {
        let mut names: Vec<_> = (tables.by_name.iter())
            .map(|(name, id)| (name.as_str(), *id))
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

    /// The final rows of all changed keys give the right name map in any
    /// order, see the guard in `set`.
    #[::tarantool::test]
    fn final_table_rows_in_any_order() {
        // (before, after) for ids 1 and 2: a swap of names and a chain.
        let cases = [
            ([(1, "a"), (2, "b")], [(1, "b"), (2, "a")]),
            ([(1, "a"), (2, "b")], [(1, "c"), (2, "a")]),
        ];
        for (before, after) in cases {
            for order in [[0, 1], [1, 0]] {
                let mut tables = TableCache::default();
                for (id, name) in before {
                    tables.set(id, Some(table(id, name, 1)));
                }
                for i in order {
                    let (id, name) = after[i];
                    tables.set(id, Some(table(id, name, 1)));
                }
                for (id, name) in after {
                    assert_eq!(tables.by_name.get(name), Some(&id), "{after:?} {order:?}");
                }
                assert_eq!(tables.by_name.len(), 2, "{after:?} {order:?}");
            }
        }
    }

    #[::tarantool::test]
    fn diff_tables_find_mismatch() {
        let storage = Catalog::for_tests();
        let tables = TableCache::load(&storage).unwrap();
        let mismatch = |corrupt: fn(&mut TableCache)| {
            let mut cached = TableCache::load(&storage).unwrap();
            corrupt(&mut cached);
            cached.diff(&tables).unwrap()
        };

        // Extra record.
        let msg = mismatch(|c| c.set(9999, Some(table(9999, "extra", 1))));
        assert!(msg.contains("_pico_table at key 9999"), "{msg}");

        // Changed record.
        let msg = mismatch(|c| {
            let def = c.by_id.get_mut(&PicoTable::TABLE_ID).unwrap();
            def.schema_version += 1;
        });
        assert!(msg.contains("_pico_table at key 512"), "{msg}");
    }
}
