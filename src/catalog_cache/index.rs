use super::first_diff;
use crate::catalog::pico_table::PicoTable;
use crate::schema::IndexDef;
use crate::storage::{Catalog, Indexes, SystemTable as _};
use ahash::AHashMap;
use smol_str::SmolStr;
use std::collections::BTreeMap;
use tarantool::index::{IndexId, IteratorType};
use tarantool::space::SpaceId;
use tarantool::tuple::Tuple;

////////////////////////////////////////////////////////////////////////////////
// IndexCache
////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct IndexCache {
    /// Ordered like the primary key of `_pico_index`.
    by_key: BTreeMap<(SpaceId, IndexId), IndexDef>,
    by_name: AHashMap<SmolStr, (SpaceId, IndexId)>,
}

impl IndexCache {
    /// Replaces the definition with key `key`, see [`TableCache::set`](super::TableCache::set).
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

    /// Same as [`TableCache::diff`](super::TableCache::diff) for `_pico_index`.
    pub(crate) fn diff(&self, actual: &Self) -> Option<String> {
        let table = Indexes::TABLE_NAME;
        first_diff(table, &self.by_key, &actual.by_key)
            .or_else(|| first_diff(table, &self.by_name, &actual.by_name))
    }
}

////////////////////////////////////////////////////////////////////////////////
// decoding
////////////////////////////////////////////////////////////////////////////////

fn decode_index(tuple: &Tuple) -> IndexDef {
    tuple.decode().unwrap_or_else(|e| {
        panic!(
            "failed to decode {} row {tuple:?}: {e}",
            Indexes::TABLE_NAME
        )
    })
}

/// Same as [`table_change`](super::table::table_change) for `_pico_index`.
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

fn index_key(tuple: &Tuple) -> (u64, u64) {
    let field = |i| tuple.field(i).ok().flatten();
    field(0)
        .zip(field(1))
        .expect("primary key of _pico_index should be a pair of unsigned integers")
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

mod tests {
    use super::*;

    fn index(table_id: SpaceId, id: IndexId, name: &str) -> IndexDef {
        IndexDef {
            table_id,
            id,
            name: name.into(),
            ..IndexDef::for_tests()
        }
    }

    fn index_names(indexes: &IndexCache) -> Vec<(&str, (SpaceId, IndexId))> {
        let mut names: Vec<_> = (indexes.by_name.iter())
            .map(|(name, key)| (name.as_str(), *key))
            .collect();
        names.sort();
        names
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

    /// The final rows of all changed keys give the right name map in any
    /// order, see the guard in `set`.
    #[::tarantool::test]
    fn final_index_rows_in_any_order() {
        // (before, after) for ids 0 and 1: a swap of names and a chain.
        let cases = [
            ([(0, "a"), (1, "b")], [(0, "b"), (1, "a")]),
            ([(0, "a"), (1, "b")], [(0, "c"), (1, "a")]),
        ];
        for (before, after) in cases {
            for order in [[0, 1], [1, 0]] {
                let mut indexes = IndexCache::default();
                for (id, name) in before {
                    indexes.set((1, id), Some(index(1, id, name)));
                }
                for i in order {
                    let (id, name) = after[i];
                    indexes.set((1, id), Some(index(1, id, name)));
                }
                for (id, name) in after {
                    assert_eq!(indexes.by_name.get(name), Some(&(1, id)));
                }
                assert_eq!(indexes.by_name.len(), 2);
            }
        }
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
    fn diff_indexes_find_mismatch() {
        let storage = Catalog::for_tests();
        let indexes = IndexCache::load(&storage).unwrap();
        let mismatch = |corrupt: fn(&mut IndexCache)| {
            let mut cached = IndexCache::load(&storage).unwrap();
            corrupt(&mut cached);
            cached.diff(&indexes).unwrap()
        };

        // Missing record.
        let msg = mismatch(|c| _ = c.by_key.remove(&(Indexes::TABLE_ID, 1)));
        assert!(msg.contains("_pico_index at key (513, 1)"), "{msg}");

        // Wrong name map.
        let msg = mismatch(|c| _ = c.by_name.insert("bogus".into(), (512, 0)));
        assert!(msg.contains(r#"_pico_index at key "bogus""#), "{msg}");
    }
}
