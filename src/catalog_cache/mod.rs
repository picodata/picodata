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

mod cache;
mod change;
mod index;
mod table;

pub use self::cache::{CatalogCache, CatalogCacheMutable, ServiceRouteCheck};
pub use self::change::may_change_catalog;
pub(crate) use self::change::CatalogChange;
pub use self::index::IndexCache;
pub use self::table::TableCache;

pub type CatalogCacheRef<'a> = NoYieldsRef<'a, CatalogCacheMutable>;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use tarantool::fiber::safety::NoYieldsRef;

/// Returns a message with the smallest key whose value differs between
/// `cached` and `actual`, including a key present in only one of them. The
/// values aren't printed, so they don't need to be `Debug`.
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
