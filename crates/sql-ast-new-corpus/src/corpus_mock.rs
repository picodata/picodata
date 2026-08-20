//! `Metadata` catalog with the anonymized corpus tables and the builtin functions —
//! the schema `sql-ast-new-corpus`'s queries bind against.

use std::collections::HashMap;
use std::time::Duration;

use smol_str::{SmolStr, ToSmolStr};

use sql_executor::executor::engine::get_builtin_functions;
use sql_ir::errors::{Entity, SbroadError};
use sql_ir::ir::function::Function;
use sql_ir::ir::metadata::Metadata;
use sql_ir::ir::relation::{Column, ColumnRole, SpaceEngine, Table};
use sql_ir::ir::types::{DerivedType, UnrestrictedType};
use sql_ir::utils::normalize_name_from_sql;

/// A `Metadata` catalog with the obfuscated corpus tables and the builtin functions.
pub struct CorpusMock {
    functions: HashMap<SmolStr, Function>,
    tables: HashMap<SmolStr, Table>,
}

impl Default for CorpusMock {
    fn default() -> Self {
        Self::new()
    }
}

/// A user column given as `(name, type, is_nullable)` — what a `CREATE TABLE`
/// statement would list, without the hidden `bucket_id` sharding column.
type ColumnSpec<'a> = (&'a str, UnrestrictedType, bool);

/// Turn [`ColumnSpec`]s into user [`Column`]s.
fn user_columns(columns: &[ColumnSpec]) -> Vec<Column> {
    columns
        .iter()
        .map(|(name, ty, is_nullable)| {
            Column::new(name, DerivedType::new(*ty), ColumnRole::User, *is_nullable)
        })
        .collect()
}

/// Accumulates the corpus tables, assigning each a sequential space id and
/// keying it by its own name.
struct Catalog {
    tables: HashMap<SmolStr, Table>,
    next_id: u32,
}

impl Catalog {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
            next_id: 1000,
        }
    }

    fn next_table_id(&mut self) -> u32 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Register a sharded `Vinyl` table on the `default` tier.
    ///
    /// The hidden `bucket_id` sharding column every sharded table carries is
    /// appended here, so `columns` lists only what a `CREATE TABLE` would.
    fn add_sharded(
        &mut self,
        name: &str,
        columns: &[ColumnSpec],
        sharding_key: &[&str],
        primary_key: &[&str],
    ) {
        let id = self.next_table_id();
        let mut columns = user_columns(columns);
        columns.push(Column::new(
            "bucket_id",
            DerivedType::new(UnrestrictedType::Integer),
            ColumnRole::Sharding,
            true,
        ));
        let table = Table::new_sharded_in_tier(
            id,
            name,
            columns,
            sharding_key,
            primary_key,
            SpaceEngine::Vinyl,
            Some("default".to_smolstr()),
        )
        .expect("test table definition must be valid");
        self.tables.insert(name.to_smolstr(), table);
    }

    /// Register a global table (no sharding key, no `bucket_id`).
    fn add_global(&mut self, name: &str, columns: &[ColumnSpec], primary_key: &[&str]) {
        let id = self.next_table_id();
        let table = Table::new_global(id, name, user_columns(columns), primary_key)
            .expect("test table definition must be valid");
        self.tables.insert(name.to_smolstr(), table);
    }

    fn build(self) -> HashMap<SmolStr, Table> {
        self.tables
    }
}

impl CorpusMock {
    #[must_use]
    pub fn new() -> Self {
        use UnrestrictedType::{Boolean, Datetime, Decimal, Integer, String};

        let mut functions = HashMap::new();
        for f in get_builtin_functions() {
            functions.insert(f.name.clone(), f.clone());
        }

        let mut catalog = Catalog::new();

        catalog.add_sharded(
            "a",
            &[
                ("cv", Integer, false),
                ("aw", Integer, false),
                ("cq", Integer, false),
                ("bk", Integer, false),
                ("bl", Integer, false),
                ("bm", Integer, false),
                ("a", Datetime, false),
                ("z", Integer, false),
                ("ay", String, false),
                ("bg", String, false),
                ("ah", String, false),
                ("f", Decimal, true),
                ("e", Decimal, true),
                ("g", Decimal, true),
            ],
            &["cv"],
            &[
                "cv", "aw", "cq", "bk", "bl", "bm", "a", "z", "ay", "bg", "ah",
            ],
        );

        catalog.add_sharded(
            "b",
            &[
                ("cv", Integer, false),
                ("aw", Integer, false),
                ("cq", Integer, false),
                ("bk", Integer, false),
                ("bl", Integer, false),
                ("bm", Integer, false),
                ("ay", String, false),
                ("bg", String, false),
                ("ah", String, false),
                ("f", Decimal, true),
                ("e", Decimal, true),
            ],
            &["cv"],
            &["cv", "aw", "cq", "bk", "bl", "bm", "ay", "bg", "ah"],
        );

        catalog.add_sharded(
            "c",
            &[
                ("cv", Integer, false),
                ("bm", Integer, false),
                ("ay", String, false),
                ("bg", String, false),
                ("ah", String, false),
            ],
            &["cv"],
            &["cv", "bm", "ay", "bg", "ah"],
        );

        catalog.add_sharded(
            "d",
            &[
                ("cv", Integer, false),
                ("cq", Integer, false),
                ("bk", Integer, false),
                ("bl", Integer, false),
                ("bm", Integer, false),
                ("a", Datetime, false),
                ("z", Integer, false),
                ("ay", String, false),
                ("f", Decimal, true),
                ("e", Decimal, true),
            ],
            &["cv"],
            &["cv", "cq", "bk", "bl", "bm", "a", "z", "ay"],
        );

        catalog.add_sharded(
            "e",
            &[
                ("cv", Integer, false),
                ("cq", Integer, false),
                ("bk", Integer, false),
                ("bl", Integer, false),
                ("bm", Integer, false),
                ("ay", String, false),
                ("f", Decimal, true),
                ("e", Decimal, true),
                ("g", Decimal, true),
            ],
            &["cv"],
            &["cv", "cq", "bk", "bl", "bm", "ay"],
        );

        catalog.add_global(
            "f",
            &[
                ("cw", Integer, false),
                ("z", Integer, true),
                ("cj", Integer, true),
            ],
            &["cw"],
        );

        catalog.add_global(
            "g",
            &[("cu", Integer, false), ("cw", Integer, true)],
            &["cu"],
        );

        catalog.add_global(
            "h",
            &[("cu", Integer, false), ("cq", Integer, true)],
            &["cu"],
        );

        catalog.add_global(
            "i",
            &[
                ("br", Integer, false),
                ("ax", String, true),
                ("cy", String, true),
                ("bc", String, true),
                ("cz", Boolean, true),
                ("ct", Boolean, true),
                ("ap", Boolean, true),
            ],
            &["br"],
        );

        catalog.add_global("j", &[("bg", String, false), ("bc", String, true)], &["bg"]);

        catalog.add_global(
            "k",
            &[
                ("bi", String, false),
                ("bj", Integer, true),
                ("bk", Integer, true),
                ("bl", Integer, true),
            ],
            &["bi"],
        );

        catalog.add_sharded(
            "l",
            &[
                ("ag", Integer, false),
                ("v", Integer, false),
                ("cw", Integer, false),
                ("cv", Integer, false),
                ("cu", Integer, false),
                ("a", Datetime, false),
                ("u", Datetime, true),
                ("d", Decimal, false),
                ("au", Integer, false),
                ("ah", String, true),
                ("ay", String, true),
                ("bm", Integer, true),
                ("bi", String, true),
                ("cs", Datetime, true),
                ("h", Integer, true),
                ("cm", Integer, true),
                ("cn", String, true),
                ("cl", Datetime, true),
                ("bg", String, true),
                ("co", Integer, true),
                ("w", Datetime, true),
                ("bb", Integer, true),
            ],
            &["cv"],
            &["ag"],
        );

        catalog.add_sharded(
            "m",
            &[
                ("ag", Integer, false),
                ("cv", Integer, false),
                ("cr", Integer, false),
                ("w", Datetime, false),
                ("z", Integer, true),
                ("cu", Integer, true),
                ("bj", Integer, true),
                ("bm", Integer, true),
                ("aw", Integer, true),
                ("aa", Decimal, true),
                ("ao", Boolean, true),
                ("bb", Integer, true),
                ("ay", String, true),
                ("bg", String, true),
                ("ah", String, true),
                ("cw", Integer, true),
                ("ac", Decimal, true),
                ("ae", String, true),
                ("ab", Datetime, true),
                ("ad", Integer, true),
                ("h", Integer, true),
            ],
            &["cv"],
            &["ag", "cr"],
        );

        catalog.add_sharded(
            "n",
            &[
                ("ag", Integer, false),
                ("v", Integer, true),
                ("cw", Integer, false),
                ("cv", Integer, false),
                ("cu", Integer, false),
                ("a", Datetime, false),
                ("u", Datetime, true),
                ("d", Decimal, false),
                ("au", Integer, true),
                ("ah", String, true),
                ("ay", String, true),
                ("bm", Integer, true),
                ("bi", String, true),
                ("cs", Datetime, true),
                ("h", Integer, true),
                ("cm", Integer, false),
                ("cn", String, true),
                ("cl", Datetime, true),
                ("bg", String, true),
                ("co", Integer, true),
                ("w", Datetime, true),
                ("i", Datetime, true),
                ("bb", Integer, true),
                ("bf", Integer, true),
            ],
            &["cv"],
            &["ag"],
        );

        catalog.add_sharded(
            "o",
            &[
                ("ag", Integer, false),
                ("cv", Integer, false),
                ("r", String, false),
                ("q", Datetime, false),
                ("n", Integer, false),
                ("m", Integer, false),
                ("ak", String, true),
                ("t", Decimal, false),
                ("s", Integer, false),
                ("bn", Integer, false),
                ("bv", Decimal, false),
            ],
            &["cv"],
            &["ag"],
        );

        catalog.add_sharded(
            "p",
            &[
                ("ag", Integer, false),
                ("cv", Integer, false),
                ("l", Integer, false),
                ("au", Integer, true),
                ("av", Integer, true),
                ("aw", Integer, true),
                ("a", Datetime, false),
                ("af", Integer, true),
                ("bv", Decimal, false),
                ("cs", Datetime, true),
            ],
            &["cv"],
            &["ag"],
        );

        catalog.add_sharded(
            "q",
            &[
                ("ag", Integer, false),
                ("cv", Integer, false),
                ("j", Integer, false),
            ],
            &["cv"],
            &["ag"],
        );

        catalog.add_sharded(
            "r",
            &[
                ("ag", Integer, false),
                ("cv", Integer, false),
                ("k", Integer, false),
                ("ch", Datetime, false),
                ("ci", Decimal, false),
                ("o", Datetime, false),
                ("y", Decimal, true),
                ("an", Integer, false),
            ],
            &["cv"],
            &["ag"],
        );

        catalog.add_sharded(
            "s",
            &[
                ("ag", Integer, false),
                ("cv", Integer, false),
                ("bx", Decimal, true),
                ("bz", Decimal, true),
                ("ca", Decimal, true),
                ("cd", Decimal, true),
                ("cc", Decimal, true),
                ("ce", Decimal, true),
                ("cf", Decimal, true),
                ("i", Datetime, true),
                ("cg", Decimal, true),
                ("ah", String, true),
                ("ay", String, true),
            ],
            &["cv"],
            &["ag"],
        );

        catalog.add_sharded(
            "t",
            &[
                ("ag", Integer, false),
                ("cv", Integer, false),
                ("bx", Decimal, true),
                ("p", Decimal, true),
                ("bz", Decimal, true),
                ("i", Datetime, false),
            ],
            &["cv"],
            &["ag"],
        );

        catalog.add_sharded(
            "u",
            &[
                ("ag", Integer, false),
                ("v", Integer, false),
                ("bx", Decimal, true),
                ("bz", Decimal, true),
                ("ca", Decimal, true),
                ("cc", Decimal, true),
                ("cd", Decimal, true),
                ("ar", Datetime, false),
                ("cg", Decimal, true),
                ("ce", Decimal, true),
                ("i", Datetime, false),
                ("cf", Decimal, true),
                ("cv", Integer, true),
                ("aq", Boolean, true),
            ],
            &["cv"],
            &["ag"],
        );

        catalog.add_sharded(
            "v",
            &[
                ("ag", Integer, false),
                ("cv", Integer, false),
                ("v", Integer, false),
                ("ai", Decimal, false),
                ("b", Integer, false),
                ("a", Datetime, false),
                ("az", Decimal, false),
                ("au", Integer, false),
                ("i", Datetime, true),
            ],
            &["cv"],
            &["ag"],
        );

        catalog.add_sharded(
            "w",
            &[
                ("ag", Integer, false),
                ("x", Integer, false),
                ("d", Decimal, false),
                ("al", Integer, false),
                ("bt", Integer, true),
                ("db", Integer, true),
                ("da", Integer, true),
                ("bs", Integer, true),
                ("c", Integer, true),
                ("i", Datetime, true),
                ("cv", Integer, true),
            ],
            &["cv"],
            &["ag"],
        );

        catalog.add_sharded(
            "x",
            &[
                ("b", Integer, false),
                ("cv", Integer, true),
                ("u", Datetime, true),
                ("cw", Integer, true),
            ],
            &["cv"],
            &["b"],
        );

        catalog.add_sharded(
            "y",
            &[
                ("ag", Integer, false),
                ("cv", Integer, false),
                ("ay", String, true),
                ("cu", Integer, true),
                ("bg", String, true),
                ("ah", String, true),
                ("bx", Decimal, true),
                ("bz", Decimal, true),
                ("ca", Decimal, true),
                ("cd", Decimal, true),
                ("cc", Decimal, true),
                ("cq", Integer, true),
                ("aw", Integer, true),
                ("v", Integer, true),
                ("i", Datetime, true),
                ("cg", Decimal, true),
            ],
            &["cv"],
            &["ag"],
        );

        catalog.add_sharded(
            "z",
            &[("cv", Integer, false), ("i", Datetime, true)],
            &["cv"],
            &["cv"],
        );

        catalog.add_sharded(
            "aa",
            &[
                ("ag", Integer, false),
                ("cv", Integer, true),
                ("ay", String, true),
                ("r", String, true),
                ("q", Datetime, true),
                ("bq", Datetime, true),
                ("t", Decimal, false),
                ("bo", String, true),
                ("bh", String, true),
                ("cu", Integer, true),
                ("bu", Datetime, true),
                ("az", Decimal, true),
                ("i", Datetime, true),
                ("ba", String, true),
                ("bp", String, true),
            ],
            &["cv"],
            &["ag"],
        );

        catalog.add_sharded(
            "ab",
            &[
                ("cv", Integer, false),
                ("aj", String, true),
                ("ay", String, true),
                ("am", Integer, true),
            ],
            &["cv"],
            &["cv"],
        );

        catalog.add_sharded(
            "ac",
            &[
                ("cv", Integer, false),
                ("bx", Decimal, true),
                ("bz", Decimal, true),
                ("ca", Decimal, true),
                ("cb", Decimal, true),
                ("cg", Decimal, true),
                ("cp", Decimal, true),
                ("i", Datetime, true),
            ],
            &["cv"],
            &["cv"],
        );

        catalog.add_sharded(
            "ad",
            &[
                ("cv", Integer, false),
                ("ay", String, false),
                ("cx", String, true),
            ],
            &["cv"],
            &["cv", "ay"],
        );

        Self {
            functions,
            tables: catalog.build(),
        }
    }
}

impl Metadata for CorpusMock {
    fn table(&self, table_name: &str) -> Result<Table, SbroadError> {
        self.tables
            .get(table_name)
            .cloned()
            .ok_or_else(|| SbroadError::NotFound(Entity::Space, table_name.to_smolstr()))
    }

    fn get_index_id(&self, _index_name: &str, _table_name: &str) -> Result<u32, SbroadError> {
        // here can be any index_id for optimize it doesn't matter
        Ok(42)
    }

    fn function(&self, fn_name: &str) -> Result<&Function, SbroadError> {
        let name = normalize_name_from_sql(fn_name);
        self.functions
            .get(&name)
            .ok_or_else(|| SbroadError::NotFound(Entity::SQLFunction, name))
    }

    fn waiting_timeout(&self) -> Duration {
        Duration::default()
    }

    fn sharding_column(&self) -> &str {
        "bucket_id"
    }

    fn sharding_key_by_space(&self, space: &str) -> Result<Vec<SmolStr>, SbroadError> {
        self.table(space)?.get_sharding_column_names()
    }

    fn sharding_positions_by_space(&self, space: &str) -> Result<Vec<usize>, SbroadError> {
        Ok(self.table(space)?.get_sk()?.to_vec())
    }
}
