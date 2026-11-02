//! A [`Metadata`] catalog built table by table — the schema a test or a
//! benchmark binds its queries against.
//!
//! Tables are given the way `CREATE TABLE` gives them (columns, sharding key,
//! primary key), indexes the way `CREATE INDEX` does; everything a catalog
//! needs but a DDL statement never spells — space and index ids, the hidden
//! `bucket_id` column, the builtin functions — the builder supplies.
//! [`corpus_catalog`](crate::corpus_catalog) is one instance of it, a suite's
//! own fixture schema another.
//!
//! [`MockCatalog::ddl`] renders the registered tables and indexes back as the
//! statements they stand in for, so a suite can pin its fixture schema with a
//! snapshot instead of restating it in a comment that goes stale.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::time::Duration;

use smol_str::{format_smolstr, SmolStr, ToSmolStr};

use sql_executor::executor::engine::get_builtin_functions;
use sql_ir::errors::{Entity, SbroadError};
use sql_ir::ir::function::Function;
use sql_ir::ir::metadata::Metadata;
use sql_ir::ir::relation::{Column, ColumnRole, SpaceEngine, Table};
use sql_ir::ir::types::{DerivedType, UnrestrictedType};

/// A user column given as `(name, type, is_nullable)` — what a `CREATE TABLE`
/// statement would list, without the hidden `bucket_id` sharding column.
pub type ColumnSpec<'a> = (&'a str, UnrestrictedType, bool);

/// Turn [`ColumnSpec`]s into user [`Column`]s.
fn user_columns(columns: &[ColumnSpec]) -> Vec<Column> {
    columns
        .iter()
        .map(|(name, ty, is_nullable)| {
            Column::new(name, DerivedType::new(*ty), ColumnRole::User, *is_nullable)
        })
        .collect()
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn primary_key_name(table: &Table) -> SmolStr {
    format_smolstr!("{}_pkey", table.id)
}

/// An index registered by [`MockCatalog::add_index`].
struct MockIndex {
    name: SmolStr,
    table: SmolStr,
    id: u32,
    columns: Vec<SmolStr>,
}

/// A [`Metadata`] implementation over the tables and indexes registered on it,
/// plus the builtin functions.
///
/// Each table `add_*` assigns the next sequential space id and keys the table
/// by its own name.
pub struct MockCatalog {
    functions: HashMap<SmolStr, Function>,
    tables: HashMap<SmolStr, Table>,
    /// In registration order, which is also each table's index id order.
    indexes: Vec<MockIndex>,
    /// Registration order, so [`ddl`](Self::ddl) renders the schema in the
    /// order it was written rather than in `tables`' hash order.
    order: Vec<SmolStr>,
    next_id: u32,
    /// Storage the sharded tables are created on, see [`MockCatalog::on_storage`].
    engine: SpaceEngine,
    tier: Option<SmolStr>,
}

impl Default for MockCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl MockCatalog {
    /// An empty catalog carrying the builtin functions.
    ///
    /// Sharded tables land on MEMTX with no tier — the defaults a bare
    /// `CREATE TABLE` gets; [`on_storage`](Self::on_storage) picks others.
    #[must_use]
    pub fn new() -> Self {
        let functions = get_builtin_functions()
            .iter()
            .map(|f| (f.name.clone(), f.clone()))
            .collect();
        Self {
            functions,
            tables: HashMap::new(),
            indexes: Vec::new(),
            order: Vec::new(),
            next_id: 1000,
            engine: SpaceEngine::Memtx,
            tier: None,
        }
    }

    /// Create the sharded tables on `engine`, in `tier`.
    #[must_use]
    pub fn on_storage(mut self, engine: SpaceEngine, tier: &str) -> Self {
        self.engine = engine;
        self.tier = Some(tier.to_smolstr());
        self
    }

    fn next_table_id(&mut self) -> u32 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Key `table` by its own name, remembering the registration order.
    ///
    /// # Panics
    /// - the name is already registered. Silently replacing would leave the
    ///   catalog disagreeing with the DDL [`ddl`](Self::ddl) renders.
    /// - the generated primary-key index name is already used by a user index.
    fn register(&mut self, table: Table) {
        let pkey = primary_key_name(&table);
        assert!(
            self.indexes.iter().all(|index| index.name != pkey),
            "index '{pkey}' already exists"
        );
        let name = table.name.clone();
        assert!(
            self.tables.insert(name.clone(), table).is_none(),
            "table '{name}' is already registered"
        );
        self.order.push(name);
    }

    /// Register a sharded table.
    ///
    /// The hidden `bucket_id` sharding column every sharded table carries is
    /// appended here, so `columns` lists only what a `CREATE TABLE` would.
    ///
    /// # Panics
    /// - a column name is duplicated, or a key names a column that is absent;
    /// - the table name is already registered.
    pub fn add_sharded(
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
            self.engine.clone(),
            self.tier.clone(),
        )
        .expect("test table definition must be valid");
        self.register(table);
    }

    /// Register a global table (no sharding key, no `bucket_id`).
    ///
    /// # Panics
    /// - a column name is duplicated, or the primary key names a column that is absent;
    /// - the table name is already registered.
    pub fn add_global(&mut self, name: &str, columns: &[ColumnSpec], primary_key: &[&str]) {
        let id = self.next_table_id();
        let table = Table::new_global(id, name, user_columns(columns), primary_key)
            .expect("test table definition must be valid");
        self.register(table);
    }

    /// Register an index on a registered table.
    ///
    /// Index names are global, as in picodata, and ids follow the table's maximum
    /// existing id. Implicit indexes are 0 for the primary key, and 1 for
    /// `bucket_id` on a sharded table unless it is a primary-key prefix.
    ///
    /// # Panics
    /// - the table is not registered, `columns` is empty, or a column is absent
    ///   from the table;
    /// - the index name is already registered, on any table.
    /// - the name conflicts with an implicit primary-key index or the table's
    ///   implicit `bucket_id` index.
    pub fn add_index(&mut self, name: &str, table: &str, columns: &[&str]) {
        let table = self
            .tables
            .get(table)
            .unwrap_or_else(|| panic!("table '{table}' is not registered"));
        assert!(!columns.is_empty(), "index '{name}' has no columns");
        for column in columns {
            assert!(
                table.columns.iter().any(|c| c.name == *column),
                "index '{name}' column '{column}' is absent from table '{}'",
                table.name
            );
        }
        assert!(
            self.indexes.iter().all(|index| index.name != name),
            "index '{name}' is already registered"
        );
        assert!(
            self.tables
                .values()
                .all(|table| primary_key_name(table) != name),
            "index '{name}' already exists"
        );

        let has_bucket_index = table.is_sharded() && !table.has_bucket_id_primary_key_prefix();
        assert!(
            !(has_bucket_index && name == "bucket_id"),
            "index '{name}' already exists"
        );
        let first_user_id = if has_bucket_index { 2 } else { 1 };
        let id = self
            .indexes
            .iter()
            .filter(|index| index.table == table.name)
            .map(|index| index.id)
            .max()
            .map_or(first_user_id, |id| {
                id.checked_add(1).expect("index id must fit u32")
            });
        self.indexes.push(MockIndex {
            name: name.to_smolstr(),
            table: table.name.clone(),
            id,
            columns: columns.iter().map(|c| c.to_smolstr()).collect(),
        });
    }

    /// The whole catalog as the `CREATE TABLE` statements it stands in for, in
    /// registration order, one blank line apart; each followed by the
    /// `CREATE INDEX` statements of its indexes.
    ///
    /// Rendered from the catalog itself, so a snapshot of it documents the
    /// fixture schema without being able to drift from it — including for a
    /// table or an index added later.
    #[must_use]
    pub fn ddl(&self) -> String {
        self.order
            .iter()
            .map(|name| {
                let mut ddl = self.table_ddl(name);
                for index in self.indexes.iter().filter(|index| index.table == *name) {
                    let columns = index
                        .columns
                        .iter()
                        .map(|column| quote_ident(column))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let _ = write!(
                        ddl,
                        "\nCREATE INDEX {} ON {} ({columns});",
                        quote_ident(&index.name),
                        quote_ident(&index.table)
                    );
                }
                ddl
            })
            .collect::<Vec<_>>()
            .join("\n\n")
    }

    /// One registered table as the `CREATE TABLE` statement it stands in for.
    ///
    /// User columns only: `bucket_id` is the catalog's own bookkeeping, not
    /// something a `CREATE TABLE` writes.
    ///
    /// # Panics
    /// - `name` names no registered table.
    #[must_use]
    pub fn table_ddl(&self, name: &str) -> String {
        let table = self
            .tables
            .get(name)
            .unwrap_or_else(|| panic!("table '{name}' is not registered"));
        let key_columns = |positions: &[usize]| {
            positions
                .iter()
                .map(|&pos| quote_ident(&table.columns[pos].name))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut ddl = format!("CREATE TABLE {} (\n", quote_ident(&table.name));
        for column in &table.columns {
            if *column.get_role() != ColumnRole::User {
                continue;
            }
            let nullability = if column.is_nullable {
                "NULL"
            } else {
                "NOT NULL"
            };
            // `DerivedType`'s `Display` is the `::type` suffix spelling.
            let _ = writeln!(
                ddl,
                "    {} {} {nullability},",
                quote_ident(&column.name),
                column.r#type
            );
        }
        let _ = writeln!(
            ddl,
            "    PRIMARY KEY ({})",
            key_columns(&table.primary_key.positions)
        );
        ddl.push(')');

        match table.get_sk() {
            // Global: no engine to choose and no key to distribute by.
            Err(_) => ddl.push_str(" DISTRIBUTED GLOBALLY;"),
            Ok(sharding_key) => {
                let engine = match table.engine() {
                    SpaceEngine::Memtx => "MEMTX",
                    SpaceEngine::Vinyl => "VINYL",
                };
                let _ = write!(
                    ddl,
                    " USING {engine} DISTRIBUTED BY ({})",
                    key_columns(sharding_key)
                );
                if let Some(tier) = &table.tier {
                    let _ = write!(ddl, " IN TIER {}", quote_ident(tier));
                }
                ddl.push(';');
            }
        }
        ddl
    }
}

impl Metadata for MockCatalog {
    fn table(&self, table_name: &str) -> Result<Table, SbroadError> {
        self.tables
            .get(table_name)
            .cloned()
            .ok_or_else(|| SbroadError::NotFound(Entity::Space, table_name.to_smolstr()))
    }

    /// Answers as picodata's catalog does: by the global index name, then
    /// checked against the table.
    fn get_index_id(&self, index_name: &str, table_name: &str) -> Result<u32, SbroadError> {
        let (owner, id) = self
            .indexes
            .iter()
            .find(|index| index.name == index_name)
            .map(|index| (index.table.as_str(), index.id))
            .or_else(|| {
                self.tables
                    .values()
                    .find(|table| primary_key_name(table) == index_name)
                    .map(|table| (table.name.as_str(), 0))
            })
            .ok_or_else(|| SbroadError::NotFound(Entity::Index, index_name.to_smolstr()))?;
        if owner != table_name {
            return Err(SbroadError::NotFound(
                Entity::Index,
                format_smolstr!("'{index_name}' for table '{table_name}'"),
            ));
        }
        Ok(id)
    }

    fn function(&self, fn_name: &str) -> Result<&Function, SbroadError> {
        self.functions
            .get(fn_name)
            .ok_or_else(|| SbroadError::NotFound(Entity::SQLFunction, SmolStr::new(fn_name)))
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

#[cfg(test)]
mod tests {
    use super::{Metadata, MockCatalog, SpaceEngine, UnrestrictedType};

    /// The DDL a table renders back to has to be the statement it stands in
    /// for: the hidden `bucket_id` is not part of it, and the storage clause
    /// follows whichever `add_*` registered the table. Its indexes follow it,
    /// whenever they were registered.
    #[test]
    fn ddl_renders_registration_order_and_storage() {
        use UnrestrictedType::{Integer, String};

        let mut catalog = MockCatalog::new().on_storage(SpaceEngine::Vinyl, "default");
        catalog.add_sharded(
            "sharded",
            &[("id", Integer, false), ("payload", String, true)],
            &["id"],
            &["id"],
        );
        catalog.add_global("global", &[("id", Integer, false)], &["id"]);
        catalog.add_index("global_id", "global", &["id"]);
        catalog.add_index("sharded_payload_id", "sharded", &["payload", "id"]);

        assert_eq!(
            catalog.ddl(),
            r#"CREATE TABLE "sharded" (
    "id" int NOT NULL,
    "payload" text NULL,
    PRIMARY KEY ("id")
) USING VINYL DISTRIBUTED BY ("id") IN TIER "default";
CREATE INDEX "sharded_payload_id" ON "sharded" ("payload", "id");

CREATE TABLE "global" (
    "id" int NOT NULL,
    PRIMARY KEY ("id")
) DISTRIBUTED GLOBALLY;
CREATE INDEX "global_id" ON "global" ("id");"#
        );
    }

    /// Two tables, one of each kind, with indexes registered interleaved.
    fn indexed_catalog() -> MockCatalog {
        use UnrestrictedType::Integer;

        let mut catalog = MockCatalog::new();
        catalog.add_sharded(
            "sharded",
            &[("a", Integer, false), ("b", Integer, true)],
            &["a"],
            &["a"],
        );
        catalog.add_global("global", &[("a", Integer, false)], &["a"]);
        catalog.add_index("sharded_a", "sharded", &["a"]);
        catalog.add_index("global_a", "global", &["a"]);
        catalog.add_index("sharded_b", "sharded", &["b"]);
        catalog
    }

    /// Ids count per table, past the primary key (0) and, on a sharded table,
    /// the `bucket_id` index (1) — the ids picodata gives the same DDL.
    #[test]
    fn index_ids_count_per_table_past_implicit_indexes() {
        let catalog = indexed_catalog();
        let id = |index, table| catalog.get_index_id(index, table).unwrap();

        assert_eq!(id("sharded_a", "sharded"), 2);
        assert_eq!(id("sharded_b", "sharded"), 3);
        assert_eq!(id("global_a", "global"), 1);
    }

    #[test]
    fn primary_key_indexes_resolve_by_global_name() {
        let catalog = indexed_catalog();
        assert_eq!(catalog.get_index_id("1000_pkey", "sharded").unwrap(), 0);
        assert_eq!(catalog.get_index_id("1001_pkey", "global").unwrap(), 0);
        assert_eq!(
            catalog
                .get_index_id("1000_pkey", "global")
                .unwrap_err()
                .to_string(),
            "index '1000_pkey' for table 'global' not found"
        );
    }

    #[test]
    #[should_panic(expected = "index '1000_pkey' already exists")]
    fn user_index_cannot_take_existing_primary_key_name() {
        let mut catalog = indexed_catalog();
        catalog.add_index("1000_pkey", "global", &["a"]);
    }

    #[test]
    #[should_panic(expected = "index '1002_pkey' already exists")]
    fn table_cannot_take_existing_user_index_name() {
        let mut catalog = indexed_catalog();
        catalog.add_index("1002_pkey", "global", &["a"]);
        catalog.add_global("next", &[("a", UnrestrictedType::Integer, false)], &["a"]);
    }

    #[test]
    #[should_panic(expected = "index 'bucket_id' already exists")]
    fn implicit_bucket_id_index_name_is_reserved() {
        let mut catalog = indexed_catalog();
        catalog.add_index("bucket_id", "sharded", &["a"]);
    }

    #[test]
    fn bucket_id_index_name_is_available_without_implicit_index() {
        for embedded_bucket_id in [false, true] {
            let mut catalog = MockCatalog::new();
            let columns = &[("a", UnrestrictedType::Integer, false)];
            if embedded_bucket_id {
                catalog.add_sharded("t", columns, &["a"], &["bucket_id", "a"]);
            } else {
                catalog.add_global("t", columns, &["a"]);
            }
            catalog.add_index("bucket_id", "t", &["a"]);
            catalog.add_index("next", "t", &["a"]);
            assert_eq!(catalog.get_index_id("bucket_id", "t").unwrap(), 1);
            assert_eq!(catalog.get_index_id("next", "t").unwrap(), 2);
        }
    }

    #[test]
    fn index_ids_follow_maximum_remaining_id() {
        let mut catalog = indexed_catalog();
        // Simulate drops without adding a DDL mutation API to the fixture builder.
        catalog.indexes.retain(|index| index.name != "sharded_a");
        catalog.add_index("after_gap", "sharded", &["a"]);
        assert_eq!(catalog.get_index_id("sharded_b", "sharded").unwrap(), 3);
        assert_eq!(catalog.get_index_id("after_gap", "sharded").unwrap(), 4);

        catalog.indexes.retain(|index| index.name != "after_gap");
        catalog.add_index("after_max_drop", "sharded", &["a"]);
        assert_eq!(
            catalog.get_index_id("after_max_drop", "sharded").unwrap(),
            4
        );

        catalog.indexes.retain(|index| index.table != "sharded");
        catalog.add_index("after_all_dropped", "sharded", &["a"]);
        assert_eq!(
            catalog
                .get_index_id("after_all_dropped", "sharded")
                .unwrap(),
            2
        );
        assert_eq!(catalog.get_index_id("global_a", "global").unwrap(), 1);
    }

    #[test]
    fn ddl_escapes_quotes_in_identifiers() {
        let mut catalog = MockCatalog::new().on_storage(SpaceEngine::Memtx, "tier\"q");
        catalog.add_sharded(
            "t\"q",
            &[("a\"q", UnrestrictedType::Integer, false)],
            &["a\"q"],
            &["a\"q"],
        );
        catalog.add_index("ix\"q", "t\"q", &["a\"q"]);
        assert_eq!(
            catalog.ddl(),
            r#"CREATE TABLE "t""q" (
    "a""q" int NOT NULL,
    PRIMARY KEY ("a""q")
) USING MEMTX DISTRIBUTED BY ("a""q") IN TIER "tier""q";
CREATE INDEX "ix""q" ON "t""q" ("a""q");"#
        );
    }

    /// The same errors picodata's catalog gives: an unknown name, and a known
    /// name on another table.
    #[test]
    fn unresolved_index_errors() {
        let catalog = indexed_catalog();
        let err = |index, table| catalog.get_index_id(index, table).unwrap_err().to_string();

        assert_eq!(err("nope", "sharded"), "index nope not found");
        assert_eq!(
            err("global_a", "sharded"),
            "index 'global_a' for table 'sharded' not found"
        );
    }

    /// Index names are global: the same name on another table is still taken.
    #[test]
    #[should_panic(expected = "index 'dup' is already registered")]
    fn duplicate_index_name_panics() {
        let mut catalog = indexed_catalog();
        catalog.add_index("dup", "sharded", &["a"]);
        catalog.add_index("dup", "global", &["a"]);
    }

    #[test]
    #[should_panic(expected = "index 'ix' column 'b' is absent from table 'global'")]
    fn index_on_absent_column_panics() {
        let mut catalog = indexed_catalog();
        catalog.add_index("ix", "global", &["b"]);
    }

    /// Registering the same name twice would leave the catalog answering with
    /// one table and [`MockCatalog::ddl`] listing another.
    #[test]
    #[should_panic(expected = "table 'dup' is already registered")]
    fn duplicate_table_name_panics() {
        let mut catalog = MockCatalog::new();
        catalog.add_global("dup", &[("id", UnrestrictedType::Integer, false)], &["id"]);
        catalog.add_global("dup", &[("id", UnrestrictedType::Integer, false)], &["id"]);
    }
}
