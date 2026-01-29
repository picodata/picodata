use crate::column_name;
use crate::schema::{IndexDef, IndexOption, TableDef, INITIAL_SCHEMA_VERSION};
use crate::storage::{
    index_by_ids_unchecked, space_by_id_unchecked, EntryIter, SystemTable, ToEntryIter, MP_CUSTOM,
};
use crate::traft::op::RenameMapping;
use std::fmt::Debug;
use tarantool::index::{
    FieldType as IndexFieldType, Index, IndexIterator, IndexType, IteratorType, Part,
};
use tarantool::session::UserId;
use tarantool::space::{Space, SpaceId, SpaceType, UpdateOps};
use tarantool::tuple::{RawBytes, Tuple};

////////////////////////////////////////////////////////////////////////////////
// PicoTable
////////////////////////////////////////////////////////////////////////////////

/// A struct for accessing contents of `_pico_table` system table.
/// `_pico_table` contains definitions of all picodata tables.
#[derive(Debug, Clone)]
pub struct PicoTable {
    pub space: Space,
    pub index_name: Index,
    pub index_id: Index,
    pub index_owner_id: Index,
}

impl SystemTable for PicoTable {
    const TABLE_NAME: &'static str = "_pico_table";
    const TABLE_ID: SpaceId = 512;
    const DESCRIPTION: &'static str = "Stores metadata of all the cluster tables in picodata.";

    fn format() -> Vec<tarantool::space::Field> {
        TableDef::format()
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![
            IndexDef {
                table_id: Self::TABLE_ID,
                // Primary index
                id: 0,
                name: "_pico_table_id".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![Part::from(("id", IndexFieldType::Unsigned)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 1,
                name: "_pico_table_name".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![Part::from(("name", IndexFieldType::String)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 2,
                name: "_pico_table_owner_id".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(false)],
                parts: vec![Part::from(("owner", IndexFieldType::Unsigned)).is_nullable(false)],
                operable: true,
                // This means the local schema is already up to date and main loop doesn't need to do anything
                schema_version: INITIAL_SCHEMA_VERSION,
            },
        ]
    }
}

impl PicoTable {
    pub const fn new() -> Self {
        Self {
            space: space_by_id_unchecked(Self::TABLE_ID),
            index_id: index_by_ids_unchecked(Self::TABLE_ID, 0),
            index_name: index_by_ids_unchecked(Self::TABLE_ID, 1),
            index_owner_id: index_by_ids_unchecked(Self::TABLE_ID, 2),
        }
    }

    pub fn create(&self) -> tarantool::Result<()> {
        let space = Space::builder(Self::TABLE_NAME)
            .id(Self::TABLE_ID)
            .space_type(SpaceType::DataLocal)
            .format(Self::format())
            .if_not_exists(true)
            .create()?;

        let index_id = space
            .index_builder("_pico_table_id")
            .unique(true)
            .part("id")
            .if_not_exists(true)
            .create()?;

        let index_name = space
            .index_builder("_pico_table_name")
            .unique(true)
            .part("name")
            .if_not_exists(true)
            .create()?;

        let index_owner_id = space
            .index_builder("_pico_table_owner_id")
            .unique(false)
            .part("owner")
            .if_not_exists(true)
            .create()?;

        debug_assert_eq!(self.space.id(), space.id());
        debug_assert_eq!(self.index_id.id(), index_id.id());
        debug_assert_eq!(self.index_name.id(), index_name.id());
        debug_assert_eq!(self.index_owner_id.id(), index_owner_id.id());

        Ok(())
    }

    #[inline(always)]
    pub fn rename(&self, id: u32, new_name: &str) -> tarantool::Result<()> {
        // We can't use UpdateOps as we use custom encoding
        let mut table_def = self.get(id)?.expect("should exist");
        table_def.name = new_name.into();
        self.put(&table_def)?;
        Ok(())
    }

    #[inline]
    pub fn get(&self, id: SpaceId) -> tarantool::Result<Option<TableDef>> {
        use ::tarantool::msgpack;

        let Some(tuple) = self.space.get(&[id])? else {
            return Ok(None);
        };
        let buf = tuple.to_vec();
        let table_def = msgpack::decode(&buf)?;
        Ok(Some(table_def))
    }

    #[inline]
    pub fn put(&self, table_def: &TableDef) -> tarantool::Result<()> {
        use ::tarantool::msgpack;

        self.space
            .replace(RawBytes::new(&msgpack::encode(table_def)))?;
        Ok(())
    }

    #[inline]
    pub fn insert(&self, table_def: &TableDef) -> tarantool::Result<()> {
        use ::tarantool::msgpack;

        self.space
            .insert(RawBytes::new(&msgpack::encode(table_def)))?;
        Ok(())
    }

    #[inline]
    pub fn update_operable(&self, id: SpaceId, operable: bool) -> tarantool::Result<()> {
        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(column_name!(TableDef, operable), operable)?;
        self.space.update(&[id], ops)?;
        Ok(())
    }

    #[inline]
    pub fn update_format(
        &self,
        id: SpaceId,
        format: &[tarantool::space::Field],
        column_renames: &RenameMapping,
    ) -> tarantool::Result<()> {
        // We can't use UpdateOps as we use custom encoding
        let mut table_def = self.get(id)?.expect("should exist");
        // apply renames to the distribution
        column_renames.transform_distribution_columns(&mut table_def.distribution);
        table_def.format = format.to_vec();
        self.put(&table_def)?;
        Ok(())
    }

    /// Change the field value of `schema_version` in the metadata of the specified table
    ///
    /// Changing the `schema_version` is necessary when changing the schema of the table to
    /// invalidate various caches, like the prepared statement cache.
    ///
    /// # Usage Example
    ///
    /// ```no_run
    /// # use tarantool::space::SpaceId;
    /// let tables = picodata::catalog::pico_table::PicoTable::new();
    ///
    /// let table_id = 28;
    /// let new_schema_version = 42;
    ///
    /// tables
    ///     .update_schema_version(table_id, new_schema_version)
    ///     .expect("updating schema_version failed");
    /// ```
    #[inline]
    pub fn update_schema_version(&self, id: SpaceId, schema_version: u64) -> tarantool::Result<()> {
        let mut ops = UpdateOps::with_capacity(1);
        ops.assign(column_name!(TableDef, schema_version), schema_version)?;
        self.space.update(&[id], ops)?;
        Ok(())
    }

    #[inline]
    pub fn delete(&self, id: SpaceId) -> tarantool::Result<Option<Tuple>> {
        self.space.delete(&[id])
    }

    #[inline]
    pub fn by_name(&self, name: &str) -> tarantool::Result<Option<TableDef>> {
        use ::tarantool::msgpack;

        let tuple = self.index_name.get(&[name])?;
        if let Some(tuple) = tuple {
            let table_def = msgpack::decode(&tuple.to_vec())?; // TODO: do we need to vec?
            Ok(Some(table_def))
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn by_id(&self, id: SpaceId) -> tarantool::Result<Option<TableDef>> {
        use ::tarantool::msgpack;

        let tuple = self.index_id.get(&[id])?;
        if let Some(tuple) = tuple {
            let table_def = msgpack::decode(&tuple.to_vec())?; // TODO: do we need to vec?
            Ok(Some(table_def))
        } else {
            Ok(None)
        }
    }

    pub fn by_owner_id(
        &self,
        owner_id: UserId,
    ) -> tarantool::Result<EntryIter<TableDef, MP_CUSTOM>> {
        let iter = self.index_owner_id.select(IteratorType::Eq, &[owner_id])?;
        Ok(EntryIter::new(iter))
    }
}

impl Default for PicoTable {
    fn default() -> Self {
        Self::new()
    }
}

impl ToEntryIter<MP_CUSTOM> for PicoTable {
    type Entry = TableDef;

    #[inline(always)]
    fn index_iter(&self) -> tarantool::Result<IndexIterator> {
        self.space.select(IteratorType::All, &())
    }
}
