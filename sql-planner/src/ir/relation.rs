//! Relation module.
//!
//! Contains following structs:
//! * Column type (`Type`)
//! * Table column (`Column`)
//! * Engine (memtx/vinyl), used by a table (`SpaceEngine`)
//! * Table, representing unnamed tuples storage (`Table`)
//! * Relation, representing named tables (`Relations` as a map of { name -> table })

use ahash::AHashMap;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::collections::HashMap;
use std::fmt::Formatter;
use tarantool::index::Metadata as IndexMetadata;
use tarantool::space::{Field, Space, SpaceEngineType, SpaceId, SystemSpace};
use tarantool::tuple::{FieldType, KeyDef, KeyDefPart};

use crate::errors::{Action, Entity, SbroadError};
use crate::ir::value::Value;
use serde::de::{Error, MapAccess, Visitor};
use serde::ser::{Serialize as SerSerialize, SerializeMap, Serializer};
use serde::{Deserialize, Deserializer, Serialize};

use super::distribution::Key;
use super::types::{DerivedType, UnrestrictedType};

const DEFAULT_VALUE: Value = Value::Null;

/// A role of the column in the relation.
#[derive(Default, PartialEq, Debug, Eq, Clone)]
pub enum ColumnRole {
    /// General purpose column available for the user.
    #[default]
    User,
    /// Column is used for sharding (contains `bucket_id` in terms of `vshard`).
    Sharding,
}

/// Table column.
#[derive(PartialEq, Debug, Eq, Clone)]
pub struct Column {
    /// Column name.
    pub name: SmolStr,
    /// Column type.
    pub r#type: DerivedType,
    /// Column role.
    pub role: ColumnRole,
    /// Column is_nullable status.
    /// Possibly `None` (e.g. in case it's taken from Tarantool local query execution metatada).
    pub is_nullable: bool,
}

impl Default for Column {
    fn default() -> Self {
        Column {
            name: SmolStr::default(),
            r#type: DerivedType::unknown(),
            role: ColumnRole::default(),
            is_nullable: true,
        }
    }
}

impl From<Column> for Field {
    fn from(column: Column) -> Self {
        let field = if let Some(ty) = column.r#type.get() {
            match ty {
                UnrestrictedType::Boolean => Field::boolean(column.name),
                UnrestrictedType::Datetime => Field::datetime(column.name),
                UnrestrictedType::Decimal => Field::decimal(column.name),
                UnrestrictedType::Double => Field::double(column.name),
                UnrestrictedType::Integer => Field::integer(column.name),
                UnrestrictedType::String => Field::string(column.name),
                UnrestrictedType::Uuid => Field::uuid(column.name),
                UnrestrictedType::Array => Field::array(column.name),
                UnrestrictedType::Any => Field::any(column.name),
                UnrestrictedType::Map => Field::map(column.name),
            }
        } else {
            Field::scalar(column.name)
        };
        field.is_nullable(true)
    }
}

impl From<&Column> for FieldType {
    fn from(column: &Column) -> Self {
        if let Some(ty) = &column.r#type.get() {
            FieldType::from(ty)
        } else {
            FieldType::Scalar
        }
    }
}

impl Column {
    #[must_use]
    pub fn default_value() -> Value {
        DEFAULT_VALUE.clone()
    }
}

/// Msgpack serializer for a column
impl SerSerialize for Column {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("name", &self.name)?;

        let type_str = match &self.r#type.get() {
            Some(ty) => match ty {
                UnrestrictedType::Boolean => "boolean",
                UnrestrictedType::Datetime => "datetime",
                UnrestrictedType::Decimal => "decimal",
                UnrestrictedType::Double => "double",
                UnrestrictedType::Integer => "integer",
                UnrestrictedType::String => "string",
                UnrestrictedType::Uuid => "uuid",
                UnrestrictedType::Array => "array",
                UnrestrictedType::Any => "any",
                UnrestrictedType::Map => "map",
            },
            None => "unknown",
        };
        map.serialize_entry("type", type_str)?;

        map.serialize_entry(
            "role",
            match self.role {
                ColumnRole::User => "user",
                ColumnRole::Sharding => "sharding",
            },
        )?;

        map.end()
    }
}

struct ColumnVisitor;

impl<'de> Visitor<'de> for ColumnVisitor {
    type Value = Column;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("column parsing failed")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut column_name = String::new();
        let mut column_type = String::new();
        let mut column_role = String::new();
        let mut column_is_nullable = String::new();
        while let Some((key, value)) = map.next_entry::<String, String>()? {
            match key.as_str() {
                "name" => column_name.push_str(&value),
                "type" => column_type.push_str(&value.to_lowercase()),
                "role" => column_role.push_str(&value.to_lowercase()),
                "is_nullable" => column_is_nullable.push_str(&value.to_lowercase()),
                _ => return Err(Error::custom(format!("invalid column param: {key}"))),
            }
        }

        let role: ColumnRole = match column_role.as_str() {
            "sharding" => ColumnRole::Sharding,
            _ => ColumnRole::User,
        };

        let is_nullable = matches!(column_is_nullable.as_str(), "true");

        let ty = match column_type.as_str() {
            "any" => DerivedType::new(UnrestrictedType::Any),
            "boolean" => DerivedType::new(UnrestrictedType::Boolean),
            "datetime" => DerivedType::new(UnrestrictedType::Datetime),
            "decimal" | "numeric" => DerivedType::new(UnrestrictedType::Decimal),
            "double" => DerivedType::new(UnrestrictedType::Double),
            "integer" | "unsigned" => DerivedType::new(UnrestrictedType::Integer),
            "string" | "text" | "varchar" => DerivedType::new(UnrestrictedType::String),
            "array" => DerivedType::new(UnrestrictedType::Array),
            "uuid" => DerivedType::new(UnrestrictedType::Uuid),
            "map" => DerivedType::new(UnrestrictedType::Map),
            "unknown" => DerivedType::unknown(),
            s => return Err(Error::custom(format!("unsupported column type: {s}"))),
        };
        Ok(Column::new(&column_name, ty, role, is_nullable))
    }
}

impl<'de> Deserialize<'de> for Column {
    fn deserialize<D>(deserializer: D) -> Result<Column, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(ColumnVisitor)
    }
}

impl Column {
    /// Column constructor.
    #[must_use]
    pub fn new(n: &str, ty: DerivedType, role: ColumnRole, is_nullable: bool) -> Self {
        Column {
            name: n.into(),
            r#type: ty,
            role,
            is_nullable,
        }
    }

    /// Get column role.
    #[must_use]
    pub fn get_role(&self) -> &ColumnRole {
        &self.role
    }
}

/// Space engine type.
/// Duplicates tarantool module's `SpaceEngine` enum.
/// The reason for duplication - `SpaceEngine` can't be
/// deserialized with `serde_yaml` crate as it doesn't support
/// borrowed strings.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum SpaceEngine {
    Memtx,
    Vinyl,
}

impl From<SpaceEngineType> for SpaceEngine {
    #[inline]
    fn from(engine_type: SpaceEngineType) -> Self {
        Self::from(&engine_type)
    }
}

impl From<&SpaceEngineType> for SpaceEngine {
    #[inline]
    fn from(engine_type: &SpaceEngineType) -> Self {
        #[allow(unreachable_patterns)]
        match engine_type {
            SpaceEngineType::Memtx => SpaceEngine::Memtx,
            SpaceEngineType::Vinyl => SpaceEngine::Vinyl,
            system_space => panic!("Space engine '{system_space:?}' is not supported"),
        }
    }
}

impl From<SpaceEngine> for SpaceEngineType {
    fn from(space_type: SpaceEngine) -> Self {
        match space_type {
            SpaceEngine::Memtx => SpaceEngineType::Memtx,
            SpaceEngine::Vinyl => SpaceEngineType::Vinyl,
        }
    }
}

impl From<&SpaceEngine> for SpaceEngineType {
    fn from(space_type: &SpaceEngine) -> Self {
        match space_type {
            SpaceEngine::Memtx => SpaceEngineType::Memtx,
            SpaceEngine::Vinyl => SpaceEngineType::Vinyl,
        }
    }
}

impl TryFrom<&str> for SpaceEngine {
    type Error = SbroadError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "memtx" => Ok(SpaceEngine::Memtx),
            "vinyl" => Ok(SpaceEngine::Vinyl),
            _ => Err(SbroadError::FailedTo(
                Action::Deserialize,
                Some(Entity::SpaceEngine),
                format_smolstr!("unsupported space engine type: {value}"),
            )),
        }
    }
}

// A helper struct to collect column positions.
#[derive(Debug)]
pub(crate) struct ColumnPositions<'column> {
    // Column positions with names as keys.
    map: AHashMap<&'column str, usize>,
}

impl<'column> ColumnPositions<'column> {
    #[allow(clippy::uninlined_format_args)]
    pub(crate) fn new(columns: &'column [Column], table: &str) -> Result<Self, SbroadError> {
        let mut map = AHashMap::with_capacity(columns.len());
        for (pos, col) in columns.iter().enumerate() {
            let name = col.name.as_str();
            if let Some(old_pos) = map.insert(name, pos) {
                return Err(SbroadError::DuplicatedValue(format_smolstr!(
                    r#"Table "{}" has a duplicating column "{}" at positions {} and {}"#,
                    table,
                    name,
                    old_pos,
                    pos,
                )));
            }
        }
        map.shrink_to_fit();
        Ok(Self { map })
    }

    pub(crate) fn get(&self, name: &str) -> Option<usize> {
        self.map.get(name).copied()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum TableKind {
    ShardedSpace {
        sharding_key: Key,
        engine: SpaceEngine,
    },
    GlobalSpace,
    SystemSpace,
}

impl TableKind {
    #[must_use]
    pub fn new_sharded(sharding_key: Key, engine: SpaceEngine) -> Self {
        Self::ShardedSpace {
            sharding_key,
            engine,
        }
    }

    #[must_use]
    pub fn new_global() -> Self {
        Self::GlobalSpace
    }

    #[must_use]
    pub fn new_system() -> Self {
        Self::SystemSpace
    }
}

fn table_new_impl<'column>(
    name: &str,
    columns: &'column [Column],
    primary_key: &'column [&str],
) -> Result<(ColumnPositions<'column>, Key), SbroadError> {
    let pos_map = ColumnPositions::new(columns, name)?;
    let primary_positions = primary_key
        .iter()
        .map(|name| match pos_map.get(name) {
            Some(pos) => {
                let _ = &columns.get(pos).ok_or_else(|| {
                    SbroadError::FailedTo(
                        Action::Create,
                        Some(Entity::Column),
                        format_smolstr!("column {name} not found at position {pos}"),
                    )
                })?;
                Ok(pos)
            }
            None => Err(SbroadError::Invalid(Entity::PrimaryKey, None)),
        })
        .collect::<Result<Vec<usize>, _>>()?;
    Ok((pos_map, Key::new(primary_positions)))
}

/// Table is a tuple storage in the cluster.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Table {
    pub id: SpaceId,
    /// List of the columns.
    pub columns: Vec<Column>,
    /// Primary key of the table (column positions).
    pub primary_key: Key,
    /// Unique table name.
    pub name: SmolStr,
    pub kind: TableKind,
    pub tier: Option<SmolStr>,
}

impl Table {
    #[must_use]
    pub fn name(&self) -> &SmolStr {
        &self.name
    }

    /// Constructor for sharded table in specified tier.
    ///
    /// # Errors
    /// - column names are duplicated;
    /// - primary key is not found among the columns;
    /// - sharding key is not found among the columns;
    pub fn new_sharded_in_tier(
        id: SpaceId,
        name: &str,
        columns: Vec<Column>,
        sharding_key: &[&str],
        primary_key: &[&str],
        engine: SpaceEngine,
        tier: Option<SmolStr>,
    ) -> Result<Self, SbroadError> {
        let mut table = Self::new_sharded(id, name, columns, sharding_key, primary_key, engine)?;
        table.tier = tier;
        Ok(table)
    }

    /// Sharded table constructor.
    ///
    /// # Errors
    /// - column names are duplicated;
    /// - primary key is not found among the columns;
    /// - sharding key is not found among the columns;
    pub fn new_sharded(
        id: SpaceId,
        name: &str,
        columns: Vec<Column>,
        sharding_key: &[&str],
        primary_key: &[&str],
        engine: SpaceEngine,
    ) -> Result<Self, SbroadError> {
        let (pos_map, primary_key) = table_new_impl(name, &columns, primary_key)?;
        let sharding_key = Key::with_columns(&columns, &pos_map, sharding_key)?;
        let kind = TableKind::new_sharded(sharding_key, engine);
        Ok(Table {
            id,
            name: name.into(),
            columns,
            primary_key,
            kind,
            tier: None,
        })
    }

    /// Global table constructor.
    ///
    /// # Errors
    /// - column names are duplicated;
    /// - primary key is not found among the columns;
    pub fn new_global(
        id: SpaceId,
        name: &str,
        columns: Vec<Column>,
        primary_key: &[&str],
    ) -> Result<Self, SbroadError> {
        let (_, primary_key) = table_new_impl(name, &columns, primary_key)?;
        let kind = TableKind::new_global();
        Ok(Table {
            id,
            name: name.into(),
            columns,
            primary_key,
            kind,
            tier: None,
        })
    }

    /// System table constructor.
    ///
    /// # Errors
    /// - column names are duplicated;
    /// - primary key is not found among the columns;
    pub fn new_system(
        id: SpaceId,
        name: &str,
        columns: Vec<Column>,
        primary_key: &[&str],
    ) -> Result<Self, SbroadError> {
        let (_, primary_key) = table_new_impl(name, &columns, primary_key)?;
        let kind = TableKind::new_system();
        Ok(Table {
            id,
            name: name.into(),
            columns,
            primary_key,
            kind,
            tier: None,
        })
    }

    /// Get position of the `bucket_id` system column in the table.
    /// Return `None` if table is global
    ///
    /// # Errors
    /// - Table doesn't have an exactly one `bucket_id` column.
    pub fn get_bucket_id_position(&self) -> Result<Option<usize>, SbroadError> {
        if self.is_global() {
            return Ok(None);
        }
        let mut bucket_id_pos = None;
        for (pos, col) in self.columns.iter().enumerate() {
            if col.role == ColumnRole::Sharding {
                if bucket_id_pos.is_some() {
                    return Err(SbroadError::UnexpectedNumberOfValues(
                        "Table has more than one bucket_id column".into(),
                    ));
                }
                bucket_id_pos = Some(pos);
            }
        }
        if bucket_id_pos.is_none() {
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                "Table {} has no bucket_id columns",
                self.name
            )));
        }
        Ok(bucket_id_pos)
    }

    #[must_use]
    pub fn is_system(&self) -> bool {
        matches!(self.kind, TableKind::SystemSpace)
    }

    #[must_use]
    pub fn engine(&self) -> SpaceEngine {
        match &self.kind {
            TableKind::SystemSpace | TableKind::GlobalSpace => SpaceEngine::Memtx,
            TableKind::ShardedSpace { engine, .. } => engine.clone(),
        }
    }

    /// Get a vector of the sharding column names.
    ///
    /// # Errors
    /// - Table internal inconsistency.
    pub fn get_sharding_column_names(&self) -> Result<Vec<SmolStr>, SbroadError> {
        let sk = self.get_sk()?;
        let mut names: Vec<SmolStr> = Vec::with_capacity(sk.len());
        for pos in sk {
            names.push(
                self.columns
                    .get(*pos)
                    .ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Column,
                            format_smolstr!(
                                "(distribution column) at position {} for Table {}",
                                *pos,
                                self.name
                            ),
                        )
                    })?
                    .name
                    .clone(),
            );
        }
        Ok(names)
    }

    /// Get sharding key if this table is sharded.
    ///
    /// # Errors
    /// - The table is global
    pub fn get_sk(&self) -> Result<&[usize], SbroadError> {
        match &self.kind {
            TableKind::ShardedSpace {
                sharding_key: shard_key,
                ..
            } => Ok(&shard_key.positions),
            TableKind::GlobalSpace | TableKind::SystemSpace => Err(SbroadError::Invalid(
                Entity::Table,
                Some(format_smolstr!(
                    "expected sharded table. Name: {}",
                    self.name
                )),
            )),
        }
    }

    /// Get a sharding key definition for the table.
    ///
    /// # Errors
    /// - Table internal inconsistency.
    /// - Invalid sharding key position.
    pub fn get_key_def(&self) -> Result<KeyDef, SbroadError> {
        let mut parts = Vec::with_capacity(self.get_sk()?.len());
        for pos in self.get_sk()? {
            let column = self.columns.get(*pos).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Column,
                    format_smolstr!(
                        "(distribution column) at position {} for Table {}",
                        *pos,
                        self.name
                    ),
                )
            })?;
            let field_no = u32::try_from(*pos).map_err(|e| {
                SbroadError::Invalid(
                    Entity::Table,
                    Some(format_smolstr!("sharding key (position {pos}) error: {e}")),
                )
            })?;
            let part = KeyDefPart {
                field_no,
                field_type: FieldType::from(column),
                is_nullable: true,
                ..Default::default()
            };
            parts.push(part);
        }
        KeyDef::new(&parts).map_err(|e| SbroadError::Invalid(Entity::Table, Some(e.to_smolstr())))
    }

    #[must_use]
    pub fn is_global(&self) -> bool {
        matches!(self.kind, TableKind::GlobalSpace | TableKind::SystemSpace)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Relations {
    pub tables: HashMap<SmolStr, Table>,
}

impl Default for Relations {
    fn default() -> Self {
        Self::new()
    }
}

impl Relations {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn insert(&mut self, table: Table) {
        self.tables.insert(table.name().clone(), table);
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn drain(&mut self) -> HashMap<SmolStr, Table> {
        std::mem::take(&mut self.tables)
    }
}

/// Retrieve primary key columns for a space.
///
/// # Errors
/// - Space not found or invalid.
pub fn space_pk_columns(
    space_name: &str,
    space_columns: &[Column],
) -> Result<Vec<SmolStr>, SbroadError> {
    let space = Space::find(space_name)
        .ok_or_else(|| SbroadError::NotFound(Entity::Space, space_name.to_smolstr()))?;
    let index: Space = SystemSpace::Index.into();
    let tuple = index
        .get(&[space.id(), 0])
        .map_err(|e| {
            SbroadError::FailedTo(Action::Get, Some(Entity::Index), format_smolstr!("{e}"))
        })?
        .ok_or_else(|| {
            SbroadError::NotFound(
                Entity::PrimaryKey,
                format_smolstr!("for space {space_name}"),
            )
        })?;
    let pk_meta = tuple.decode::<IndexMetadata>().map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::PrimaryKey),
            format_smolstr!("{e}"),
        )
    })?;
    let mut primary_key = Vec::with_capacity(pk_meta.parts.len());
    for part in pk_meta.parts {
        let col_pos = part.field as usize;
        let col = space_columns
            .get(col_pos)
            .ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::PrimaryKey,
                    Some(format_smolstr!(
                        "{space_name} part referes to unknown column position: {col_pos}"
                    )),
                )
            })?
            .name
            .clone();
        primary_key.push(col);
    }
    Ok(primary_key)
}
#[cfg(test)]
mod tests;
