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
use std::fmt::{self, Formatter};
use tarantool::index::Metadata as IndexMetadata;
use tarantool::space::{Field, FieldType as SpaceFieldType, Space, SpaceEngineType, SystemSpace};
use tarantool::tuple::{FieldType, KeyDef, KeyDefPart};
use tarantool::util::NumOrStr;

use serde::de::{Error, MapAccess, Visitor};
use serde::ser::{Serialize as SerSerialize, SerializeMap, Serializer};
use serde::{Deserialize, Deserializer, Serialize};

use crate::errors::{Action, Entity, SbroadError};
use crate::ir::value::Value;

use super::distribution::Key;

const DEFAULT_VALUE: Value = Value::Null;

/// Supported column types, which is used in a schema only.
/// This `Type` is derived from the result's metadata.
#[derive(Serialize, Default, Deserialize, PartialEq, Hash, Debug, Eq, Clone, Copy)]
pub enum Type {
    Any,
    Map,
    Array,
    Boolean,
    Datetime,
    Decimal,
    Double,
    Integer,
    #[default]
    Scalar,
    String,
    Number,
    Uuid,
    Unsigned,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Type::Array => write!(f, "array"),
            Type::Boolean => write!(f, "boolean"),
            Type::Decimal => write!(f, "decimal"),
            Type::Datetime => write!(f, "datetime"),
            Type::Double => write!(f, "double"),
            Type::Integer => write!(f, "integer"),
            Type::Scalar => write!(f, "scalar"),
            Type::String => write!(f, "string"),
            Type::Number => write!(f, "number"),
            Type::Uuid => write!(f, "uuid"),
            Type::Unsigned => write!(f, "unsigned"),
            Type::Any => write!(f, "any"),
            Type::Map => write!(f, "map"),
        }
    }
}

impl From<&Type> for FieldType {
    fn from(data_type: &Type) -> Self {
        match data_type {
            Type::Boolean => FieldType::Boolean,
            Type::Decimal => FieldType::Decimal,
            Type::Datetime => FieldType::Datetime,
            Type::Double => FieldType::Double,
            Type::Integer => FieldType::Integer,
            Type::Number => FieldType::Number,
            Type::Scalar => FieldType::Scalar,
            Type::Uuid => FieldType::Uuid,
            Type::String => FieldType::String,
            Type::Unsigned => FieldType::Unsigned,
            Type::Array => FieldType::Array,
            Type::Any => FieldType::Any,
            Type::Map => FieldType::Map,
        }
    }
}

impl From<&Type> for SpaceFieldType {
    fn from(data_type: &Type) -> Self {
        match data_type {
            Type::Boolean => SpaceFieldType::Boolean,
            Type::Datetime => SpaceFieldType::Datetime,
            Type::Decimal => SpaceFieldType::Decimal,
            Type::Double => SpaceFieldType::Double,
            Type::Integer => SpaceFieldType::Integer,
            Type::Number => SpaceFieldType::Number,
            Type::Scalar => SpaceFieldType::Scalar,
            Type::String => SpaceFieldType::String,
            Type::Uuid => SpaceFieldType::Uuid,
            Type::Unsigned => SpaceFieldType::Unsigned,
            Type::Array => SpaceFieldType::Array,
            Type::Any => SpaceFieldType::Any,
            Type::Map => SpaceFieldType::Map,
        }
    }
}

impl TryFrom<SpaceFieldType> for Type {
    type Error = SbroadError;

    fn try_from(field_type: SpaceFieldType) -> Result<Self, Self::Error> {
        match field_type {
            SpaceFieldType::Boolean => Ok(Type::Boolean),
            SpaceFieldType::Datetime => Ok(Type::Datetime),
            SpaceFieldType::Decimal => Ok(Type::Decimal),
            SpaceFieldType::Double => Ok(Type::Double),
            SpaceFieldType::Integer => Ok(Type::Integer),
            SpaceFieldType::Number => Ok(Type::Number),
            SpaceFieldType::Scalar => Ok(Type::Scalar),
            SpaceFieldType::String => Ok(Type::String),
            SpaceFieldType::Unsigned => Ok(Type::Unsigned),
            SpaceFieldType::Array => Ok(Type::Array),
            SpaceFieldType::Uuid => Ok(Type::Uuid),
            SpaceFieldType::Any
            | SpaceFieldType::Varbinary
            | SpaceFieldType::Map
            | SpaceFieldType::Interval => Err(SbroadError::NotImplemented(
                Entity::Type,
                field_type.to_smolstr(),
            )),
        }
    }
}

impl Type {
    /// Type constructor.
    /// Used in `Metadata` `table` method implementations to get columns type when constructing
    /// tables.
    ///
    /// # Errors
    /// - Invalid type name.
    pub fn new(s: &str) -> Result<Self, SbroadError> {
        match s.to_string().to_lowercase().as_str() {
            "boolean" => Ok(Type::Boolean),
            "datetime" => Ok(Type::Datetime),
            "decimal" => Ok(Type::Decimal),
            "double" => Ok(Type::Double),
            "integer" => Ok(Type::Integer),
            "number" => Ok(Type::Number),
            "scalar" => Ok(Type::Scalar),
            "string" | "text" => Ok(Type::String),
            "uuid" => Ok(Type::Uuid),
            "unsigned" => Ok(Type::Unsigned),
            "array" => Ok(Type::Array),
            "any" => Ok(Type::Any),
            "map" => Ok(Type::Map),
            v => Err(SbroadError::Invalid(
                Entity::Type,
                Some(format_smolstr!("Unable to transform {v} to Type.")),
            )),
        }
    }

    /// The type of the column is scalar.
    /// Only scalar types can be used as a distribution key.
    #[must_use]
    pub fn is_scalar(&self) -> bool {
        matches!(
            self,
            Type::Boolean
                | Type::Datetime
                | Type::Decimal
                | Type::Double
                | Type::Integer
                | Type::Number
                | Type::Scalar
                | Type::String
                | Type::Uuid
                | Type::Unsigned
        )
    }

    /// Check if the type can be casted to another type.
    #[must_use]
    pub fn is_castable_to(&self, to: &Type) -> bool {
        matches!(
            (self, to),
            (Type::Array, Type::Array)
                | (Type::Boolean, Type::Boolean)
                | (
                    Type::Double | Type::Integer | Type::Unsigned | Type::Decimal | Type::Number,
                    Type::Double | Type::Integer | Type::Unsigned | Type::Decimal | Type::Number,
                )
                | (Type::Scalar, Type::Scalar)
                | (Type::String | Type::Uuid, Type::String | Type::Uuid)
        )
    }
}

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
    pub r#type: Type,
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
            r#type: Type::default(),
            role: ColumnRole::default(),
            is_nullable: true,
        }
    }
}

impl From<Column> for Field {
    fn from(column: Column) -> Self {
        let field = match column.r#type {
            Type::Boolean => Field::boolean(column.name),
            Type::Datetime => Field::datetime(column.name),
            Type::Decimal => Field::decimal(column.name),
            Type::Double => Field::double(column.name),
            Type::Integer => Field::integer(column.name),
            Type::Number => Field::number(column.name),
            Type::Scalar => Field::scalar(column.name),
            Type::String => Field::string(column.name),
            Type::Uuid => Field::uuid(column.name),
            Type::Unsigned => Field::unsigned(column.name),
            Type::Array => Field::array(column.name),
            Type::Any => Field::any(column.name),
            Type::Map => Field::map(column.name),
        };
        field.is_nullable(true)
    }
}

impl From<&Column> for FieldType {
    fn from(column: &Column) -> Self {
        FieldType::from(&column.r#type)
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
        match &self.r#type {
            Type::Boolean => map.serialize_entry("type", "boolean")?,
            Type::Datetime => map.serialize_entry("type", "datetime")?,
            Type::Decimal => map.serialize_entry("type", "decimal")?,
            Type::Double => map.serialize_entry("type", "double")?,
            Type::Integer => map.serialize_entry("type", "integer")?,
            Type::Number => map.serialize_entry("type", "number")?,
            Type::Scalar => map.serialize_entry("type", "scalar")?,
            Type::String => map.serialize_entry("type", "string")?,
            Type::Uuid => map.serialize_entry("type", "uuid")?,
            Type::Unsigned => map.serialize_entry("type", "unsigned")?,
            Type::Array => map.serialize_entry("type", "array")?,
            Type::Any => map.serialize_entry("type", "any")?,
            Type::Map => map.serialize_entry("type", "map")?,
        }
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

        match column_type.as_str() {
            "any" => Ok(Column::new(&column_name, Type::Any, role, is_nullable)),
            "boolean" => Ok(Column::new(&column_name, Type::Boolean, role, is_nullable)),
            "datetime" => Ok(Column::new(&column_name, Type::Datetime, role, is_nullable)),
            "decimal" => Ok(Column::new(&column_name, Type::Decimal, role, is_nullable)),
            "double" => Ok(Column::new(&column_name, Type::Double, role, is_nullable)),
            "integer" => Ok(Column::new(&column_name, Type::Integer, role, is_nullable)),
            "number" | "numeric" => Ok(Column::new(&column_name, Type::Number, role, is_nullable)),
            "scalar" => Ok(Column::new(&column_name, Type::Scalar, role, is_nullable)),
            "string" | "text" | "varchar" => {
                Ok(Column::new(&column_name, Type::String, role, is_nullable))
            }
            "unsigned" => Ok(Column::new(&column_name, Type::Unsigned, role, is_nullable)),
            "array" => Ok(Column::new(&column_name, Type::Array, role, is_nullable)),
            "uuid" => Ok(Column::new(&column_name, Type::Uuid, role, is_nullable)),
            "map" => Ok(Column::new(&column_name, Type::Map, role, is_nullable)),
            s => Err(Error::custom(format!("unsupported column type: {s}"))),
        }
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
    pub fn new(n: &str, t: Type, role: ColumnRole, is_nullable: bool) -> Self {
        Column {
            name: n.into(),
            r#type: t,
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
        name: &str,
        columns: Vec<Column>,
        sharding_key: &[&str],
        primary_key: &[&str],
        engine: SpaceEngine,
        tier: Option<SmolStr>,
    ) -> Result<Self, SbroadError> {
        let mut table = Self::new_sharded(name, columns, sharding_key, primary_key, engine)?;
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
        name: &str,
        columns: Vec<Column>,
        primary_key: &[&str],
    ) -> Result<Self, SbroadError> {
        let (_, primary_key) = table_new_impl(name, &columns, primary_key)?;
        let kind = TableKind::new_global();
        Ok(Table {
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
        name: &str,
        columns: Vec<Column>,
        primary_key: &[&str],
    ) -> Result<Self, SbroadError> {
        let (_, primary_key) = table_new_impl(name, &columns, primary_key)?;
        let kind = TableKind::new_system();
        Ok(Table {
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
        let col_pos = if let NumOrStr::Num(pos) = part.field {
            pos as usize
        } else {
            return Err(SbroadError::Invalid(
                Entity::PrimaryKey,
                Some(format_smolstr!(
                    "part of {space_name} has unexpected format: {part:?}"
                )),
            ));
        };
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
