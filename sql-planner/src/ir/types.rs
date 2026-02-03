use core::fmt;
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;
use sql_protocol::dql_encoder::ColumnType;
use sql_type_system::expr::Type as TypeSystemType;
use std::fmt::Formatter;
use std::io::Write;
use tarantool::msgpack;
use tarantool::msgpack::{Context, DecodeError, EncodeError};
use tarantool::space::FieldType as SpaceFieldType;
use tarantool::tuple::FieldType;

use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::ast::Rule;

/// Data types available in DDL (`CREATE TABLE`, `ALTER TABLE`).
/// Precise mapping to tarantool field types.
#[derive(Serialize, Deserialize, PartialEq, Hash, Debug, Eq, Clone, Copy)]
pub enum DomainType {
    Json,
    Boolean,
    Datetime,
    Decimal,
    Double,
    Integer,
    String,
    Uuid,
    Unsigned,
    Any, // Not yet supported in grammar
}

impl DomainType {
    /// The type of the column is scalar.
    /// Only scalar types can be used as a distribution key.
    #[must_use]
    pub fn is_scalar(&self) -> bool {
        matches!(
            self,
            Self::Boolean
                | Self::Datetime
                | Self::Decimal
                | Self::Double
                | Self::Integer
                | Self::String
                | Self::Uuid
                | Self::Unsigned
        )
    }
}

impl fmt::Display for DomainType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DomainType::Boolean => write!(f, "bool"),
            DomainType::Decimal => write!(f, "decimal"),
            DomainType::Datetime => write!(f, "datetime"),
            DomainType::Double => write!(f, "double"),
            DomainType::Integer => write!(f, "int"),
            DomainType::String => write!(f, "string"),
            DomainType::Uuid => write!(f, "uuid"),
            DomainType::Json => write!(f, "json"),
            DomainType::Unsigned => write!(f, "unsigned"),
            DomainType::Any => write!(f, "any"),
        }
    }
}

impl From<&DomainType> for FieldType {
    fn from(data_type: &DomainType) -> Self {
        match data_type {
            DomainType::Boolean => FieldType::Boolean,
            DomainType::Decimal => FieldType::Decimal,
            DomainType::Datetime => FieldType::Datetime,
            DomainType::Double => FieldType::Double,
            DomainType::Integer => FieldType::Integer,
            DomainType::Unsigned => FieldType::Unsigned,
            DomainType::Uuid => FieldType::Uuid,
            DomainType::String => FieldType::String,
            DomainType::Json => FieldType::Map,
            DomainType::Any => FieldType::Any,
        }
    }
}

impl From<&DomainType> for SpaceFieldType {
    fn from(data_type: &DomainType) -> Self {
        match data_type {
            DomainType::Boolean => SpaceFieldType::Boolean,
            DomainType::Datetime => SpaceFieldType::Datetime,
            DomainType::Decimal => SpaceFieldType::Decimal,
            DomainType::Double => SpaceFieldType::Double,
            DomainType::Integer => SpaceFieldType::Integer,
            DomainType::Unsigned => SpaceFieldType::Unsigned,
            DomainType::String => SpaceFieldType::String,
            DomainType::Uuid => SpaceFieldType::Uuid,
            DomainType::Json => SpaceFieldType::Map,
            DomainType::Any => SpaceFieldType::Any,
        }
    }
}

impl From<&DomainType> for UnrestrictedType {
    fn from(data_type: &DomainType) -> Self {
        match data_type {
            DomainType::Boolean => UnrestrictedType::Boolean,
            DomainType::Datetime => UnrestrictedType::Datetime,
            DomainType::Decimal => UnrestrictedType::Decimal,
            DomainType::Double => UnrestrictedType::Double,
            DomainType::Integer | DomainType::Unsigned => UnrestrictedType::Integer,
            DomainType::String => UnrestrictedType::String,
            DomainType::Uuid => UnrestrictedType::Uuid,
            DomainType::Json => UnrestrictedType::Map,
            DomainType::Any => UnrestrictedType::Any,
        }
    }
}

// Types from CAST and it's aliases.
#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum CastType {
    Boolean,
    Datetime,
    Decimal,
    Double,
    Integer,
    Json,
    String,
    Uuid,
}

impl fmt::Display for CastType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            CastType::Boolean => write!(f, "bool"),
            CastType::Decimal => write!(f, "decimal"),
            CastType::Datetime => write!(f, "datetime"),
            CastType::Double => write!(f, "double"),
            CastType::Integer => write!(f, "int"),
            CastType::String => write!(f, "string"),
            CastType::Uuid => write!(f, "uuid"),
            CastType::Json => write!(f, "map"),
        }
    }
}

impl From<&CastType> for TypeSystemType {
    fn from(value: &CastType) -> Self {
        match value {
            CastType::Boolean => TypeSystemType::Boolean,
            CastType::Datetime => TypeSystemType::Datetime,
            CastType::Double => TypeSystemType::Double,
            CastType::Decimal => TypeSystemType::Numeric,
            CastType::Integer => TypeSystemType::Integer,
            CastType::Json => TypeSystemType::Map,
            CastType::String => TypeSystemType::Text,
            CastType::Uuid => TypeSystemType::Uuid,
        }
    }
}

impl From<&UnrestrictedType> for CastType {
    fn from(value: &UnrestrictedType) -> Self {
        match value {
            UnrestrictedType::Boolean => CastType::Boolean,
            UnrestrictedType::Decimal => CastType::Decimal,
            UnrestrictedType::Datetime => CastType::Datetime,
            UnrestrictedType::Double => CastType::Double,
            UnrestrictedType::Integer => CastType::Integer,
            UnrestrictedType::Uuid => CastType::Uuid,
            UnrestrictedType::String => CastType::String,
            UnrestrictedType::Map | UnrestrictedType::Any | UnrestrictedType::Array => {
                unreachable!("incorrect type transition, only scalar types supported");
            }
        }
    }
}

impl TryFrom<&Rule> for CastType {
    type Error = SbroadError;

    fn try_from(ast_type: &Rule) -> Result<Self, Self::Error> {
        match ast_type {
            Rule::TypeBool => Ok(Self::Boolean),
            Rule::TypeDatetime => Ok(Self::Datetime),
            Rule::TypeDecimal => Ok(Self::Decimal),
            Rule::TypeDouble => Ok(Self::Double),
            Rule::TypeInt => Ok(Self::Integer),
            Rule::TypeString | Rule::TypeText | Rule::TypeVarchar => Ok(Self::String),
            Rule::TypeUuid => Ok(Self::Uuid),
            _ => Err(SbroadError::Unsupported(
                Entity::Type,
                Some(format_smolstr!("{ast_type:?}")),
            )),
        }
    }
}

/// All types that can be encountered in sbroad internals.
/// Types can be divided into two main categories:
///
/// 1. **User-facing types** - available via SQL (DDL, CASTs)
///    - Represented by `DomainType` (which includes `unsigned`) and `CastType`
///    - Note: `Unsigned` is handled via `Integer` in this enum
///
/// 2. **Service types** - used internally by sbroad and picodata
///    - Examples: `ANY`, `ARRAY`
///    - Not exposed to end users
#[derive(Copy, Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum UnrestrictedType {
    // User-facing types
    Map, // aka JSON
    Boolean,
    Datetime,
    Decimal,
    Double,
    Integer,
    String,
    Uuid,

    // Internal service types
    Any,
    Array,
}

impl UnrestrictedType {
    /// Type constructor.
    /// Used in `Metadata` `table` method implementations to get columns type when constructing
    /// tables.
    ///
    /// # Errors
    /// - Invalid type name.
    pub fn new(s: &str) -> Result<Self, SbroadError> {
        match s.to_string().to_lowercase().as_str() {
            "boolean" => Ok(UnrestrictedType::Boolean),
            "any" => Ok(UnrestrictedType::Any),
            "datetime" => Ok(UnrestrictedType::Datetime),
            "decimal" => Ok(UnrestrictedType::Decimal),
            "double" => Ok(UnrestrictedType::Double),
            "integer" | "unsigned" => Ok(UnrestrictedType::Integer),
            "string" | "text" => Ok(UnrestrictedType::String),
            "uuid" => Ok(UnrestrictedType::Uuid),
            "array" => Ok(UnrestrictedType::Array),
            "map" => Ok(UnrestrictedType::Map),
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
            UnrestrictedType::Boolean
                | UnrestrictedType::Datetime
                | UnrestrictedType::Decimal
                | UnrestrictedType::Double
                | UnrestrictedType::Integer
                | UnrestrictedType::String
                | UnrestrictedType::Uuid
        )
    }

    /// Check if the type can be casted to another type.
    #[must_use]
    pub fn is_castable_to(&self, to: &UnrestrictedType) -> bool {
        matches!(
            (self, to),
            (UnrestrictedType::Array, UnrestrictedType::Array)
                | (UnrestrictedType::Boolean, UnrestrictedType::Boolean)
                | (
                    UnrestrictedType::Double
                        | UnrestrictedType::Integer
                        | UnrestrictedType::Decimal,
                    UnrestrictedType::Double
                        | UnrestrictedType::Integer
                        | UnrestrictedType::Decimal,
                )
                | (
                    UnrestrictedType::String | UnrestrictedType::Uuid,
                    UnrestrictedType::String | UnrestrictedType::Uuid
                )
        )
    }
}

impl From<CastType> for UnrestrictedType {
    fn from(cast_type: CastType) -> Self {
        match cast_type {
            CastType::Boolean => UnrestrictedType::Boolean,
            CastType::Datetime => UnrestrictedType::Datetime,
            CastType::Decimal => UnrestrictedType::Decimal,
            CastType::Double => UnrestrictedType::Double,
            CastType::Integer => UnrestrictedType::Integer,
            CastType::Json => UnrestrictedType::Map,
            CastType::String => UnrestrictedType::String,
            CastType::Uuid => UnrestrictedType::Uuid,
        }
    }
}

impl TryFrom<Rule> for UnrestrictedType {
    type Error = SbroadError;

    fn try_from(ast_type: Rule) -> Result<Self, Self::Error> {
        match ast_type {
            Rule::TypeBool => Ok(Self::Boolean),
            Rule::TypeDatetime => Ok(Self::Datetime),
            Rule::TypeDecimal => Ok(Self::Decimal),
            Rule::TypeDouble => Ok(Self::Double),
            Rule::TypeJSON => Ok(Self::Double),
            Rule::TypeInt => Ok(Self::Integer),
            Rule::TypeString | Rule::TypeText | Rule::TypeVarchar => Ok(Self::String),
            Rule::TypeUuid => Ok(Self::Uuid),
            _ => Err(SbroadError::Unsupported(
                Entity::Type,
                Some(format_smolstr!("{ast_type:?}")),
            )),
        }
    }
}

impl TryFrom<SpaceFieldType> for UnrestrictedType {
    type Error = SbroadError;

    fn try_from(field_type: SpaceFieldType) -> Result<Self, Self::Error> {
        match field_type {
            SpaceFieldType::Boolean => Ok(UnrestrictedType::Boolean),
            SpaceFieldType::Datetime => Ok(UnrestrictedType::Datetime),
            SpaceFieldType::Decimal => Ok(UnrestrictedType::Decimal),
            SpaceFieldType::Double => Ok(UnrestrictedType::Double),
            SpaceFieldType::Integer | SpaceFieldType::Unsigned => Ok(UnrestrictedType::Integer),
            SpaceFieldType::String => Ok(UnrestrictedType::String),
            SpaceFieldType::Array => Ok(UnrestrictedType::Array),
            SpaceFieldType::Uuid => Ok(UnrestrictedType::Uuid),
            SpaceFieldType::Any => Ok(UnrestrictedType::Any),
            SpaceFieldType::Map => Ok(UnrestrictedType::Map),
            SpaceFieldType::Varbinary | SpaceFieldType::Interval => Err(
                SbroadError::NotImplemented(Entity::Type, field_type.to_smolstr()),
            ),
            SpaceFieldType::Number | SpaceFieldType::Scalar => Err(SbroadError::Unsupported(
                Entity::Type,
                Some(field_type.to_smolstr()),
            )),
        }
    }
}

impl From<&UnrestrictedType> for FieldType {
    fn from(data_type: &UnrestrictedType) -> Self {
        match data_type {
            UnrestrictedType::Boolean => FieldType::Boolean,
            UnrestrictedType::Decimal => FieldType::Decimal,
            UnrestrictedType::Datetime => FieldType::Datetime,
            UnrestrictedType::Double => FieldType::Double,
            UnrestrictedType::Integer => FieldType::Integer,
            UnrestrictedType::Uuid => FieldType::Uuid,
            UnrestrictedType::String => FieldType::String,
            UnrestrictedType::Array => FieldType::Array,
            UnrestrictedType::Map => FieldType::Map,
            UnrestrictedType::Any => FieldType::Any,
        }
    }
}

impl From<&UnrestrictedType> for SpaceFieldType {
    fn from(data_type: &UnrestrictedType) -> Self {
        match data_type {
            UnrestrictedType::Boolean => SpaceFieldType::Boolean,
            UnrestrictedType::Datetime => SpaceFieldType::Datetime,
            UnrestrictedType::Decimal => SpaceFieldType::Decimal,
            UnrestrictedType::Double => SpaceFieldType::Double,
            UnrestrictedType::Integer => SpaceFieldType::Integer,
            UnrestrictedType::String => SpaceFieldType::String,
            UnrestrictedType::Uuid => SpaceFieldType::Uuid,
            UnrestrictedType::Array => SpaceFieldType::Array,
            UnrestrictedType::Map => SpaceFieldType::Map,
            UnrestrictedType::Any => SpaceFieldType::Any,
        }
    }
}

impl fmt::Display for UnrestrictedType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            UnrestrictedType::Boolean => write!(f, "bool"),
            UnrestrictedType::Decimal => write!(f, "decimal"),
            UnrestrictedType::Datetime => write!(f, "datetime"),
            UnrestrictedType::Double => write!(f, "double"),
            UnrestrictedType::Integer => write!(f, "int"),
            UnrestrictedType::String => write!(f, "string"),
            UnrestrictedType::Uuid => write!(f, "uuid"),
            UnrestrictedType::Map => write!(f, "map"),
            UnrestrictedType::Array => write!(f, "array"),
            UnrestrictedType::Any => write!(f, "any"),
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct DerivedType(Option<UnrestrictedType>);

impl DerivedType {
    pub fn unknown() -> Self {
        Self(None)
    }

    pub fn new(ty: UnrestrictedType) -> Self {
        Self(Some(ty))
    }

    pub fn get(&self) -> &Option<UnrestrictedType> {
        &self.0
    }

    pub fn get_mut(&mut self) -> &mut Option<UnrestrictedType> {
        &mut self.0
    }

    pub fn set(&mut self, ty: UnrestrictedType) {
        self.0 = Some(ty)
    }
}

impl fmt::Display for DerivedType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => write!(f, "unknown"),
            Some(t) => t.fmt(f),
        }
    }
}

impl From<ColumnType> for DerivedType {
    fn from(value: ColumnType) -> Self {
        (&value).into()
    }
}

impl From<&ColumnType> for DerivedType {
    fn from(value: &ColumnType) -> Self {
        match value {
            ColumnType::Map => Self::new(UnrestrictedType::Map),
            ColumnType::Boolean => Self::new(UnrestrictedType::Boolean),
            ColumnType::Datetime => Self::new(UnrestrictedType::Datetime),
            ColumnType::Decimal => Self::new(UnrestrictedType::Decimal),
            ColumnType::Double => Self::new(UnrestrictedType::Double),
            ColumnType::Integer => Self::new(UnrestrictedType::Integer),
            ColumnType::String => Self::new(UnrestrictedType::String),
            ColumnType::Uuid => Self::new(UnrestrictedType::Uuid),
            ColumnType::Any => Self::new(UnrestrictedType::Any),
            ColumnType::Array => Self::new(UnrestrictedType::Array),
            ColumnType::Scalar => Self::unknown(),
        }
    }
}

impl From<DerivedType> for ColumnType {
    fn from(value: DerivedType) -> Self {
        (&value).into()
    }
}

impl From<&DerivedType> for ColumnType {
    fn from(value: &DerivedType) -> Self {
        let Some(ty) = &value.get() else {
            return ColumnType::Scalar;
        };

        match ty {
            UnrestrictedType::Map => ColumnType::Map,
            UnrestrictedType::Boolean => ColumnType::Boolean,
            UnrestrictedType::Datetime => ColumnType::Datetime,
            UnrestrictedType::Decimal => ColumnType::Decimal,
            UnrestrictedType::Double => ColumnType::Double,
            UnrestrictedType::Integer => ColumnType::Integer,
            UnrestrictedType::String => ColumnType::String,
            UnrestrictedType::Uuid => ColumnType::Uuid,
            UnrestrictedType::Any => ColumnType::Any,
            UnrestrictedType::Array => ColumnType::Array,
        }
    }
}

impl msgpack::Encode for DerivedType {
    fn encode(&self, w: &mut impl Write, _context: &Context) -> Result<(), EncodeError> {
        let column_type: ColumnType = self.into();
        rmp::encode::write_pfix(w, column_type as u8).map_err(Into::into)
    }
}

impl<'de> msgpack::Decode<'de> for DerivedType {
    fn decode(r: &mut &'de [u8], _context: &Context) -> Result<Self, DecodeError> {
        let column_type: ColumnType = rmp::decode::read_pfix(r)
            .map_err(DecodeError::from_vre::<Self>)?
            .try_into()
            .map_err(DecodeError::new::<Self>)?;
        Ok(column_type.into())
    }
}
