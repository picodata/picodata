use core::fmt;
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, ToSmolStr};
use sql_protocol::dql_encoder::ColumnType;
use sql_type_system::expr::NestedType as TypeSystemNestedType;
use sql_type_system::expr::Type as TypeSystemType;
use std::fmt::Formatter;
use std::io::Write;
use tarantool::msgpack;
use tarantool::msgpack::{Context, DecodeError, EncodeError};
use tarantool::space::{FieldType as SpaceFieldType, TypedArray};
use tarantool::tuple::FieldType;

use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::ast::Rule;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum NestedType {
    Integer,
    Double,
    Numeric,
    Text,
    Boolean,
    Datetime,
    Uuid,
    /// Element type for `json[]`.
    Map,
    /// Forbidden for user tables.
    Any,
}

impl From<NestedType> for TypeSystemNestedType {
    fn from(value: NestedType) -> Self {
        match value {
            NestedType::Integer => Self::Integer,
            NestedType::Double => Self::Double,
            NestedType::Numeric => Self::Numeric,
            NestedType::Text => Self::Text,
            NestedType::Boolean => Self::Boolean,
            NestedType::Datetime => Self::Datetime,
            NestedType::Uuid => Self::Uuid,
            NestedType::Map => Self::Map,
            NestedType::Any => Self::Any,
        }
    }
}

impl NestedType {
    /// Stable string tag naming this element type for `_pico_array_cast` builtin.
    #[must_use]
    pub fn as_marker(&self) -> &'static str {
        match self {
            NestedType::Integer => "int",
            NestedType::Double => "double",
            NestedType::Numeric => "decimal",
            NestedType::Text => "string",
            NestedType::Boolean => "bool",
            NestedType::Datetime => "datetime",
            NestedType::Uuid => "uuid",
            NestedType::Map => "map",
            NestedType::Any => "any",
        }
    }

    /// Inverse of [`NestedType::as_marker`].
    ///
    /// # Errors
    /// - `marker` is not a recognized element-type tag.
    pub fn from_marker(marker: &str) -> Result<Self, SbroadError> {
        let nested = match marker {
            "int" => NestedType::Integer,
            "double" => NestedType::Double,
            "decimal" => NestedType::Numeric,
            "string" => NestedType::Text,
            "bool" => NestedType::Boolean,
            "datetime" => NestedType::Datetime,
            "uuid" => NestedType::Uuid,
            "map" => NestedType::Map,
            "any" => NestedType::Any,
            other => {
                return Err(SbroadError::Invalid(
                    Entity::Type,
                    Some(format_smolstr!(
                        "unknown array element type marker: {other}"
                    )),
                ))
            }
        };
        Ok(nested)
    }
}

impl From<TypeSystemNestedType> for NestedType {
    fn from(value: TypeSystemNestedType) -> Self {
        match value {
            TypeSystemNestedType::Integer => Self::Integer,
            TypeSystemNestedType::Double => Self::Double,
            TypeSystemNestedType::Numeric => Self::Numeric,
            TypeSystemNestedType::Text => Self::Text,
            TypeSystemNestedType::Boolean => Self::Boolean,
            TypeSystemNestedType::Datetime => Self::Datetime,
            TypeSystemNestedType::Uuid => Self::Uuid,
            TypeSystemNestedType::Map => Self::Map,
            TypeSystemNestedType::Any => Self::Any,
        }
    }
}

/// SQL domain types with a precise mapping to tarantool field types.
/// User-defined DDL columns use [`ColumnDefType`].
#[derive(Serialize, Deserialize, PartialEq, Hash, Debug, Eq, Clone, Copy)]
pub enum DomainType {
    Array(NestedType),
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

/// Types allowed for user-defined table columns.
#[derive(Serialize, Deserialize, PartialEq, Hash, Debug, Eq, Clone, Copy)]
#[serde(try_from = "DomainType", into = "DomainType")]
pub struct ColumnDefType(DomainType);

impl ColumnDefType {
    #[must_use]
    pub fn is_scalar(&self) -> bool {
        self.0.is_scalar()
    }

    #[must_use]
    pub fn as_domain_type(&self) -> &DomainType {
        &self.0
    }
}

impl TryFrom<DomainType> for ColumnDefType {
    type Error = SbroadError;

    fn try_from(data_type: DomainType) -> Result<Self, Self::Error> {
        if matches!(
            data_type,
            DomainType::Array(NestedType::Any) | DomainType::Any
        ) {
            return Err(SbroadError::Unsupported(
                Entity::Type,
                Some(format_smolstr!(
                    "{data_type} is not allowed as a user column type"
                )),
            ));
        }
        Ok(Self(data_type))
    }
}

impl From<ColumnDefType> for DomainType {
    fn from(data_type: ColumnDefType) -> Self {
        data_type.0
    }
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
            DomainType::Array(NestedType::Any) => write!(f, "array"),
            DomainType::Array(elem) => {
                write!(f, "{}[]", UnrestrictedType::from(*elem))
            }
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
            DomainType::Array(_) => FieldType::Array,
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
            DomainType::Array(elem) => SpaceFieldType::Array((*elem).into()),
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

impl From<&ColumnDefType> for SpaceFieldType {
    fn from(data_type: &ColumnDefType) -> Self {
        Self::from(data_type.as_domain_type())
    }
}

impl From<&DomainType> for UnrestrictedType {
    fn from(data_type: &DomainType) -> Self {
        match data_type {
            DomainType::Array(elem) => UnrestrictedType::Array(*elem),
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
    Array(NestedType),
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
            CastType::Array(elem) => write!(f, "{}[]", UnrestrictedType::from(*elem)),
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
            CastType::Array(n) => TypeSystemType::Array((*n).into()),
        }
    }
}

impl TryFrom<&UnrestrictedType> for CastType {
    type Error = SbroadError;

    fn try_from(value: &UnrestrictedType) -> Result<Self, Self::Error> {
        match value {
            UnrestrictedType::Boolean => Ok(CastType::Boolean),
            UnrestrictedType::Decimal => Ok(CastType::Decimal),
            UnrestrictedType::Datetime => Ok(CastType::Datetime),
            UnrestrictedType::Double => Ok(CastType::Double),
            UnrestrictedType::Integer => Ok(CastType::Integer),
            UnrestrictedType::Uuid => Ok(CastType::Uuid),
            UnrestrictedType::String => Ok(CastType::String),
            UnrestrictedType::Array(n) => Ok(CastType::Array(*n)),
            UnrestrictedType::Map | UnrestrictedType::Any => Err(Self::Error::Invalid(
                Entity::Type,
                Some(format_smolstr!("cannot cast to Map/Any")),
            )),
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
///    - Examples: `ANY`
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
    Array(NestedType),

    // Internal service types
    Any,
}

impl From<NestedType> for TypedArray {
    fn from(elem: NestedType) -> Self {
        match elem {
            NestedType::Boolean => Self::Boolean,
            NestedType::Datetime => Self::Datetime,
            NestedType::Numeric => Self::Decimal,
            NestedType::Double => Self::Double,
            NestedType::Integer => Self::Integer,
            NestedType::Text => Self::String,
            NestedType::Uuid => Self::Uuid,
            NestedType::Map => Self::Map,
            NestedType::Any => Self::Any,
        }
    }
}

impl From<TypedArray> for NestedType {
    fn from(elem: TypedArray) -> Self {
        match elem {
            TypedArray::Boolean => Self::Boolean,
            TypedArray::Datetime => Self::Datetime,
            TypedArray::Decimal => Self::Numeric,
            TypedArray::Double => Self::Double,
            TypedArray::Integer => Self::Integer,
            TypedArray::String => Self::Text,
            TypedArray::Uuid => Self::Uuid,
            TypedArray::Map => Self::Map,
            TypedArray::Any => Self::Any,
        }
    }
}

impl From<NestedType> for UnrestrictedType {
    fn from(elem: NestedType) -> Self {
        match elem {
            NestedType::Boolean => UnrestrictedType::Boolean,
            NestedType::Datetime => UnrestrictedType::Datetime,
            NestedType::Numeric => UnrestrictedType::Decimal,
            NestedType::Double => UnrestrictedType::Double,
            NestedType::Integer => UnrestrictedType::Integer,
            NestedType::Text => UnrestrictedType::String,
            NestedType::Uuid => UnrestrictedType::Uuid,
            NestedType::Map => UnrestrictedType::Map,
            NestedType::Any => UnrestrictedType::Any,
        }
    }
}

impl TryFrom<UnrestrictedType> for NestedType {
    type Error = SbroadError;

    fn try_from(ty: UnrestrictedType) -> Result<Self, Self::Error> {
        match ty {
            UnrestrictedType::Boolean => Ok(NestedType::Boolean),
            UnrestrictedType::Datetime => Ok(NestedType::Datetime),
            UnrestrictedType::Decimal => Ok(NestedType::Numeric),
            UnrestrictedType::Double => Ok(NestedType::Double),
            UnrestrictedType::Integer => Ok(NestedType::Integer),
            UnrestrictedType::String => Ok(NestedType::Text),
            UnrestrictedType::Uuid => Ok(NestedType::Uuid),
            UnrestrictedType::Map => Ok(NestedType::Map),
            UnrestrictedType::Any => Ok(NestedType::Any),
            UnrestrictedType::Array(_) => Err(SbroadError::Invalid(
                Entity::Type,
                Some(format_smolstr!(
                    "type {ty} is not a valid array element type"
                )),
            )),
        }
    }
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
            "decimal" | "numeric" => Ok(UnrestrictedType::Decimal),
            "double" => Ok(UnrestrictedType::Double),
            "integer" | "unsigned" => Ok(UnrestrictedType::Integer),
            "string" | "text" => Ok(UnrestrictedType::String),
            "uuid" => Ok(UnrestrictedType::Uuid),
            "array" => Ok(UnrestrictedType::Array(NestedType::Any)),
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
            (UnrestrictedType::Array(_), UnrestrictedType::Array(_))
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
            CastType::Array(n) => UnrestrictedType::Array(n),
        }
    }
}

impl TryFrom<Rule> for NestedType {
    type Error = SbroadError;

    fn try_from(ast_type: Rule) -> Result<Self, Self::Error> {
        match ast_type {
            Rule::TypeBool => Ok(NestedType::Boolean),
            Rule::TypeDatetime => Ok(NestedType::Datetime),
            Rule::TypeDecimal => Ok(NestedType::Numeric),
            Rule::TypeDouble => Ok(NestedType::Double),
            Rule::TypeInt => Ok(NestedType::Integer),
            Rule::TypeString | Rule::TypeText | Rule::TypeVarchar => Ok(NestedType::Text),
            Rule::TypeUuid => Ok(NestedType::Uuid),
            Rule::TypeJSON => Ok(NestedType::Map),
            _ => Err(SbroadError::Unsupported(
                Entity::Type,
                Some(format_smolstr!(
                    "{ast_type:?} cannot be used as an array element type"
                )),
            )),
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
            Rule::TypeJSON => Ok(Self::Map),
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
            SpaceFieldType::Array(label) => Ok(UnrestrictedType::Array(label.into())),
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
            UnrestrictedType::Array(_) => FieldType::Array,
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
            UnrestrictedType::Array(elem) => SpaceFieldType::Array((*elem).into()),
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
            UnrestrictedType::Array(NestedType::Any) => write!(f, "array"),
            UnrestrictedType::Array(elem) => {
                write!(f, "{}[]", UnrestrictedType::from(*elem))
            }
            UnrestrictedType::Any => write!(f, "any"),
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, Default)]
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
            ColumnType::Array => Self::new(UnrestrictedType::Array(NestedType::Any)),
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
            UnrestrictedType::Array(_) => ColumnType::Array,
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

#[cfg(test)]
mod tests {
    use super::{ColumnDefType, DomainType, NestedType};

    #[test]
    fn column_def_type_rejects_internal_types() {
        assert!(ColumnDefType::try_from(DomainType::Any).is_err());
        assert!(ColumnDefType::try_from(DomainType::Array(NestedType::Any)).is_err());

        let encoded = rmp_serde::to_vec(&DomainType::Any).unwrap();
        assert!(rmp_serde::from_slice::<ColumnDefType>(&encoded).is_err());
    }

    #[test]
    fn column_def_type_accepts_user_types() {
        let data_type = DomainType::Array(NestedType::Integer);
        let column_type = ColumnDefType::try_from(data_type).unwrap();

        assert_eq!(column_type.as_domain_type(), &data_type);
    }

    #[test]
    fn nested_type_marker_round_trips() {
        for nested in [
            NestedType::Integer,
            NestedType::Double,
            NestedType::Numeric,
            NestedType::Text,
            NestedType::Boolean,
            NestedType::Datetime,
            NestedType::Uuid,
            NestedType::Map,
            NestedType::Any,
        ] {
            assert_eq!(NestedType::from_marker(nested.as_marker()).unwrap(), nested);
        }
        assert!(NestedType::from_marker("bogus").is_err());
    }
}
