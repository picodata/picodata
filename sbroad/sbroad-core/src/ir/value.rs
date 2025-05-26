//! Value module.

use rmp::Marker;
use serde::{Deserialize, Serialize, Serializer};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::cmp::Ordering;
use std::fmt::{self, Display};
use std::hash::Hash;
use std::io::Write;
use std::num::NonZeroI32;
use std::str::FromStr;
use tarantool::datetime::Datetime;
use tarantool::decimal::Decimal;
use tarantool::ffi::datetime::MP_DATETIME;
use tarantool::ffi::decimal::MP_DECIMAL;
use tarantool::ffi::uuid::MP_UUID;
use tarantool::msgpack::{Context, Decode, DecodeError, Encode, EncodeError, ExtStruct};
use tarantool::tlua;
use tarantool::tuple::{FieldType, KeyDefPart};
use tarantool::uuid::Uuid;

use crate::error;
use crate::errors::{Entity, SbroadError};
use crate::executor::hash::ToHashString;
use crate::ir::relation::DerivedType;
use crate::ir::value::double::Double;

use super::relation::Type;
use crate::frontend::sql::{try_parse_bool, try_parse_datetime};

#[derive(
    Debug, Serialize, Deserialize, Hash, PartialEq, Eq, Clone, PartialOrd, Ord, Encode, Decode,
)]
pub struct Tuple(pub(crate) Vec<Value>);

impl Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "[{}]",
            self.0
                .iter()
                .map(ToSmolStr::to_smolstr)
                .collect::<Vec<SmolStr>>()
                .join(",")
        )
    }
}

impl From<Vec<Value>> for Tuple {
    fn from(v: Vec<Value>) -> Self {
        Tuple(v)
    }
}

impl<L: tlua::AsLua> tlua::Push<L> for Tuple {
    type Err = tlua::Void;

    #[allow(unreachable_code)]
    fn push_to_lua(&self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        match self.0.push_to_lua(lua) {
            Ok(r) => Ok(r),
            Err(e) => {
                error!(Option::from("push ir tuple to lua"), &format!("{:?}", e.0),);
                Err((tlua::Void::from(e.0), e.1))
            }
        }
    }
}

impl<L> tlua::PushInto<L> for Tuple
where
    L: tlua::AsLua,
{
    type Err = tlua::Void;

    #[allow(unreachable_code)]
    fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        match self.0.push_into_lua(lua) {
            Ok(r) => Ok(r),
            Err(e) => {
                error!(
                    Option::from("push ir tuple into lua"),
                    &format!("{:?}", e.0),
                );
                Err((tlua::Void::from(e.0), e.1))
            }
        }
    }
}

impl<L> tlua::PushOneInto<L> for Tuple where L: tlua::AsLua {}

impl<L> tlua::LuaRead<L> for Tuple
where
    L: tlua::AsLua,
{
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Tuple, (L, tlua::WrongType)> {
        match Vec::lua_read_at_position(lua, index) {
            Ok(v) => Ok(Tuple::from(v)),
            Err(lua) => Err(lua),
        }
    }
}

/// SQL uses three-valued logic. We need to implement
/// it to compare values with each other.
#[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Trivalent {
    False,
    True,
    Unknown,
}

impl From<bool> for Trivalent {
    fn from(f: bool) -> Self {
        if f {
            Trivalent::True
        } else {
            Trivalent::False
        }
    }
}

/// Values are used to keep constants in the IR tree
/// or results in the virtual tables.
#[derive(Hash, PartialEq, Debug, Default, Clone, Deserialize, Serialize, PartialOrd, Ord)]
pub enum Value {
    /// Boolean type.
    Boolean(bool),
    /// Fixed point type.
    /// Box here to make the size of Value 32 bytes
    Decimal(Box<Decimal>),
    /// Floating point type.
    Double(Double),
    /// Datetime type,
    Datetime(Datetime),
    /// Signed integer type.
    Integer(i64),
    /// SQL NULL ("unknown" in the terms of three-valued logic).
    #[default]
    Null,
    /// String type.
    String(String),
    /// Unsigned integer type.
    Unsigned(u64),
    /// Tuple type
    Tuple(Tuple),
    /// Uuid type
    Uuid(Uuid),
}

impl<'de> Decode<'de> for Value {
    fn decode(r: &mut &'de [u8], context: &Context) -> Result<Self, DecodeError> {
        if r.is_empty() {
            return Err(DecodeError::new::<Self>("empty stream on value decode"));
        }

        let marker = Marker::from_u8(r[0]);
        match marker {
            Marker::Null => {
                rmp::decode::read_nil(r).map_err(DecodeError::from_vre::<Self>)?;
                Ok(Value::Null)
            }
            Marker::True | Marker::False => {
                let v = rmp::decode::read_bool(r).map_err(DecodeError::from_vre::<Self>)?;
                Ok(Value::Boolean(v))
            }
            Marker::FixPos(val) => {
                rmp::decode::read_pfix(r).map_err(DecodeError::from_vre::<Self>)?;
                Ok(Value::from(val as u64))
            }
            Marker::FixNeg(val) => {
                rmp::decode::read_nfix(r).map_err(DecodeError::from_vre::<Self>)?;
                Ok(Value::from(val as i64))
            }
            Marker::U8 => Ok(Value::from(u8::decode(r, context)? as u64)),
            Marker::U16 => Ok(Value::from(u16::decode(r, context)? as u64)),
            Marker::U32 => Ok(Value::from(u32::decode(r, context)? as u64)),
            Marker::U64 => Ok(Value::from(u64::decode(r, context)?)),
            Marker::I8 => Ok(Value::from(i8::decode(r, context)? as i64)),
            Marker::I16 => Ok(Value::from(i16::decode(r, context)? as i64)),
            Marker::I32 => Ok(Value::from(i32::decode(r, context)? as i64)),
            Marker::I64 => Ok(Value::from(i64::decode(r, context)?)),
            Marker::F32 => Ok(Value::from(f32::decode(r, context)? as f64)),
            Marker::F64 => Ok(Value::from(f64::decode(r, context)?)),
            Marker::FixStr(_) | Marker::Str8 | Marker::Str16 | Marker::Str32 => {
                Ok(Value::String(String::decode(r, context)?))
            }
            Marker::FixArray(_) | Marker::Array16 | Marker::Array32 => {
                Ok(Vec::decode(r, context)?.into())
            }
            Marker::FixExt1
            | Marker::FixExt2
            | Marker::FixExt4
            | Marker::FixExt8
            | Marker::FixExt16
            | Marker::Ext8
            | Marker::Ext16
            | Marker::Ext32 => {
                let ext: ExtStruct = Decode::decode(r, context)?;

                match ext.tag {
                    MP_DECIMAL => {
                        let value: Decimal = ext.try_into().map_err(DecodeError::new::<Self>)?;
                        Ok(value.into())
                    }
                    MP_UUID => Ok(Value::Uuid(
                        ext.try_into().map_err(DecodeError::new::<Self>)?,
                    )),
                    MP_DATETIME => Ok(Value::Datetime(
                        ext.try_into().map_err(DecodeError::new::<Self>)?,
                    )),
                    tag => Err(DecodeError::new::<Self>(format_smolstr!(
                        "value with an unknown tag {tag}"
                    ))),
                }
            }
            Marker::FixMap(_)
            | Marker::Map16
            | Marker::Map32
            | Marker::Bin8
            | Marker::Bin16
            | Marker::Bin32 => {
                let value = rmpv::decode::read_value(r).map_err(DecodeError::new::<Self>)?;
                Err(DecodeError::new::<Self>(format_smolstr!(
                    "unexpected value: {value:?}"
                )))
            }
            Marker::Reserved => {
                rmp::decode::read_marker(r).map_err(|e| DecodeError::new::<Self>(e.0))?;
                Err(DecodeError::new::<Self>("shouldn't be used"))
            }
        }
    }
}

impl Encode for Value {
    fn encode(&self, w: &mut impl Write, context: &Context) -> Result<(), EncodeError> {
        match self {
            Value::Boolean(v) => v.encode(w, context),
            Value::Decimal(v) => v.encode(w, context),
            Value::Double(v) => v.encode(w, context),
            Value::Datetime(v) => v.encode(w, context),
            Value::Integer(v) => v.encode(w, context),
            Value::Null => ().encode(w, context),
            Value::String(v) => v.encode(w, context),
            Value::Unsigned(v) => v.encode(w, context),
            Value::Tuple(v) => v.encode(w, context),
            Value::Uuid(v) => v.encode(w, context),
        }
    }
}

/// Custom Ordering using Trivalent instead of simple Equal.
/// We cannot even derive `PartialOrd` for Values because of Doubles.
#[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum TrivalentOrdering {
    Less,
    Equal,
    Greater,
    Unknown,
}

impl From<Ordering> for TrivalentOrdering {
    fn from(value: Ordering) -> Self {
        match value {
            Ordering::Less => TrivalentOrdering::Less,
            Ordering::Equal => TrivalentOrdering::Equal,
            Ordering::Greater => TrivalentOrdering::Greater,
        }
    }
}

impl TrivalentOrdering {
    /// Transforms `TrivalentOrdering` to Ordering.
    ///
    /// # Errors
    /// Unacceptable `TrivalentOrdering` to transform
    pub fn to_ordering(&self) -> Result<Ordering, SbroadError> {
        match self {
            Self::Less => Ok(Ordering::Less),
            Self::Equal => Ok(Ordering::Equal),
            Self::Greater => Ok(Ordering::Greater),
            Self::Unknown => Err(SbroadError::Invalid(
                Entity::Value,
                Some("Can not cast Unknown to Ordering".into()),
            )),
        }
    }
}

/// As a side effect, `NaN == NaN` is true.
/// We should manually care about this case in the code.
impl Eq for Value {}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Boolean(v) => write!(f, "{v}"),
            Value::Null => write!(f, "NULL"),
            Value::Unsigned(v) => write!(f, "{v}"),
            Value::Integer(v) => write!(f, "{v}"),
            Value::Datetime(v) => write!(f, "{v}"),
            Value::Double(v) => fmt::Display::fmt(&v, f),
            Value::Decimal(v) => fmt::Display::fmt(v, f),
            Value::String(v) => write!(f, "'{v}'"),
            Value::Tuple(v) => write!(f, "{v}"),
            Value::Uuid(v) => fmt::Display::fmt(v, f),
        }
    }
}

impl From<bool> for Value {
    fn from(f: bool) -> Self {
        Value::Boolean(f)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::Unsigned(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Integer(i64::from(v))
    }
}

impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Value::Unsigned(u64::from(v))
    }
}

impl From<Double> for Value {
    fn from(v: Double) -> Self {
        Value::Double(v)
    }
}

impl From<Datetime> for Value {
    fn from(v: Datetime) -> Self {
        Value::Datetime(v)
    }
}

impl From<Decimal> for Value {
    fn from(v: Decimal) -> Self {
        Value::Decimal(Box::new(v))
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<SmolStr> for Value {
    fn from(v: SmolStr) -> Self {
        Value::String(v.to_string())
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        if v.is_nan() {
            return Value::Null;
        }
        if v.is_subnormal() || v.is_infinite() || v.is_finite() && v.fract().abs() >= f64::EPSILON {
            Value::Double(v.into())
        } else {
            Value::Integer(v as i64)
        }
    }
}

impl From<Tuple> for Value {
    fn from(v: Tuple) -> Self {
        Value::Tuple(v)
    }
}

impl From<Vec<Value>> for Value {
    fn from(v: Vec<Value>) -> Self {
        let t = Tuple::from(v);
        Value::Tuple(t)
    }
}

impl From<Trivalent> for Value {
    fn from(f: Trivalent) -> Self {
        match f {
            Trivalent::False => Value::Boolean(false),
            Trivalent::True => Value::Boolean(true),
            Trivalent::Unknown => Value::Null,
        }
    }
}

impl From<Uuid> for Value {
    fn from(v: Uuid) -> Self {
        Value::Uuid(v)
    }
}

/// Helper function to extract inner numerical value from `value` and cast it to `Decimal`.
///
/// # Errors
/// - Inner `value` field is not numerical.
#[allow(dead_code)]
pub(crate) fn value_to_decimal_or_error(value: &Value) -> Result<Decimal, SbroadError> {
    match value {
        Value::Integer(s) => Ok(Decimal::from(*s)),
        Value::Unsigned(s) => Ok(Decimal::from(*s)),
        Value::Double(s) => {
            let from_string_cast = Decimal::from_str(&format!("{s}"));
            if let Ok(d) = from_string_cast {
                Ok(d)
            } else {
                Err(SbroadError::Invalid(
                    Entity::Value,
                    Some(format_smolstr!("Can't cast {value:?} to decimal")),
                ))
            }
        }
        Value::Decimal(s) => Ok(**s),
        _ => Err(SbroadError::Invalid(
            Entity::Value,
            Some(format_smolstr!(
                "Only numerical values can be casted to Decimal. {value:?} was met"
            )),
        )),
    }
}

impl Value {
    /// Adding. Applicable only to numerical values.
    ///
    /// # Errors
    /// - Passed values are not numerical.
    #[allow(dead_code)]
    pub(crate) fn add(&self, other: &Value) -> Result<Value, SbroadError> {
        let self_decimal = value_to_decimal_or_error(self)?;
        let other_decimal = value_to_decimal_or_error(other)?;

        Ok(Value::from(self_decimal + other_decimal))
    }

    /// Subtraction. Applicable only to numerical values.
    ///
    /// # Errors
    /// - Passed values are not numerical.
    #[allow(dead_code)]
    pub(crate) fn sub(&self, other: &Value) -> Result<Value, SbroadError> {
        let self_decimal = value_to_decimal_or_error(self)?;
        let other_decimal = value_to_decimal_or_error(other)?;

        Ok(Value::from(self_decimal - other_decimal))
    }

    /// Multiplication. Applicable only to numerical values.
    ///
    /// # Errors
    /// - Passed values are not numerical.
    #[allow(dead_code)]
    pub(crate) fn mult(&self, other: &Value) -> Result<Value, SbroadError> {
        let self_decimal = value_to_decimal_or_error(self)?;
        let other_decimal = value_to_decimal_or_error(other)?;

        Ok(Value::from(self_decimal * other_decimal))
    }

    /// Division. Applicable only to numerical values.
    ///
    /// # Errors
    /// - Passed values are not numerical.
    #[allow(dead_code)]
    pub(crate) fn div(&self, other: &Value) -> Result<Value, SbroadError> {
        let self_decimal = value_to_decimal_or_error(self)?;
        let other_decimal = value_to_decimal_or_error(other)?;

        if other_decimal == 0 {
            Err(SbroadError::Invalid(
                Entity::Value,
                Some(format_smolstr!("Can not divide {self:?} by zero {other:?}")),
            ))
        } else {
            Ok(Value::from(self_decimal / other_decimal))
        }
    }

    /// Negation. Applicable only to numerical values.
    ///
    /// # Errors
    /// - Passed value is not numerical.
    #[allow(dead_code)]
    pub(crate) fn negate(&self) -> Result<Value, SbroadError> {
        let self_decimal = value_to_decimal_or_error(self)?;

        Ok(Value::from(-self_decimal))
    }

    /// Concatenation. Applicable only to `Value::String`.
    ///
    /// # Errors
    /// - Passed values are not `Value::String`.
    #[allow(dead_code)]
    pub(crate) fn concat(&self, other: &Value) -> Result<Value, SbroadError> {
        let (Value::String(s), Value::String(o)) = (self, other) else {
            return Err(SbroadError::Invalid(
                Entity::Value,
                Some(format_smolstr!(
                    "{self:?} and {other:?} must be strings to be concatenated"
                )),
            ));
        };

        Ok(Value::from(format!("{s}{o}")))
    }

    /// Logical AND. Applicable only to `Value::Boolean`.
    ///
    /// # Errors
    /// - Passed values are not `Value::Boolean`.
    #[allow(dead_code)]
    pub(crate) fn and(&self, other: &Value) -> Result<Value, SbroadError> {
        let (Value::Boolean(s), Value::Boolean(o)) = (self, other) else {
            return Err(SbroadError::Invalid(
                Entity::Value,
                Some(format_smolstr!(
                    "{self:?} and {other:?} must be booleans to be applied to AND operation"
                )),
            ));
        };

        Ok(Value::from(*s && *o))
    }

    /// Logical OR. Applicable only to `Value::Boolean`.
    ///
    /// # Errors
    /// - Passed values are not `Value::Boolean`.
    #[allow(dead_code)]
    pub(crate) fn or(&self, other: &Value) -> Result<Value, SbroadError> {
        let (Value::Boolean(s), Value::Boolean(o)) = (self, other) else {
            return Err(SbroadError::Invalid(
                Entity::Value,
                Some(format_smolstr!(
                    "{self:?} and {other:?} must be booleans to be applied to OR operation"
                )),
            ));
        };

        Ok(Value::from(*s || *o))
    }

    /// Checks equality of the two values.
    /// The result uses three-valued logic.
    #[allow(clippy::too_many_lines)]
    #[must_use]
    pub fn eq(&self, other: &Value) -> Trivalent {
        match self {
            Value::Boolean(s) => match other {
                Value::Boolean(o) => (s == o).into(),
                Value::Null => Trivalent::Unknown,
                Value::Unsigned(_)
                | Value::Integer(_)
                | Value::Datetime(_)
                | Value::Decimal(_)
                | Value::Double(_)
                | Value::String(_)
                | Value::Uuid(_)
                | Value::Tuple(_) => Trivalent::False,
            },
            Value::Null => Trivalent::Unknown,
            Value::Integer(s) => match other {
                Value::Boolean(_)
                | Value::String(_)
                | Value::Uuid(_)
                | Value::Tuple(_)
                | Value::Datetime(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Integer(o) => (s == o).into(),
                Value::Decimal(o) => (Decimal::from(*s) == **o).into(),
                // If double can't be converted to decimal without error then it is not equal to integer.
                Value::Double(o) => (Decimal::from_str(&format!("{s}"))
                    == Decimal::from_str(&format!("{o}")))
                .into(),
                Value::Unsigned(o) => (&Decimal::from(*s) == o).into(),
            },
            Value::Double(s) => match other {
                Value::Boolean(_)
                | Value::String(_)
                | Value::Tuple(_)
                | Value::Uuid(_)
                | Value::Datetime(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Integer(o) => (*s == Double::from(*o)).into(),
                // If double can't be converted to decimal without error then it is not equal to decimal.
                Value::Decimal(o) => (Decimal::from_str(&format!("{s}")) == Ok(**o)).into(),
                Value::Double(o) => (s == o).into(),
                // If double can't be converted to decimal without error then it is not equal to unsigned.
                Value::Unsigned(o) => {
                    (Decimal::from_str(&format!("{s}")) == Ok(Decimal::from(*o))).into()
                }
            },
            Value::Decimal(s) => match other {
                Value::Boolean(_)
                | Value::String(_)
                | Value::Tuple(_)
                | Value::Uuid(_)
                | Value::Datetime(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Integer(o) => (**s == Decimal::from(*o)).into(),
                Value::Decimal(o) => (s == o).into(),
                // If double can't be converted to decimal without error then it is not equal to decimal.
                Value::Double(o) => (Ok(**s) == Decimal::from_str(&format!("{o}"))).into(),
                Value::Unsigned(o) => (**s == Decimal::from(*o)).into(),
            },
            Value::Unsigned(s) => match other {
                Value::Boolean(_)
                | Value::String(_)
                | Value::Uuid(_)
                | Value::Tuple(_)
                | Value::Datetime(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Integer(o) => (Decimal::from(*s) == *o).into(),
                Value::Decimal(o) => (Decimal::from(*s) == **o).into(),
                // If double can't be converted to decimal without error then it is not equal to unsigned.
                Value::Double(o) => {
                    (Ok(Decimal::from(*s)) == Decimal::from_str(&format!("{o}"))).into()
                }
                Value::Unsigned(o) => (s == o).into(),
            },
            Value::String(s) => match other {
                Value::Boolean(_)
                | Value::Integer(_)
                | Value::Datetime(_)
                | Value::Decimal(_)
                | Value::Double(_)
                | Value::Unsigned(_)
                | Value::Uuid(_)
                | Value::Tuple(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::String(o) => s.eq(o).into(),
            },
            Value::Tuple(_) => match other {
                Value::Boolean(_)
                | Value::Integer(_)
                | Value::Datetime(_)
                | Value::Decimal(_)
                | Value::Double(_)
                | Value::Unsigned(_)
                | Value::String(_)
                | Value::Uuid(_)
                | Value::Tuple(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
            },
            Value::Uuid(s) => match other {
                Value::Boolean(_)
                | Value::Integer(_)
                | Value::Datetime(_)
                | Value::Decimal(_)
                | Value::Double(_)
                | Value::String(_)
                | Value::Unsigned(_)
                | Value::Tuple(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Uuid(o) => s.eq(o).into(),
            },
            Value::Datetime(s) => match other {
                Value::Boolean(_)
                | Value::Integer(_)
                | Value::String(_)
                | Value::Decimal(_)
                | Value::Double(_)
                | Value::Unsigned(_)
                | Value::Uuid(_)
                | Value::Tuple(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Datetime(o) => s.eq(o).into(),
            },
        }
    }

    #[must_use]
    pub fn as_key_def_part(&self, field_no: u32) -> KeyDefPart {
        let field_type = match self {
            Value::Boolean(_) => FieldType::Boolean,
            Value::Integer(_) => FieldType::Integer,
            Value::Datetime(_) => FieldType::Datetime,
            Value::Decimal(_) => FieldType::Decimal,
            Value::Double(_) => FieldType::Double,
            Value::Unsigned(_) => FieldType::Unsigned,
            Value::String(_) => FieldType::String,
            Value::Tuple(_) => FieldType::Array,
            Value::Uuid(_) => FieldType::Uuid,
            Value::Null => FieldType::Any,
        };
        KeyDefPart {
            field_no,
            field_type,
            collation: None,
            is_nullable: true,
            path: None,
        }
    }

    /// Compares two values.
    /// The result uses four-valued logic (standard `Ordering` variants and
    /// `Unknown` in case `Null` was met).
    ///
    /// Returns `None` in case of
    /// * String casting Error or types mismatch.
    /// * Float `NaN` comparison occurred.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn partial_cmp(&self, other: &Value) -> Option<TrivalentOrdering> {
        match self {
            Value::Boolean(s) => match other {
                Value::Boolean(o) => TrivalentOrdering::from(s.cmp(o)).into(),
                Value::Null => TrivalentOrdering::Unknown.into(),
                Value::Unsigned(_)
                | Value::Integer(_)
                | Value::Datetime(_)
                | Value::Decimal(_)
                | Value::Double(_)
                | Value::String(_)
                | Value::Uuid(_)
                | Value::Tuple(_) => None,
            },
            Value::Null => TrivalentOrdering::Unknown.into(),
            Value::Integer(s) => match other {
                Value::Boolean(_)
                | Value::Datetime(_)
                | Value::String(_)
                | Value::Uuid(_)
                | Value::Tuple(_) => None,
                Value::Null => TrivalentOrdering::Unknown.into(),
                Value::Integer(o) => TrivalentOrdering::from(s.cmp(o)).into(),
                Value::Decimal(o) => TrivalentOrdering::from(Decimal::from(*s).cmp(o)).into(),
                // If double can't be converted to decimal without error then it is not equal to integer.
                Value::Double(o) => {
                    let self_converted = Decimal::from_str(&format!("{s}"));
                    let other_converted = Decimal::from_str(&format!("{o}"));
                    match (self_converted, other_converted) {
                        (Ok(d1), Ok(d2)) => TrivalentOrdering::from(d1.cmp(&d2)).into(),
                        _ => None,
                    }
                }
                Value::Unsigned(o) => {
                    TrivalentOrdering::from(Decimal::from(*s).cmp(&Decimal::from(*o))).into()
                }
            },
            Value::Datetime(s) => match other {
                Value::Boolean(_)
                | Value::Integer(_)
                | Value::Decimal(_)
                | Value::Double(_)
                | Value::Unsigned(_)
                | Value::Uuid(_)
                | Value::String(_)
                | Value::Tuple(_) => None,
                Value::Null => TrivalentOrdering::Unknown.into(),
                Value::Datetime(o) => TrivalentOrdering::from(s.cmp(o)).into(),
            },
            Value::Double(s) => match other {
                Value::Boolean(_)
                | Value::Datetime(_)
                | Value::String(_)
                | Value::Tuple(_)
                | Value::Uuid(_) => None,
                Value::Null => TrivalentOrdering::Unknown.into(),
                Value::Integer(o) => {
                    if let Some(ord) = s.partial_cmp(&Double::from(*o)) {
                        TrivalentOrdering::from(ord).into()
                    } else {
                        None
                    }
                }
                // If double can't be converted to decimal without error then it is not equal to decimal.
                Value::Decimal(o) => {
                    if let Ok(d) = Decimal::from_str(&format!("{s}")) {
                        TrivalentOrdering::from(d.cmp(o)).into()
                    } else {
                        None
                    }
                }
                Value::Double(o) => {
                    if let Some(ord) = s.partial_cmp(o) {
                        TrivalentOrdering::from(ord).into()
                    } else {
                        None
                    }
                }
                // If double can't be converted to decimal without error then it is not equal to unsigned.
                Value::Unsigned(o) => {
                    if let Ok(d) = Decimal::from_str(&format!("{s}")) {
                        TrivalentOrdering::from(d.cmp(&Decimal::from(*o))).into()
                    } else {
                        None
                    }
                }
            },
            Value::Decimal(s) => match other {
                Value::Boolean(_)
                | Value::Datetime(_)
                | Value::String(_)
                | Value::Uuid(_)
                | Value::Tuple(_) => None,
                Value::Null => TrivalentOrdering::Unknown.into(),
                Value::Integer(o) => TrivalentOrdering::from((**s).cmp(&Decimal::from(*o))).into(),
                Value::Decimal(o) => TrivalentOrdering::from(s.cmp(o)).into(),
                // If double can't be converted to decimal without error then it is not equal to decimal.
                Value::Double(o) => {
                    if let Ok(d) = Decimal::from_str(&format!("{o}")) {
                        TrivalentOrdering::from((**s).cmp(&d)).into()
                    } else {
                        None
                    }
                }
                Value::Unsigned(o) => TrivalentOrdering::from((**s).cmp(&Decimal::from(*o))).into(),
            },
            Value::Unsigned(s) => match other {
                Value::Boolean(_)
                | Value::Datetime(_)
                | Value::String(_)
                | Value::Uuid(_)
                | Value::Tuple(_) => None,
                Value::Null => TrivalentOrdering::Unknown.into(),
                Value::Integer(o) => {
                    TrivalentOrdering::from(Decimal::from(*s).cmp(&Decimal::from(*o))).into()
                }
                Value::Decimal(o) => TrivalentOrdering::from(Decimal::from(*s).cmp(o)).into(),
                // If double can't be converted to decimal without error then it is not equal to unsigned.
                Value::Double(o) => {
                    if let Ok(d) = Decimal::from_str(&format!("{o}")) {
                        TrivalentOrdering::from(Decimal::from(*s).cmp(&d)).into()
                    } else {
                        None
                    }
                }
                Value::Unsigned(o) => TrivalentOrdering::from(s.cmp(o)).into(),
            },
            Value::String(s) => match other {
                Value::Boolean(_)
                | Value::Integer(_)
                | Value::Datetime(_)
                | Value::Decimal(_)
                | Value::Double(_)
                | Value::Unsigned(_)
                | Value::Uuid(_)
                | Value::Tuple(_) => None,
                Value::Null => TrivalentOrdering::Unknown.into(),
                Value::String(o) => TrivalentOrdering::from(s.cmp(o)).into(),
            },
            Value::Uuid(u) => match other {
                Value::Boolean(_)
                | Value::Integer(_)
                | Value::Datetime(_)
                | Value::Decimal(_)
                | Value::Double(_)
                | Value::Unsigned(_)
                | Value::String(_)
                | Value::Tuple(_) => None,
                Value::Null => TrivalentOrdering::Unknown.into(),
                Value::Uuid(o) => TrivalentOrdering::from(u.cmp(o)).into(),
            },
            Value::Tuple(_) => match other {
                Value::Boolean(_)
                | Value::Integer(_)
                | Value::Datetime(_)
                | Value::Decimal(_)
                | Value::Double(_)
                | Value::Unsigned(_)
                | Value::String(_)
                | Value::Uuid(_)
                | Value::Tuple(_) => None,
                Value::Null => TrivalentOrdering::Unknown.into(),
            },
        }
    }

    /// Cast a value to a different type.
    #[allow(clippy::too_many_lines)]
    pub fn cast(self, column_type: Type) -> Result<Self, SbroadError> {
        fn cast_error(value: &Value, column_type: Type) -> SbroadError {
            SbroadError::Invalid(
                Entity::Value,
                Some(format_smolstr!("Failed to cast {value} to {column_type}.")),
            )
        }

        match column_type {
            Type::Any => Ok(self),
            Type::Array | Type::Map => match self {
                Value::Null => Ok(Value::Null),
                _ => Err(cast_error(&self, column_type)),
            },
            Type::Boolean => match self {
                Value::Boolean(_) => Ok(self),
                Value::Null => Ok(Value::Null),
                Value::String(ref s) => try_parse_bool(s)
                    .ok_or_else(|| cast_error(&self, column_type))
                    .map(Value::Boolean),
                _ => Err(cast_error(&self, column_type)),
            },
            Type::Datetime => match self {
                Value::Null => Ok(Value::Null),
                Value::Datetime(_) => Ok(self),
                Value::String(ref s) => try_parse_datetime(s)
                    .ok_or_else(|| cast_error(&self, column_type))
                    .map(Value::Datetime),
                _ => Err(cast_error(&self, column_type)),
            },
            Type::Decimal => match self {
                Value::Decimal(_) => Ok(self),
                Value::Double(ref v) => Ok(Value::Decimal(
                    Decimal::from_str(&format!("{v}"))
                        .map_err(|_| cast_error(&self, column_type))?
                        .into(),
                )),
                Value::Integer(v) => Ok(Value::Decimal(Decimal::from(v).into())),
                Value::Unsigned(v) => Ok(Value::Decimal(Decimal::from(v).into())),
                Value::String(ref v) => Ok(Value::Decimal(
                    Decimal::from_str(v)
                        .map_err(|_| cast_error(&self, column_type))?
                        .into(),
                )),
                Value::Null => Ok(Value::Null),
                _ => Err(cast_error(&self, column_type)),
            },
            Type::Double => match self {
                Value::Double(_) => Ok(self),
                Value::Decimal(v) => Ok(Value::Double(Double::from_str(&format!("{v}"))?)),
                Value::Integer(v) => Ok(Value::Double(Double::from(v))),
                Value::Unsigned(v) => Ok(Value::Double(Double::from(v))),
                Value::String(v) => Ok(Value::Double(Double::from_str(&v)?)),
                Value::Null => Ok(Value::Null),
                _ => Err(cast_error(&self, column_type)),
            },
            Type::Integer => match self {
                Value::Integer(_) => Ok(self),
                Value::Decimal(ref v) => Ok(Value::Integer(
                    v.to_i64().ok_or_else(|| cast_error(&self, column_type))?,
                )),
                Value::Double(ref v) => v
                    .to_string()
                    .parse::<i64>()
                    .map(Value::Integer)
                    .map_err(|_| cast_error(&self, column_type)),
                Value::Unsigned(v) => Ok(Value::Integer(
                    i64::try_from(v).map_err(|_| cast_error(&self, column_type))?,
                )),
                Value::String(ref v) => v
                    .parse::<i64>()
                    .map(Value::Integer)
                    .map_err(|_| cast_error(&self, column_type)),
                Value::Null => Ok(Value::Null),
                _ => Err(cast_error(&self, column_type)),
            },
            Type::String => match self {
                Value::String(_) => Ok(self),
                Value::Null => Ok(Value::Null),
                _ => Err(cast_error(&self, column_type)),
            },
            Type::Uuid => match self {
                Value::Uuid(_) => Ok(self),
                Value::String(ref v) => Ok(Value::Uuid(
                    Uuid::parse_str(v).map_err(|_| cast_error(&self, column_type))?,
                )),
                Value::Null => Ok(Value::Null),
                _ => Err(cast_error(&self, column_type)),
            },
            Type::Unsigned => match self {
                Value::Unsigned(_) => Ok(self),
                Value::Integer(v) => Ok(Value::Unsigned(
                    u64::try_from(v).map_err(|_| cast_error(&self, column_type))?,
                )),
                Value::Decimal(ref v) => Ok(Value::Unsigned(
                    v.to_u64().ok_or_else(|| cast_error(&self, column_type))?,
                )),
                Value::Double(ref v) => v
                    .to_string()
                    .parse::<u64>()
                    .map(Value::Unsigned)
                    .map_err(|_| cast_error(&self, column_type)),
                Value::String(ref v) => v
                    .parse::<u64>()
                    .map(Value::Unsigned)
                    .map_err(|_| cast_error(&self, column_type)),
                Value::Null => Ok(Value::Null),
                _ => Err(cast_error(&self, column_type)),
            },
        }
    }

    /// Cast a value to a different type and wrap into encoded value.
    /// If the target type is the same as the current type, the value
    /// is returned by reference. Otherwise, the value is cloned.
    ///
    /// # Errors
    /// - the value cannot be cast to the given type.
    #[allow(clippy::too_many_lines)]
    pub fn cast_and_encode(&self, column_type: &DerivedType) -> Result<EncodedValue, SbroadError> {
        let Some(column_type) = column_type.get() else {
            return Ok(self.into());
        };

        // First, try variants returning EncodedValue::Ref to avoid cloning.
        match (column_type, self) {
            (Type::Any, value) => return Ok(value.into()),
            (Type::Boolean, Value::Boolean(_)) => return Ok(self.into()),
            (Type::Datetime, Value::Datetime(_)) => return Ok(self.into()),
            (Type::Decimal, Value::Decimal(_)) => return Ok(self.into()),
            (Type::Double, Value::Double(_)) => return Ok(self.into()),
            (Type::Integer, Value::Integer(_)) => return Ok(self.into()),
            (Type::String, Value::String(_)) => return Ok(self.into()),
            (Type::Uuid, Value::Uuid(_)) => return Ok(self.into()),
            (Type::Unsigned, Value::Unsigned(_)) => return Ok(self.into()),
            _ => (),
        }

        // Then, apply cast with clone.
        self.clone().cast(*column_type).map(Into::into)
    }

    #[must_use]
    pub fn get_type(&self) -> DerivedType {
        let ty = match self {
            Value::Unsigned(_) => Type::Unsigned,
            Value::Integer(_) => Type::Integer,
            Value::Datetime(_) => Type::Datetime,
            Value::Decimal(_) => Type::Decimal,
            Value::Double(_) => Type::Double,
            Value::Boolean(_) => Type::Boolean,
            Value::String(_) => Type::String,
            Value::Tuple(_) => Type::Array,
            Value::Uuid(_) => Type::Uuid,
            Value::Null => return DerivedType::unknown(),
        };
        DerivedType::new(ty)
    }
}

impl ToHashString for Value {
    fn to_hash_string(&self) -> String {
        match self {
            Value::Unsigned(v) => v.to_string(),
            Value::Integer(v) => v.to_string(),
            Value::Datetime(v) => v.to_string(),
            // It is important to trim trailing zeros when converting to string.
            // Otherwise, the hash from `1.000` and `1` would be different,
            // though the values are the same.
            // We don't use internal hash function because we calculate the hash
            // from the string representation for all other types.
            Value::Decimal(v) => v.trim().to_string(),
            Value::Double(v) => v.to_string(),
            Value::Boolean(v) => v.to_string(),
            Value::String(v) => v.to_string(),
            Value::Tuple(v) => v.to_string(),
            Value::Uuid(v) => v.to_string(),
            Value::Null => "NULL".to_string(),
        }
    }
}

/// A helper enum to encode values into `MessagePack`.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub enum EncodedValue<'v> {
    Ref(MsgPackValue<'v>),
    Owned(Value),
}

impl Serialize for EncodedValue<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            EncodedValue::Ref(v) => v.serialize(serializer),
            EncodedValue::Owned(Value::Boolean(v)) => v.serialize(serializer),
            EncodedValue::Owned(Value::Decimal(v)) => v.serialize(serializer),
            EncodedValue::Owned(Value::Double(v)) => v.serialize(serializer),
            EncodedValue::Owned(Value::Datetime(v)) => v.serialize(serializer),
            EncodedValue::Owned(Value::Integer(v)) => v.serialize(serializer),
            EncodedValue::Owned(Value::Null) => ().serialize(serializer),
            EncodedValue::Owned(Value::String(v)) => v.serialize(serializer),
            EncodedValue::Owned(Value::Unsigned(v)) => v.serialize(serializer),
            EncodedValue::Owned(Value::Tuple(v)) => v.serialize(serializer),
            EncodedValue::Owned(Value::Uuid(v)) => v.serialize(serializer),
        }
    }
}

impl EncodedValue<'_> {
    /// Try to convert to double underlying value.
    pub fn double(&self) -> Option<f64> {
        match &self {
            EncodedValue::Ref(MsgPackValue::Double(value)) => Some(**value),
            EncodedValue::Owned(Value::Double(value)) => Some(value.value),
            _ => None,
        }
    }
}

impl<'v> From<MsgPackValue<'v>> for EncodedValue<'v> {
    fn from(value: MsgPackValue<'v>) -> Self {
        EncodedValue::Ref(value)
    }
}

impl<'v> From<&'v Value> for EncodedValue<'v> {
    fn from(value: &'v Value) -> Self {
        EncodedValue::from(MsgPackValue::from(value))
    }
}

impl From<Value> for EncodedValue<'_> {
    fn from(value: Value) -> Self {
        EncodedValue::Owned(value)
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Serialize, Clone)]
#[serde(untagged)]
pub enum MsgPackValue<'v> {
    Boolean(&'v bool),
    Datetime(&'v Datetime),
    Decimal(&'v Decimal),
    Double(&'v f64),
    Integer(&'v i64),
    Unsigned(&'v u64),
    String(&'v String),
    Tuple(&'v Tuple),
    Uuid(&'v Uuid),
    Null(()),
}

impl<'v> From<&'v Value> for MsgPackValue<'v> {
    fn from(value: &'v Value) -> Self {
        match value {
            Value::Boolean(v) => MsgPackValue::Boolean(v),
            Value::Datetime(v) => MsgPackValue::Datetime(v),
            Value::Decimal(v) => MsgPackValue::Decimal(v),
            Value::Double(v) => MsgPackValue::Double(&v.value),
            Value::Integer(v) => MsgPackValue::Integer(v),
            Value::Null => MsgPackValue::Null(()),
            Value::String(v) => MsgPackValue::String(v),
            Value::Tuple(v) => MsgPackValue::Tuple(v),
            Value::Uuid(v) => MsgPackValue::Uuid(v),
            Value::Unsigned(v) => MsgPackValue::Unsigned(v),
        }
    }
}

impl<'v> From<EncodedValue<'v>> for Value {
    fn from(value: EncodedValue<'v>) -> Self {
        match value {
            EncodedValue::Ref(MsgPackValue::Boolean(v)) => Value::Boolean(*v),
            EncodedValue::Ref(MsgPackValue::Datetime(v)) => Value::Datetime(*v),
            EncodedValue::Ref(MsgPackValue::Decimal(v)) => Value::Decimal((*v).into()),
            EncodedValue::Ref(MsgPackValue::Double(v)) => Value::Double(Double::from(*v)),
            EncodedValue::Ref(MsgPackValue::Integer(v)) => Value::Integer(*v),
            EncodedValue::Ref(MsgPackValue::Unsigned(v)) => Value::Unsigned(*v),
            EncodedValue::Ref(MsgPackValue::String(v)) => Value::String(v.clone()),
            EncodedValue::Ref(MsgPackValue::Tuple(v)) => Value::Tuple(v.clone()),
            EncodedValue::Ref(MsgPackValue::Uuid(v)) => Value::Uuid(*v),
            EncodedValue::Ref(MsgPackValue::Null(())) => Value::Null,
            EncodedValue::Owned(v) => v,
        }
    }
}

impl From<Value> for String {
    fn from(v: Value) -> Self {
        match v {
            Value::Unsigned(v) => v.to_string(),
            Value::Integer(v) => v.to_string(),
            Value::Datetime(v) => v.to_string(),
            Value::Decimal(v) => v.to_string(),
            Value::Double(v) => v.to_string(),
            Value::Boolean(v) => v.to_string(),
            Value::String(v) => v,
            Value::Tuple(v) => v.to_string(),
            Value::Uuid(v) => v.to_string(),
            Value::Null => "NULL".to_string(),
        }
    }
}

impl<L: tlua::AsLua> tlua::Push<L> for Value {
    type Err = tlua::Void;

    fn push_to_lua(&self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        match self {
            Value::Unsigned(v) => v.push_to_lua(lua),
            Value::Integer(v) => v.push_to_lua(lua),
            Value::Datetime(v) => v.push_to_lua(lua),
            Value::Decimal(v) => v.push_to_lua(lua),
            Value::Double(v) => v.push_to_lua(lua),
            Value::Boolean(v) => v.push_to_lua(lua),
            Value::String(v) => v.push_to_lua(lua),
            Value::Tuple(v) => v.push_to_lua(lua),
            Value::Uuid(v) => v.push_to_lua(lua),
            Value::Null => tlua::Null.push_to_lua(lua),
        }
    }
}

impl<L> tlua::PushInto<L> for Value
where
    L: tlua::AsLua,
{
    type Err = tlua::Void;

    fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        match self {
            Value::Unsigned(v) => v.push_into_lua(lua),
            Value::Integer(v) => v.push_into_lua(lua),
            Value::Datetime(v) => v.push_into_lua(lua),
            Value::Decimal(v) => v.push_into_lua(lua),
            Value::Double(v) => v.push_into_lua(lua),
            Value::Boolean(v) => v.push_into_lua(lua),
            Value::String(v) => v.push_into_lua(lua),
            Value::Tuple(v) => v.push_into_lua(lua),
            Value::Uuid(v) => v.push_into_lua(lua),
            Value::Null => tlua::Null.push_into_lua(lua),
        }
    }
}

impl<L> tlua::PushOneInto<L> for Value where L: tlua::AsLua {}

impl<L> tlua::LuaRead<L> for Value
where
    L: tlua::AsLua,
{
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Value, (L, tlua::WrongType)> {
        // At the moment Tarantool module can't distinguish between
        // double and integer/unsigned. So we have to do it manually.
        if let Ok(v) = f64::lua_read_at_position(&lua, index) {
            if v.is_subnormal()
                || v.is_nan()
                || v.is_infinite()
                || v.is_finite() && v.fract().abs() >= f64::EPSILON
            {
                return Ok(Value::Double(Double::from(v)));
            }
        }
        let lua = match tlua::LuaRead::lua_read_at_position(lua, index) {
            Ok(v) => return Ok(Self::Unsigned(v)),
            Err((lua, _)) => lua,
        };
        let lua = match tlua::LuaRead::lua_read_at_position(lua, index) {
            Ok(v) => return Ok(Self::Integer(v)),
            Err((lua, _)) => lua,
        };
        let lua = match tlua::LuaRead::lua_read_at_position(lua, index) {
            Ok(v) => {
                let value: Decimal = v;
                return Ok(Self::Decimal(value.into()));
            }
            Err((lua, _)) => lua,
        };
        let lua = match tlua::LuaRead::lua_read_at_position(lua, index) {
            Ok(v) => {
                let value: Double = v;
                return Ok(Self::Double(value));
            }
            Err((lua, _)) => lua,
        };
        let lua = match tlua::LuaRead::lua_read_at_position(lua, index) {
            Ok(v) => return Ok(Self::Boolean(v)),
            Err((lua, _)) => lua,
        };
        let lua = match tlua::LuaRead::lua_read_at_position(lua, index) {
            Ok(v) => return Ok(Self::String(v)),
            Err((lua, _)) => lua,
        };
        let lua = match tlua::LuaRead::lua_read_at_position(lua, index) {
            Ok(v) => return Ok(Self::Tuple(v)),
            Err((lua, _)) => lua,
        };
        let lua = match tlua::LuaRead::lua_read_at_position(lua, index) {
            Ok(v) => return Ok(Self::Uuid(v)),
            Err((lua, _)) => lua,
        };
        let Err((lua, _)) = tlua::Null::lua_read_at_position(lua, index) else {
            return Ok(Self::Null);
        };

        let err = tlua::WrongType::info("reading value from Lua")
            .expected("Lua type that can be casted to sbroad value")
            .actual("unsupported Lua type");
        Err((lua, err))
    }
}

pub mod double;
#[cfg(test)]
mod tests;
