use bytes::{BufMut, Bytes, BytesMut};
use pgwire::types::ToSqlText;
use postgres_types::Type;
use postgres_types::{FromSql, ToSql};
use postgres_types::{IsNull, Oid};
use serde_json::Value;
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::error::Error;
use std::str;
use tarantool::tlua::{AsLua, Nil, PushInto};

use crate::pgproto::error::{DecodingError, PgError, PgResult};

pub fn type_from_name(name: &str) -> PgResult<Type> {
    match name {
        "integer" => Ok(Type::INT8),
        "string" => Ok(Type::TEXT),
        "boolean" => Ok(Type::BOOL),
        "double" => Ok(Type::FLOAT8),
        "any" => Ok(Type::ANY),
        _ => Err(PgError::FeatureNotSupported(format!(
            "unknown column type \'{name}\'"
        ))),
    }
}

/// This type is used to send Format over the wire.
pub type RawFormat = i16;

#[derive(Debug, Clone, Copy, Serialize_repr, Deserialize_repr)]
#[repr(i16)]
pub enum Format {
    Text = 0,
    Binary = 1,
}

impl<L: AsLua> PushInto<L> for Format {
    type Err = tarantool::tlua::Void;

    fn push_into_lua(self, lua: L) -> Result<tarantool::tlua::PushGuard<L>, (Self::Err, L)> {
        let value = self as RawFormat;
        value.push_into_lua(lua)
    }
}

impl TryFrom<RawFormat> for Format {
    type Error = PgError;
    fn try_from(value: RawFormat) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Format::Text),
            1 => Ok(Format::Binary),
            _ => Err(PgError::FeatureNotSupported(format!(
                "encoding type {value}"
            ))),
        }
    }
}

#[derive(Debug)]
pub enum PgValue {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Text(String),
    Null,
}

impl TryFrom<Value> for PgValue {
    type Error = PgError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let ret = match value {
            Value::Number(number) => {
                if number.is_f64() {
                    PgValue::Float(number.as_f64().unwrap())
                } else if number.is_i64() {
                    PgValue::Integer(number.as_i64().unwrap())
                } else {
                    Err(PgError::FeatureNotSupported(format!(
                        "unsupported type {number}"
                    )))?
                }
            }
            Value::String(string) => PgValue::Text(string),
            Value::Bool(bool) => PgValue::Boolean(bool),
            Value::Null => PgValue::Null,
            _ => Err(PgError::FeatureNotSupported(format!(
                "unsupported type {value}"
            )))?,
        };
        Ok(ret)
    }
}

fn decode_text_as_bool(s: &str) -> PgResult<bool> {
    // bool has many representations in text format.
    // NOTE: see `parse_bool_with_len` in pg
    match s {
        "t" | "true" | "yes" | "on" | "1" => Ok(true),
        "f" | "false" | "no" | "off" | "0" => Ok(false),
        _ => Err(PgError::DecodingError(DecodingError::Other(
            format!("couldn't decode \'{s}\' as bool").into(),
        ))),
    }
}

fn bool_to_sql_text(val: bool, buf: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
    // There are many representations of bool in text, but some connectors do not support all of them,
    // for instance, pg8000 doesn't recognize "true"/"false" as valid boolean values.
    // It seems that "t"/"f" variants are likely to be supported, because they are more efficient and
    // at least they work with psql, psycopg and pg8000.
    buf.put_u8(if val { b't' } else { b'f' });
    Ok(IsNull::No)
}

impl PgValue {
    fn encode_text(&self, buf: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        match &self {
            PgValue::Boolean(val) => bool_to_sql_text(*val, buf),
            PgValue::Integer(number) => number.to_sql_text(&Type::INT8, buf),
            PgValue::Float(number) => number.to_sql_text(&Type::FLOAT8, buf),
            PgValue::Text(string) => string.to_sql_text(&Type::TEXT, buf),
            PgValue::Null => Ok(IsNull::Yes),
        }
    }

    fn encode_binary(&self, buf: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        match &self {
            PgValue::Boolean(val) => val.to_sql(&Type::BOOL, buf),
            PgValue::Integer(number) => number.to_sql(&Type::INT8, buf),
            PgValue::Float(number) => number.to_sql(&Type::FLOAT8, buf),
            PgValue::Text(string) => string.to_sql(&Type::TEXT, buf),
            PgValue::Null => Ok(IsNull::Yes),
        }
    }

    pub fn encode(&self, format: &Format, buf: &mut BytesMut) -> PgResult<Option<Bytes>> {
        let len = buf.len();
        let is_null = match format {
            Format::Text => self.encode_text(buf),
            Format::Binary => self.encode_binary(buf),
        }
        .map_err(|e| PgError::EncodingError(e))?;
        if let IsNull::No = is_null {
            Ok(Some(buf.split_off(len).freeze()))
        } else {
            Ok(None)
        }
    }

    fn decode_text(bytes: Option<&Bytes>, ty: Type) -> PgResult<Self> {
        let Some(bytes) = bytes else {
            return Ok(PgValue::Null);
        };

        let s = String::from_utf8(bytes.to_vec()).map_err(DecodingError::from)?;
        Ok(match ty {
            Type::INT8 | Type::INT4 | Type::INT2 => {
                PgValue::Integer(s.parse::<i64>().map_err(DecodingError::from)?)
            }
            Type::FLOAT8 | Type::FLOAT4 => {
                PgValue::Float(s.parse::<f64>().map_err(DecodingError::from)?)
            }
            Type::TEXT => PgValue::Text(s),
            Type::BOOL => PgValue::Boolean(decode_text_as_bool(&s.to_lowercase())?),
            _ => {
                return Err(PgError::FeatureNotSupported(format!(
                    "unsupported type {ty}"
                )))
            }
        })
    }

    fn decode_binary(bytes: Option<&Bytes>, ty: Type) -> PgResult<Self> {
        fn do_decode_binary<'a, T: FromSql<'a>>(ty: &Type, raw: &'a [u8]) -> PgResult<T> {
            T::from_sql(ty, raw).map_err(|e| PgError::DecodingError(DecodingError::Other(e)))
        }

        let Some(bytes) = bytes else {
            return Ok(PgValue::Null);
        };

        Ok(match ty {
            Type::INT8 => PgValue::Integer(do_decode_binary::<i64>(&ty, bytes)?),
            Type::INT4 => PgValue::Integer(do_decode_binary::<i32>(&ty, bytes)?.into()),
            Type::INT2 => PgValue::Integer(do_decode_binary::<i16>(&ty, bytes)?.into()),
            Type::FLOAT8 => PgValue::Float(do_decode_binary::<f64>(&ty, bytes)?),
            Type::FLOAT4 => PgValue::Float(do_decode_binary::<f32>(&ty, bytes)?.into()),
            Type::TEXT => PgValue::Text(do_decode_binary(&ty, bytes)?),
            Type::BOOL => PgValue::Boolean(do_decode_binary(&ty, bytes)?),
            _ => {
                return Err(PgError::FeatureNotSupported(format!(
                    "unsupported type {ty}"
                )))
            }
        })
    }

    pub fn decode(bytes: Option<&Bytes>, oid: Oid, format: Format) -> PgResult<Self> {
        let ty =
            Type::from_oid(oid).ok_or(PgError::ProtocolViolation(format!("unknown oid: {oid}")))?;
        match format {
            Format::Binary => Self::decode_binary(bytes, ty),
            Format::Text => Self::decode_text(bytes, ty),
        }
    }
}

impl<L: AsLua> PushInto<L> for PgValue {
    type Err = tarantool::tlua::Void;

    fn push_into_lua(self, lua: L) -> Result<tarantool::tlua::PushGuard<L>, (Self::Err, L)> {
        match self {
            PgValue::Boolean(value) => value.push_into_lua(lua),
            PgValue::Text(value) => value.push_into_lua(lua),
            PgValue::Integer(value) => value.push_into_lua(lua),
            PgValue::Float(value) => value.push_into_lua(lua),
            PgValue::Null => PushInto::push_into_lua(Nil, lua),
        }
    }
}
