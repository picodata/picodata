use bytes::{BufMut, Bytes, BytesMut};
use pgwire::types::ToSqlText;
use postgres_types::Type;
use postgres_types::{FromSql, ToSql};
use postgres_types::{IsNull, Oid};
use sbroad::ir::value::Value;
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::error::Error;
use std::str;

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

#[derive(Debug, Clone, Copy, Default, Serialize_repr, Deserialize_repr)]
#[repr(i16)]
pub enum Format {
    #[default]
    Text = 0,
    Binary = 1,
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
pub struct PgValue(sbroad::ir::value::Value);

impl PgValue {
    pub fn into_inner(self) -> Value {
        self.0
    }

    fn integer(value: i64) -> Self {
        Self(Value::Integer(value))
    }

    fn float(value: f64) -> Self {
        Self(Value::Double(value.into()))
    }

    fn text(value: String) -> Self {
        Self(Value::String(value))
    }

    fn boolean(value: bool) -> Self {
        Self(Value::Boolean(value))
    }

    fn null() -> Self {
        Self(Value::Null)
    }
}

impl TryFrom<rmpv::Value> for PgValue {
    type Error = PgError;

    fn try_from(value: rmpv::Value) -> Result<Self, Self::Error> {
        match value {
            rmpv::Value::Nil => Ok(PgValue::null()),
            rmpv::Value::Boolean(v) => Ok(PgValue::boolean(v)),
            rmpv::Value::F32(v) => Ok(PgValue::float(v.into())),
            rmpv::Value::F64(v) => Ok(PgValue::float(v)),
            rmpv::Value::Integer(v) => {
                let i = if v.is_i64() {
                    v.as_i64().unwrap()
                } else if v.is_u64() {
                    // NOTE: u64::MAX can't be converted into i64
                    i64::try_from(v.as_u64().unwrap())
                        .map_err(|e| PgError::EncodingError(e.into()))?
                } else {
                    Err(PgError::EncodingError(
                        format!("couldn't encode integer: {v:?}").into(),
                    ))?
                };
                Ok(PgValue::integer(i))
            }
            rmpv::Value::String(v) => {
                let Some(s) = v.as_str() else {
                    Err(PgError::EncodingError(
                        format!("couldn't encode string: {v:?}").into(),
                    ))?
                };
                Ok(PgValue::text(s.to_owned()))
            }
            value => Err(PgError::FeatureNotSupported(format!("value: {value:?}"))),
        }
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

// TODO: support decimal type
impl PgValue {
    fn encode_text(&self, buf: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        match &self.0 {
            Value::Boolean(val) => bool_to_sql_text(*val, buf),
            Value::Integer(number) => number.to_sql_text(&Type::INT8, buf),
            Value::Double(double) => double.value.to_sql_text(&Type::FLOAT8, buf),
            Value::String(string) => string.to_sql_text(&Type::TEXT, buf),
            Value::Null => Ok(IsNull::Yes),
            value => Err(format!("unsupported value: {value:?}"))?,
        }
    }

    fn encode_binary(&self, buf: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        match &self.0 {
            Value::Boolean(val) => val.to_sql(&Type::BOOL, buf),
            Value::Integer(number) => number.to_sql(&Type::INT8, buf),
            Value::Double(double) => double.value.to_sql(&Type::FLOAT8, buf),
            Value::String(string) => string.to_sql(&Type::TEXT, buf),
            Value::Null => Ok(IsNull::Yes),
            value => Err(format!("unsupported value: {value:?}"))?,
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
            return Ok(PgValue::null());
        };

        let s = String::from_utf8(bytes.to_vec()).map_err(DecodingError::from)?;
        Ok(match ty {
            Type::INT8 | Type::INT4 | Type::INT2 => {
                PgValue::integer(s.parse::<i64>().map_err(DecodingError::from)?)
            }
            Type::FLOAT8 | Type::FLOAT4 => {
                PgValue::float(s.parse::<f64>().map_err(DecodingError::from)?)
            }
            Type::TEXT => PgValue::text(s),
            Type::BOOL => PgValue::boolean(decode_text_as_bool(&s.to_lowercase())?),
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
            return Ok(PgValue::null());
        };

        Ok(match ty {
            Type::INT8 => PgValue::integer(do_decode_binary::<i64>(&ty, bytes)?),
            Type::INT4 => PgValue::integer(do_decode_binary::<i32>(&ty, bytes)?.into()),
            Type::INT2 => PgValue::integer(do_decode_binary::<i16>(&ty, bytes)?.into()),
            Type::FLOAT8 => PgValue::float(do_decode_binary::<f64>(&ty, bytes)?),
            Type::FLOAT4 => PgValue::float(do_decode_binary::<f32>(&ty, bytes)?.into()),
            Type::TEXT => PgValue::text(do_decode_binary(&ty, bytes)?),
            Type::BOOL => PgValue::boolean(do_decode_binary(&ty, bytes)?),
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
