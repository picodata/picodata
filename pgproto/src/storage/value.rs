use bytes::{BufMut, Bytes, BytesMut};
use pgwire::api::Type;
use pgwire::types::ToSqlText;
use postgres_types::IsNull;
use serde_json::Value;
use serde_repr::Deserialize_repr;
use std::str;

use crate::error::{PgError, PgResult};

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

#[derive(Debug, Clone, Copy, Deserialize_repr)]
#[repr(i16)]
pub enum Format {
    Text = 0,
    Binary = 1,
}

impl TryFrom<i16> for Format {
    type Error = PgError;
    fn try_from(value: i16) -> Result<Self, Self::Error> {
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

impl PgValue {
    pub fn encode(&self, buf: &mut BytesMut) -> PgResult<Option<Bytes>> {
        let do_encode = |buf: &mut BytesMut| match &self {
            PgValue::Boolean(val) => {
                buf.put_u8(if *val { b't' } else { b'f' });
                Ok(IsNull::No)
            }
            PgValue::Integer(number) => {
                number.to_sql_text(&Type::INT8, buf)?;
                Ok(IsNull::No)
            }
            PgValue::Float(number) => {
                number.to_sql_text(&Type::FLOAT8, buf)?;
                Ok(IsNull::No)
            }
            PgValue::Text(string) => string.to_sql_text(&Type::TEXT, buf),
            PgValue::Null => Ok(IsNull::Yes),
        };

        let len = buf.len();
        let is_null = do_encode(buf).map_err(|e| PgError::EncodingError(e.to_string()))?;
        if let IsNull::No = is_null {
            Ok(Some(buf.split_off(len).freeze()))
        } else {
            Ok(None)
        }
    }
}
