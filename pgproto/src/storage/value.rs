use bytes::{BufMut, Bytes, BytesMut};
use pgwire::api::Type;
use pgwire::types::ToSqlText;
use postgres_types::IsNull;
use serde_json::Value;
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

#[derive(Debug)]
pub struct PgValue(Value);

impl From<Value> for PgValue {
    fn from(value: Value) -> Self {
        PgValue(value)
    }
}

impl PgValue {
    pub fn encode(&self, buf: &mut BytesMut) -> PgResult<Option<Bytes>> {
        // TODO: add ToSqlText::to_sql_text_checked for type checking.
        // Value::Bool(bool).to_sql_text(&Type::FLOAT8) doesn't result in an error.
        let do_encode = |buf: &mut BytesMut| match &self.0 {
            Value::Bool(val) => {
                buf.put_u8(if *val { b't' } else { b'f' });
                Ok(IsNull::No)
            }
            Value::String(string) => string.to_sql_text(&Type::TEXT, buf),
            Value::Number(number) => {
                if number.is_f64() {
                    number.as_f64().to_sql_text(&Type::FLOAT8, buf)?;
                } else {
                    number.as_i64().to_sql_text(&Type::INT8, buf)?;
                }
                Ok(IsNull::No)
            }
            _ => {
                let value = &self.0;
                Err(format!("can't encode value {value:?}"))?
            }
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
