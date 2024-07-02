use crate::pgproto::error::{DecodingError, EncodingError, PgError, PgResult};
use bytes::{Bytes, BytesMut};
use pgwire::{
    api::results::{DataRowEncoder, FieldFormat},
    error::PgWireResult,
    types::ToSqlText,
};
use postgres_types::{FromSql, IsNull, Oid, ToSql, Type};
use sbroad::ir::value::{LuaValue, Value as SbroadValue};
use serde::de::DeserializeOwned;
use serde_repr::{Deserialize_repr, Serialize_repr};
use smol_str::{StrExt, ToSmolStr};
use std::{
    error::Error,
    str::{self, FromStr},
};

/// This type is used to send Format over the wire.
pub type RawFormat = i16;

#[derive(Debug, Clone, Copy, Default, Serialize_repr, Deserialize_repr)]
#[repr(i16)]
pub enum Format {
    #[default]
    Text = 0,
    Binary = 1,
}

impl From<&Format> for FieldFormat {
    fn from(value: &Format) -> Self {
        Self::from(*value as i16)
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

fn bool_from_str(s: &str) -> PgResult<bool> {
    let s = s.to_lowercase_smolstr();
    // bool has many representations in text format.
    // NOTE: see `parse_bool_with_len` in pg
    match s.as_str() {
        "t" | "true" | "yes" | "on" | "1" => Ok(true),
        "f" | "false" | "no" | "off" | "0" => Ok(false),
        _ => Err(DecodingError::new(format!(
            "couldn't decode \'{s}\' as bool"
        )))?,
    }
}

type SqlError = Box<dyn Error + Sync + Send>;
type SqlResult<T> = Result<T, Box<dyn Error + Sync + Send>>;

/// UUID wrapper for smooth encoding & decoding.
#[derive(Debug, Copy, Clone, serde::Deserialize)]
#[repr(transparent)]
pub struct Uuid(tarantool::uuid::Uuid);

impl FromStr for Uuid {
    type Err = <tarantool::uuid::Uuid as FromStr>::Err;

    #[inline(always)]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        tarantool::uuid::Uuid::from_str(s).map(Self)
    }
}

impl<'a> FromSql<'a> for Uuid {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> SqlResult<Self> {
        let uuid = uuid::Uuid::from_sql(ty, raw)?;
        Ok(Uuid(uuid.into()))
    }

    postgres_types::accepts!(UUID);
}

impl ToSqlText for Uuid {
    fn to_sql_text(&self, _ty: &Type, out: &mut BytesMut) -> SqlResult<IsNull> {
        self.0.to_string().to_sql_text(&Type::TEXT, out)
    }
}

impl ToSql for Uuid {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> SqlResult<IsNull> {
        self.0.into_inner().to_sql(ty, out)
    }

    postgres_types::accepts!(UUID);
    postgres_types::to_sql_checked!();
}

/// Decimal wrapper for smooth encoding & decoding.
#[derive(Debug, Copy, Clone, serde::Deserialize)]
#[repr(transparent)]
pub struct Decimal(tarantool::decimal::Decimal);

impl FromStr for Decimal {
    type Err = SqlError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let decimal = tarantool::decimal::Decimal::from_str(s)
            .map_err(|_| format!("failed to parse `{s}` as decimal"))?;

        Ok(Self(decimal))
    }
}

impl<'a> FromSql<'a> for Decimal {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> SqlResult<Self> {
        let decimal = rust_decimal::Decimal::from_sql(ty, raw)?;
        Self::from_str(&decimal.to_smolstr())
    }

    postgres_types::accepts!(NUMERIC);
}

impl ToSqlText for Decimal {
    fn to_sql_text(&self, _ty: &Type, out: &mut BytesMut) -> SqlResult<IsNull> {
        self.0.to_string().to_sql_text(&Type::TEXT, out)
    }
}

impl ToSql for Decimal {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> SqlResult<IsNull> {
        let string = self.0.to_string();
        let decimal = rust_decimal::Decimal::from_str_exact(&string)?;
        decimal.to_sql(ty, out)
    }

    postgres_types::accepts!(NUMERIC);
    postgres_types::to_sql_checked!();
}

#[derive(Debug, Clone)]
pub enum PgValue {
    Integer(i64),
    Float(f64),
    // TODO: Consider using smol_str
    Text(String),
    Boolean(bool),
    Numeric(Decimal),
    Uuid(Uuid),
    TextArray(Vec<String>),
    Null,
}

impl TryFrom<rmpv::Value> for PgValue {
    type Error = PgError;

    fn try_from(value: rmpv::Value) -> Result<Self, Self::Error> {
        fn rmpv_to_string(value: &rmpv::Value) -> String {
            if let Some(s) = value.as_str() {
                // we don't use `to_string` because it wraps string values in quotes
                return s.into();
            }
            value.to_string()
        }

        fn deserialize_rmpv_ext<T: DeserializeOwned>(
            value: &rmpv::Value,
        ) -> Result<T, EncodingError> {
            // TODO: Find a way to avoid this redundant encoding.
            let buf = rmp_serde::encode::to_vec(&value).map_err(EncodingError::new)?;
            let val = rmp_serde::from_slice(&buf).map_err(EncodingError::new)?;
            Ok(val)
        }

        match &value {
            rmpv::Value::Nil => Ok(PgValue::Null),
            rmpv::Value::Boolean(v) => Ok(PgValue::Boolean(*v)),
            rmpv::Value::F32(v) => Ok(PgValue::Float(*v as _)),
            rmpv::Value::F64(v) => Ok(PgValue::Float(*v)),
            rmpv::Value::Integer(v) => Ok(PgValue::Integer({
                if let Some(v) = v.as_i64() {
                    v
                } else if let Some(v) = v.as_u64() {
                    // NOTE: u64::MAX can't be converted into i64
                    i64::try_from(v).map_err(EncodingError::new)?
                } else {
                    Err(EncodingError::new(format!(
                        "couldn't encode integer: {v:?}"
                    )))?
                }
            })),
            rmpv::Value::Array(values) => {
                let string_values = values.iter().map(rmpv_to_string).collect();
                Ok(PgValue::TextArray(string_values))
            }
            rmpv::Value::String(v) => {
                let Some(s) = v.as_str() else {
                    Err(EncodingError::new(format!("couldn't encode string: {v:?}")))?
                };
                Ok(PgValue::Text(s.to_owned()))
            }
            rmpv::Value::Ext(1, _data) => {
                let decimal = deserialize_rmpv_ext(&value)?;
                Ok(PgValue::Numeric(decimal))
            }
            rmpv::Value::Ext(2, _data) => {
                let uuid = deserialize_rmpv_ext(&value)?;
                Ok(PgValue::Uuid(uuid))
            }

            value => Err(PgError::FeatureNotSupported(format!("value: {value:?}"))),
        }
    }
}

impl From<PgValue> for SbroadValue {
    fn from(value: PgValue) -> Self {
        match value {
            PgValue::Integer(number) => SbroadValue::from(number),
            PgValue::Float(float) => SbroadValue::from(float),
            PgValue::Text(string) => SbroadValue::from(string),
            PgValue::Boolean(val) => SbroadValue::from(val),
            PgValue::Numeric(decimal) => SbroadValue::from(decimal.0),
            PgValue::Uuid(uuid) => SbroadValue::from(uuid.0),
            PgValue::TextArray(values) => {
                let values: Vec<_> = values.into_iter().map(SbroadValue::from).collect();
                SbroadValue::from(values)
            }
            PgValue::Null => SbroadValue::Null,
        }
    }
}

impl From<PgValue> for LuaValue {
    fn from(value: PgValue) -> Self {
        SbroadValue::from(value).into()
    }
}

impl PgValue {
    pub fn encode(&self, format: FieldFormat, encoder: &mut DataRowEncoder) -> PgWireResult<()> {
        pub fn do_encode<T: ToSql + ToSqlText>(
            encoder: &mut DataRowEncoder,
            value: &T,
            ty: Type,
            format: FieldFormat,
        ) -> PgWireResult<()> {
            encoder.encode_field_with_type_and_format(value, &ty, format)
        }

        match self {
            PgValue::Integer(v) => do_encode(encoder, v, Type::INT8, format),
            PgValue::Float(v) => do_encode(encoder, v, Type::FLOAT8, format),
            PgValue::Text(v) => do_encode(encoder, v, Type::TEXT, format),
            PgValue::TextArray(v) => do_encode(encoder, v, Type::TEXT_ARRAY, format),
            PgValue::Boolean(v) => do_encode(encoder, v, Type::BOOL, format),
            PgValue::Numeric(v) => do_encode(encoder, v, Type::NUMERIC, format),
            PgValue::Uuid(v) => do_encode(encoder, v, Type::UUID, format),
            PgValue::Null => {
                // XXX: one could call this a clever hack...
                do_encode(encoder, &None::<i64>, Type::INT8, format)
            }
        }
    }

    fn decode_text(bytes: &Bytes, ty: Type) -> PgResult<Self> {
        fn do_parse<T: FromStr>(s: &str) -> PgResult<T>
        where
            T::Err: Into<SqlError>,
        {
            Ok(T::from_str(s).map_err(DecodingError::new)?)
        }

        let s = String::from_utf8(bytes.to_vec()).map_err(DecodingError::new)?;
        Ok(match ty {
            Type::INT8 | Type::INT4 | Type::INT2 => PgValue::Integer(do_parse(&s)?),
            Type::FLOAT8 | Type::FLOAT4 => PgValue::Float(do_parse(&s)?),
            Type::TEXT | Type::VARCHAR => PgValue::Text(s),
            Type::BOOL => PgValue::Boolean(bool_from_str(&s)?),
            Type::NUMERIC => PgValue::Numeric(do_parse(&s)?),
            Type::UUID => PgValue::Uuid(do_parse(&s)?),
            _ => return Err(PgError::FeatureNotSupported(format!("type {ty}"))),
        })
    }

    fn decode_binary(bytes: &Bytes, ty: Type) -> PgResult<Self> {
        fn do_decode<'a, T: FromSql<'a>>(ty: Type, raw: &'a [u8]) -> PgResult<T> {
            Ok(T::from_sql(&ty, raw).map_err(DecodingError::new)?)
        }

        Ok(match ty {
            Type::INT8 => PgValue::Integer(do_decode::<i64>(ty, bytes)?),
            Type::INT4 => PgValue::Integer(do_decode::<i32>(ty, bytes)?.into()),
            Type::INT2 => PgValue::Integer(do_decode::<i16>(ty, bytes)?.into()),
            Type::FLOAT8 => PgValue::Float(do_decode::<f64>(ty, bytes)?),
            Type::FLOAT4 => PgValue::Float(do_decode::<f32>(ty, bytes)?.into()),
            Type::TEXT | Type::VARCHAR => PgValue::Text(do_decode(ty, bytes)?),
            Type::BOOL => PgValue::Boolean(do_decode(ty, bytes)?),
            Type::NUMERIC => PgValue::Numeric(do_decode(ty, bytes)?),
            Type::UUID => PgValue::Uuid(do_decode(ty, bytes)?),
            _ => return Err(PgError::FeatureNotSupported(format!("type {ty}"))),
        })
    }

    pub fn decode(bytes: Option<&Bytes>, oid: Oid, format: Format) -> PgResult<Self> {
        let ty = Type::from_oid(oid)
            .ok_or_else(|| PgError::FeatureNotSupported(format!("unknown oid: {oid}")))?;

        let Some(bytes) = bytes else {
            return Ok(PgValue::Null);
        };

        match format {
            Format::Binary => Self::decode_binary(bytes, ty),
            Format::Text => Self::decode_text(bytes, ty),
        }
    }
}
