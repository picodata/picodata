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
    fmt::Debug,
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
        _ => Err(DecodingError::new(format!("cannot decode \'{s}\' as bool")))?,
    }
}

fn deserialize_rmpv_ext<T: DeserializeOwned>(value: &rmpv::Value) -> Result<T, EncodingError> {
    // TODO: Find a way to avoid this redundant encoding.
    let buf = rmp_serde::encode::to_vec(&value).map_err(EncodingError::new)?;
    let val = rmp_serde::from_slice(&buf).map_err(EncodingError::new)?;
    Ok(val)
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

/// Json wrapper for smooth encoding.
#[derive(Debug, Clone)]
pub struct Json(rmpv::Value);

impl std::fmt::Display for Json {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let string = serde_json::to_string(&self.0).map_err(|_| {
            crate::tlog!(Warning, "failed to print `{self:?}` as json");
            std::fmt::Error
        })?;
        f.write_str(&string)
    }
}

impl FromStr for Json {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map(Self)
    }
}

impl<'a> FromSql<'a> for Json {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> SqlResult<Self> {
        let json = postgres_types::Json::<rmpv::Value>::from_sql(ty, raw)?;
        Ok(Self(json.0))
    }

    postgres_types::accepts!(JSON, JSONB);
}

impl ToSqlText for Json {
    fn to_sql_text(&self, _ty: &Type, out: &mut BytesMut) -> SqlResult<IsNull> {
        // Note: json text representation is the same as binary
        self.to_sql(&Type::JSON, out)
    }
}

impl ToSql for Json {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> SqlResult<IsNull> {
        postgres_types::Json(&self.0).to_sql(ty, out)
    }

    postgres_types::accepts!(JSON, JSONB);
    postgres_types::to_sql_checked!();
}

#[derive(Debug, Clone)]
pub enum PgValue {
    Float(f64),
    Integer(i64),
    Boolean(bool),
    Text(String),
    Json(Json),
    Uuid(Uuid),
    Numeric(Decimal),
    Null,
}

impl TryFrom<rmpv::Value> for PgValue {
    type Error = PgError;

    fn try_from(value: rmpv::Value) -> Result<Self, Self::Error> {
        match value {
            rmpv::Value::Nil => Ok(PgValue::Null),
            rmpv::Value::F32(v) => Ok(PgValue::Float(v as _)),
            rmpv::Value::F64(v) => Ok(PgValue::Float(v)),
            rmpv::Value::Boolean(v) => Ok(PgValue::Boolean(v)),
            rmpv::Value::Integer(v) => Ok(PgValue::Integer({
                if let Some(v) = v.as_i64() {
                    v
                } else if let Some(v) = v.as_u64() {
                    // NOTE: u64::MAX can't be converted into i64
                    i64::try_from(v).map_err(EncodingError::new)?
                } else {
                    Err(EncodingError::new(format!("cannot encode integer: {v}")))?
                }
            })),
            rmpv::Value::Map(_) | rmpv::Value::Array(_) => {
                // Any map-like structure will be encoded as json.
                Ok(PgValue::Json(Json(value)))
            }
            rmpv::Value::String(v) => {
                let Some(s) = v.as_str() else {
                    Err(EncodingError::new(format!("cannot encode string: {v:?}")))?
                };
                Ok(PgValue::Text(s.to_owned()))
            }
            rmpv::Value::Ext(1, _) => {
                let decimal = deserialize_rmpv_ext(&value)?;
                Ok(PgValue::Numeric(decimal))
            }
            rmpv::Value::Ext(2, _) => {
                let uuid = deserialize_rmpv_ext(&value)?;
                Ok(PgValue::Uuid(uuid))
            }

            value => Err(PgError::FeatureNotSupported(format!("value: {value:?}"))),
        }
    }
}

impl TryFrom<PgValue> for SbroadValue {
    type Error = PgError;

    fn try_from(value: PgValue) -> Result<Self, Self::Error> {
        match value {
            PgValue::Float(v) => Ok(SbroadValue::from(v)),
            PgValue::Boolean(v) => Ok(SbroadValue::from(v)),
            PgValue::Integer(v) => Ok(SbroadValue::from(v)),
            PgValue::Text(v) => Ok(SbroadValue::from(v)),
            PgValue::Numeric(v) => Ok(SbroadValue::from(v.0)),
            PgValue::Uuid(v) => Ok(SbroadValue::from(v.0)),
            PgValue::Null => Ok(SbroadValue::Null),
            PgValue::Json(v) => {
                // Anyhow, currently Sbroad cannot work with these types.
                Err(PgError::FeatureNotSupported(format!(
                    "cannot encode json: {v}",
                )))
            }
        }
    }
}

impl TryFrom<PgValue> for LuaValue {
    type Error = PgError;

    fn try_from(value: PgValue) -> Result<Self, Self::Error> {
        SbroadValue::try_from(value).map(Into::into)
    }
}

/// These implementations should be kept in sync with types in
/// [`crate::pgproto::backend::describe::Describe`].
/// Further reading: function pg_type_from_sbroad.
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
            PgValue::Float(v) => do_encode(encoder, v, Type::FLOAT8, format),
            PgValue::Integer(v) => do_encode(encoder, v, Type::INT8, format),
            PgValue::Boolean(v) => do_encode(encoder, v, Type::BOOL, format),
            PgValue::Text(v) => do_encode(encoder, v, Type::TEXT, format),
            PgValue::Json(v) => do_encode(encoder, v, Type::JSON, format),
            PgValue::Uuid(v) => do_encode(encoder, v, Type::UUID, format),
            PgValue::Numeric(v) => do_encode(encoder, v, Type::NUMERIC, format),
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
            Type::JSON | Type::JSONB => PgValue::Json(do_parse(&s)?),
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
            Type::JSON | Type::JSONB => PgValue::Json(do_decode(ty, bytes)?),
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
