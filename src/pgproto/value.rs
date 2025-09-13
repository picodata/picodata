use super::error::DynError;
use crate::pgproto::error::{DecodingError, EncodingError, PgError, PgResult};
use bytes::{BufMut, BytesMut};
use pgwire::{api::results::DataRowEncoder, types::ToSqlText};
use postgres_types::{FromSql, IsNull, Oid, ToSql, Type};
use sbroad::{
    frontend::sql::{try_parse_bool, try_parse_datetime},
    ir::value::Value as SbroadValue,
};
use smol_str::{format_smolstr, ToSmolStr};
use std::{
    fmt::Debug,
    str::{self, FromStr},
};
use time::macros::format_description;

/// This type is used to send Format over the wire.
pub type RawFormat = i16;
pub type FieldFormat = pgwire::api::results::FieldFormat;

/// Bool wrapper for smooth encoding & decoding.
#[derive(Debug, Copy, Clone, serde::Deserialize)]
#[repr(transparent)]
pub struct Bool(bool);

impl FromStr for Bool {
    type Err = Box<DynError>;

    #[inline(always)]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        try_parse_bool(s)
            .map(Self)
            .ok_or_else(|| DecodingError::bad_lit_of_type(s, "bool").into())
    }
}

impl<'a> FromSql<'a> for Bool {
    #[inline(always)]
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<DynError>> {
        bool::from_sql(ty, raw).map(Bool)
    }

    postgres_types::accepts!(BOOL);
}

impl ToSqlText for Bool {
    #[inline(always)]
    fn to_sql_text(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<DynError>> {
        self.0.to_sql_text(&Type::TEXT, out)
    }
}

impl ToSql for Bool {
    #[inline(always)]
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<DynError>> {
        self.0.to_sql(ty, out)
    }

    postgres_types::accepts!(BOOL);
    postgres_types::to_sql_checked!();
}

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
    #[inline(always)]
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<DynError>> {
        let uuid = uuid::Uuid::from_sql(ty, raw)?;
        Ok(Uuid(uuid.into()))
    }

    postgres_types::accepts!(UUID);
}

impl ToSqlText for Uuid {
    #[inline(always)]
    fn to_sql_text(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<DynError>> {
        self.0.to_string().to_sql_text(&Type::TEXT, out)
    }
}

impl ToSql for Uuid {
    #[inline(always)]
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<DynError>> {
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
    type Err = Box<DynError>;

    #[inline(always)]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let decimal = tarantool::decimal::Decimal::from_str(s)
            .map_err(|_| format!("failed to parse `{s}` as decimal"))?;

        Ok(Self(decimal))
    }
}

impl<'a> FromSql<'a> for Decimal {
    #[inline(always)]
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<DynError>> {
        let decimal = rust_decimal::Decimal::from_sql(ty, raw)?;
        Self::from_str(&decimal.to_smolstr())
    }

    postgres_types::accepts!(NUMERIC);
}

impl ToSqlText for Decimal {
    #[inline(always)]
    fn to_sql_text(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<DynError>> {
        self.0.to_string().to_sql_text(&Type::TEXT, out)
    }
}

impl ToSql for Decimal {
    #[inline(always)]
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<DynError>> {
        let string = self.0.to_string();
        let decimal = rust_decimal::Decimal::from_str_exact(&string)?;
        decimal.to_sql(ty, out)
    }

    postgres_types::accepts!(NUMERIC);
    postgres_types::to_sql_checked!();
}

/// JSON wrapper for smooth encoding & decoding.
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

    #[inline(always)]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map(Self)
    }
}

impl<'a> FromSql<'a> for Json {
    #[inline(always)]
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<DynError>> {
        let json = postgres_types::Json::<rmpv::Value>::from_sql(ty, raw)?;
        Ok(Self(json.0))
    }

    postgres_types::accepts!(JSON, JSONB);
}

impl ToSqlText for Json {
    #[inline(always)]
    fn to_sql_text(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<DynError>> {
        // Note: json text representation is the same as binary
        self.to_sql(&Type::JSON, out)
    }
}

impl ToSql for Json {
    #[inline(always)]
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<DynError>> {
        postgres_types::Json(&self.0).to_sql(ty, out)
    }

    postgres_types::accepts!(JSON, JSONB);
    postgres_types::to_sql_checked!();
}

/// Datetime wrapper for smooth encoding & decoding.
#[derive(Debug, Copy, Clone, serde::Deserialize)]
#[repr(transparent)]
pub struct Timestamptz(tarantool::datetime::Datetime);

impl FromStr for Timestamptz {
    type Err = Box<DynError>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        try_parse_datetime(s)
            .map(Self)
            .ok_or_else(|| DecodingError::bad_lit_of_type(s, "datetime").into())
    }
}

impl<'a> FromSql<'a> for Timestamptz {
    #[inline(always)]
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<DynError>> {
        let datetime = time::OffsetDateTime::from_sql(ty, raw)?;
        Ok(Self(datetime.into()))
    }

    postgres_types::accepts!(TIMESTAMPTZ);
}

impl ToSqlText for Timestamptz {
    /// Date formats based on [EncodeDateTime](https://github.com/postgres/postgres/blob/ba8f00eef6d/src/interfaces/ecpg/pgtypeslib/dt_common.c#L767-L798) from PostgreSQL.
    fn to_sql_text(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<DynError>> {
        let datetime = self.0.into_inner();
        let fmt = match (datetime.microsecond(), datetime.offset().minutes_past_hour()) {
            (0, 0) => format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second][offset_hour sign:mandatory]"
            ),
            (_, 0) => format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:6][offset_hour sign:mandatory]"
            ),
            (0, _) => format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second][offset_hour sign:mandatory]:[offset_minute]"
            ),
            _ => format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:6][offset_hour sign:mandatory]:[offset_minute]"
            ),
        };
        out.put_slice(self.0.into_inner().format(&fmt)?.as_bytes());

        Ok(IsNull::No)
    }
}

impl ToSql for Timestamptz {
    #[inline(always)]
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<DynError>> {
        self.0.into_inner().to_sql(ty, out)
    }

    postgres_types::accepts!(TIMESTAMPTZ);
    postgres_types::to_sql_checked!();
}

#[derive(Debug, Clone)]
pub enum PgValue {
    Float(f64),
    Integer(i64),
    Boolean(Bool),
    Text(String),
    Timestamptz(Timestamptz),
    Json(Json),
    Uuid(Uuid),
    Numeric(Decimal),
    Null,
}

impl TryFrom<PgValue> for SbroadValue {
    type Error = PgError;

    fn try_from(value: PgValue) -> Result<Self, Self::Error> {
        match value {
            PgValue::Float(v) => Ok(SbroadValue::from(v)),
            PgValue::Boolean(v) => Ok(SbroadValue::from(v.0)),
            PgValue::Integer(v) => Ok(SbroadValue::from(v)),
            PgValue::Text(v) => Ok(SbroadValue::from(v)),
            PgValue::Numeric(v) => Ok(SbroadValue::from(v.0)),
            PgValue::Uuid(v) => Ok(SbroadValue::from(v.0)),
            PgValue::Timestamptz(v) => Ok(SbroadValue::from(v.0)),
            PgValue::Null => Ok(SbroadValue::Null),
            PgValue::Json(_) => {
                // Anyhow, currently Sbroad cannot work with these types.
                Err(PgError::FeatureNotSupported(format_smolstr!(
                    "cannot represent json in sbroad",
                )))
            }
        }
    }
}

/// These implementations should be kept in sync with types in
/// [`crate::pgproto::backend::describe::Describe`].
/// Further reading: function pg_type_from_sbroad.
impl PgValue {
    pub fn try_from_rmpv(value: rmpv::Value, ty: &Type) -> PgResult<Self> {
        use rmpv::Value;
        match (value, ty.clone()) {
            (Value::Nil, _) => Ok(PgValue::Null),
            (Value::Boolean(v), Type::BOOL) => Ok(PgValue::Boolean(Bool(v))),
            (Value::F32(v), Type::FLOAT8) => Ok(PgValue::Float(v as _)),
            (Value::F64(v), Type::FLOAT8) => Ok(PgValue::Float(v)),
            (Value::Integer(v), Type::INT8) => {
                // Note: PgValue::Integer is sent as INT8, so only INT8 is allowed here. Otherwise,
                // we can send 8 bytes integer while the client is expecting 2 or 4 bytes.
                Ok(PgValue::Integer({
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
                }))
            }
            (Value::Integer(v), Type::FLOAT8) => {
                // Convert NUMBER value represented as integer to float.
                let v = v.as_f64().ok_or_else(|| {
                    EncodingError::new(format!("couldn't encode NUMBER value {v:?} as FLOAT8"))
                })?;
                Ok(PgValue::Float(v))
            }
            (Value::Array(v), Type::JSON | Type::JSONB) => {
                // Any array-like structure will be encoded as json.
                Ok(PgValue::Json(Json(Value::Array(v))))
            }
            (Value::Map(v), Type::JSON | Type::JSONB) => {
                // Any map-like structure will be encoded as json.
                Ok(PgValue::Json(Json(Value::Map(v))))
            }
            (Value::String(v), Type::TEXT | Type::VARCHAR) => {
                if !v.is_str() {
                    return Err(EncodingError::new(format!("couldn't encode string: {v:?}")).into());
                }
                let s = v
                    .into_str()
                    .expect("Value is checked as string above")
                    .to_string();
                Ok(PgValue::Text(s))
            }
            (Value::Ext(1, v), Type::NUMERIC) => {
                let decimal =
                    rmpv::ext::from_value(Value::Ext(1, v)).map_err(EncodingError::new)?;
                Ok(PgValue::Numeric(decimal))
            }
            (Value::Integer(v), Type::NUMERIC) => {
                // Decimal values can be represented as integers in msgpack.
                let v = v.as_i64().ok_or_else(|| {
                    EncodingError::new(format!("couldn't encode DECIMAL value {v:?} as NUMERIC"))
                })?;
                Ok(PgValue::Numeric(Decimal(v.into())))
            }
            (Value::F32(v), Type::NUMERIC) => {
                let decimal =
                    tarantool::decimal::Decimal::try_from(v).map_err(EncodingError::new)?;
                Ok(PgValue::Numeric(Decimal(decimal)))
            }
            (Value::F64(v), Type::NUMERIC) => {
                let decimal =
                    tarantool::decimal::Decimal::try_from(v).map_err(EncodingError::new)?;
                Ok(PgValue::Numeric(Decimal(decimal)))
            }
            (Value::Ext(2, v), Type::UUID) => {
                let uuid = rmpv::ext::from_value(Value::Ext(2, v)).map_err(EncodingError::new)?;
                Ok(PgValue::Uuid(uuid))
            }
            (Value::Ext(4, v), Type::TIMESTAMPTZ) => {
                let datetime =
                    rmpv::ext::from_value(Value::Ext(4, v)).map_err(EncodingError::new)?;
                Ok(PgValue::Timestamptz(datetime))
            }
            (any, Type::JSON | Type::JSONB) => Ok(PgValue::Json(Json(any))),

            (value, ty) => Err(PgError::FeatureNotSupported(format_smolstr!(
                "{value:?} cannot be represented as a value of type {ty:?}"
            ))),
        }
    }

    pub fn encode(
        &self,
        format: FieldFormat,
        encoder: &mut DataRowEncoder,
    ) -> Result<(), EncodingError> {
        // TODO: rewrite this once rust supports generic closures
        pub fn do_encode<T: ToSql + ToSqlText>(
            encoder: &mut DataRowEncoder,
            value: &T,
            ty: Type,
            format: FieldFormat,
        ) -> Result<(), EncodingError> {
            encoder
                .encode_field_with_type_and_format(value, &ty, format)
                .map_err(EncodingError::new)
        }

        match self {
            PgValue::Float(v) => do_encode(encoder, v, Type::FLOAT8, format),
            PgValue::Integer(v) => do_encode(encoder, v, Type::INT8, format),
            PgValue::Boolean(v) => do_encode(encoder, v, Type::BOOL, format),
            PgValue::Text(v) => do_encode(encoder, v, Type::TEXT, format),
            PgValue::Json(v) => do_encode(encoder, v, Type::JSON, format),
            PgValue::Uuid(v) => do_encode(encoder, v, Type::UUID, format),
            PgValue::Numeric(v) => do_encode(encoder, v, Type::NUMERIC, format),
            PgValue::Timestamptz(v) => do_encode(encoder, v, Type::TIMESTAMPTZ, format),
            PgValue::Null => {
                // XXX: one could call this a clever hack...
                do_encode(encoder, &None::<i64>, Type::INT8, format)
            }
        }
    }

    fn decode_text(bytes: &[u8], ty: Type) -> Result<Self, DecodingError> {
        // TODO: rewrite this once rust supports generic closures
        fn do_parse<T: FromStr>(ty: Type, s: &str) -> Result<T, DecodingError>
        where
            T::Err: Into<Box<DynError>>,
        {
            // We deliberately ignore type's own parsing error; std types tend to have
            // not very helpful errors like `invalid digit found in string`.
            T::from_str(s).map_err(|_| DecodingError::bad_lit_of_type(s, ty))
        }

        let s = String::from_utf8(bytes.to_vec()).map_err(DecodingError::bad_utf8)?;
        Ok(match ty {
            Type::INT8 | Type::INT4 | Type::INT2 => PgValue::Integer(do_parse(ty, &s)?),
            Type::FLOAT8 | Type::FLOAT4 => PgValue::Float(do_parse(ty, &s)?),
            Type::TEXT | Type::VARCHAR => PgValue::Text(s),
            Type::BOOL => PgValue::Boolean(do_parse(ty, &s)?),
            Type::NUMERIC => PgValue::Numeric(do_parse(ty, &s)?),
            Type::UUID => PgValue::Uuid(do_parse(ty, &s)?),
            Type::JSON | Type::JSONB => PgValue::Json(do_parse(ty, &s)?),
            Type::TIMESTAMPTZ => PgValue::Timestamptz(do_parse(ty, &s)?),
            _ => return Err(DecodingError::unsupported_type(ty)),
        })
    }

    fn decode_binary(bytes: &[u8], ty: Type) -> Result<Self, DecodingError> {
        fn do_decode<'a, T: FromSql<'a>>(ty: Type, raw: &'a [u8]) -> Result<T, DecodingError> {
            T::from_sql(&ty, raw).map_err(|_| DecodingError::bad_bin_of_type(ty))
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
            Type::TIMESTAMPTZ => PgValue::Timestamptz(do_decode(ty, bytes)?),
            _ => return Err(DecodingError::unsupported_type(ty)),
        })
    }

    pub fn decode(
        bytes: Option<&[u8]>,
        oid: Oid,
        format: FieldFormat,
    ) -> Result<Self, DecodingError> {
        let ty = Type::from_oid(oid).ok_or_else(|| DecodingError::unknown_oid(oid))?;

        let Some(bytes) = bytes else {
            return Ok(PgValue::Null);
        };

        match format {
            FieldFormat::Binary => Self::decode_binary(bytes, ty),
            FieldFormat::Text => Self::decode_text(bytes, ty),
        }
    }
}
