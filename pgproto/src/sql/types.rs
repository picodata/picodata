use bytes::Bytes;
use once_cell::sync::Lazy;
use pgwire::messages::data::{DataRow, FieldDescription, RowDescription};
use serde_json::Value;
use std::collections::HashMap;
use std::iter::zip;

use crate::error::{PgError, PgResult};

pub const BOOLEAN: TypeInfo = TypeInfo { id: 16, size: 1 };
pub const INTEGER8: TypeInfo = TypeInfo { id: 20, size: 8 };
pub const TEXT: TypeInfo = TypeInfo { id: 25, size: -1 };
pub const FLOAT8: TypeInfo = TypeInfo { id: 701, size: 8 };
pub const UNKNOWN: TypeInfo = TypeInfo { id: 705, size: -2 };

/// Convert type name into type info.
pub static TYPE_NAME_INTO_TYPE_INFO: Lazy<HashMap<&'static str, TypeInfo>> = Lazy::new(|| {
    let mut m: HashMap<&'static str, TypeInfo> = HashMap::new();
    m.insert("integer", INTEGER8);
    m.insert("string", TEXT);
    m.insert("boolean", BOOLEAN);
    m.insert("double", FLOAT8);
    m.insert("any", UNKNOWN);
    m
});

pub type Id = u32;
pub type Size = i16;

/// Describes a postgres type.
pub struct TypeInfo {
    pub id: Id,
    pub size: Size,
}

impl Default for &TypeInfo {
    fn default() -> Self {
        &UNKNOWN
    }
}

impl TypeInfo {
    pub fn make_field_description(&self, name: String) -> FieldDescription {
        let name = name.to_owned();

        // ** From postgres sources **
        // resorigtbl/resorigcol identify the source of the column, if it is a
        // simple reference to a column of a base table (or view).  If it is not
        // a simple reference, these fields are zeroes.
        let resorigtbl = 0;
        let resorigcol = 0;

        // typmod records type-specific data supplied at table creation time
        // (for example, the max length of a varchar field).  The
        // value will generally be -1 for types that do not need typmod.
        let typemod = -1;

        // encoding format, 0 - text, 1 - binary
        let format = 0;

        FieldDescription::new(
            name, resorigtbl, resorigcol, self.id, self.size, typemod, format,
        )
    }
}

pub enum Format {
    Text,
    Binary,
}

impl From<i16> for Format {
    fn from(value: i16) -> Self {
        match value {
            1 => Format::Binary,
            _ => Format::Text,
        }
    }
}

type JsonEncoder = fn(V: &Value, format: Format) -> PgResult<Bytes>;

/// Get an encoder for the given type id.
static ID_INTO_ENCODER: Lazy<HashMap<Id, JsonEncoder>> = Lazy::new(|| {
    let mut m: HashMap<Id, JsonEncoder> = HashMap::new();
    m.insert(INTEGER8.id, |value, format| {
        let v = value
            .as_i64()
            .ok_or(PgError::EncodingError(format!("expected i64, got {value}")))?;
        match format {
            Format::Text => {
                let s = v.to_string();
                Ok(s.into())
            }
            Format::Binary => unimplemented!(),
        }
    });
    m.insert(TEXT.id, |value, format| {
        let v = value
            .as_str()
            .ok_or(PgError::EncodingError(format!("expected str, got {value}")))?;
        match format {
            Format::Text => Ok(v.to_owned().into()),
            Format::Binary => unimplemented!(),
        }
    });
    m.insert(BOOLEAN.id, |value, format| {
        let v = value.as_bool().ok_or(PgError::EncodingError(format!(
            "expected bool, got {value}"
        )))?;
        match format {
            Format::Text => {
                let s = if v { "t" } else { "s" };
                Ok(s.into())
            }
            Format::Binary => unimplemented!(),
        }
    });
    m.insert(FLOAT8.id, |value, format| {
        let v = value
            .as_f64()
            .ok_or(PgError::EncodingError(format!("expected f64, got {value}")))?;
        match format {
            Format::Text => {
                let s = v.to_string();
                Ok(s.into())
            }
            Format::Binary => unimplemented!(),
        }
    });
    m.insert(UNKNOWN.id, |_, format| {
        let s = "UNKNOWN";
        match format {
            Format::Text => Ok(s.into()),
            Format::Binary => unimplemented!(),
        }
    });
    m
});

pub struct ValueRows {
    rows: Vec<Vec<Value>>,
}

impl ValueRows {
    pub fn new(rows: Vec<Vec<Value>>) -> Self {
        Self { rows }
    }

    /// Encode the rows according to the format.
    pub fn encode(self, format: &RowDescription) -> PgResult<Vec<DataRow>> {
        let encode_row = |row, format: &RowDescription| {
            let data_row = zip(row, format.fields())
                .map(|(value, desc)| {
                    let id = desc.type_id();
                    let format = *desc.format_code();
                    let encoder = ID_INTO_ENCODER
                        .get(id)
                        .ok_or(PgError::EncodingError(format!("unknown type id {id}")))?;
                    let bytes = encoder(&value, format.into())?;
                    Ok(Some(bytes))
                })
                .collect::<PgResult<_>>()?;
            PgResult::Ok(DataRow::new(data_row))
        };

        self.rows
            .into_iter()
            .map(|row| -> PgResult<_> { encode_row(row, format) })
            .collect::<PgResult<_>>()
    }
}
