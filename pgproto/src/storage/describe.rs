use super::value::Format;
use crate::{error::PgResult, storage::value};
use pgwire::messages::data::{FieldDescription, RowDescription};
use postgres_types::{Oid, Type};
use serde::Deserialize;
use serde_repr::Deserialize_repr;
use std::iter::zip;

/// Contains a query description.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Describe {
    command_tag: CommandTag,
    query_type: QueryType,
    /// Output columns format.
    metadata: Vec<MetadataColumn>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct StatementDescribe {
    #[serde(flatten)]
    pub describe: Describe,
    pub param_oids: Vec<Oid>,
}

impl StatementDescribe {
    pub fn ncolumns(&self) -> usize {
        self.describe.metadata.len()
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct PortalDescribe {
    #[serde(flatten)]
    describe: Describe,
    output_format: Vec<Format>,
}

impl PortalDescribe {
    pub fn row_description(&self) -> PgResult<Option<RowDescription>> {
        match self.query_type() {
            QueryType::Acl | QueryType::Ddl | QueryType::Dml => Ok(None),
            QueryType::Dql | QueryType::Explain => {
                let metadata = &self.describe.metadata;
                let output_format = &self.output_format;
                let row_description = zip(metadata, output_format)
                    .map(|(col, format)| {
                        let type_str = col.r#type.as_str();
                        value::type_from_name(type_str)
                            .map(|ty| field_description(col.name.clone(), ty, *format))
                    })
                    .collect::<PgResult<_>>()?;
                Ok(Some(RowDescription::new(row_description)))
            }
        }
    }

    pub fn query_type(&self) -> &QueryType {
        self.describe.query_type()
    }

    pub fn command_tag(&self) -> &CommandTag {
        self.describe.command_tag()
    }

    pub fn output_format(&self) -> &[Format] {
        &self.output_format
    }

    // Enforce use of the text format for output rows. We use it for simple query, as it supports only the text format.
    pub fn set_text_output_format(&mut self) {
        let mut output_format = Vec::new();
        output_format.resize(self.ncolumns(), Format::Text);
        self.output_format = output_format;
    }

    pub fn ncolumns(&self) -> usize {
        self.describe.metadata.len()
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
pub struct MetadataColumn {
    name: String,
    r#type: String,
}

// WARNING: keep it sync with picodata.
#[derive(Debug, Clone, Default, PartialEq, Deserialize_repr)]
#[repr(u8)]
pub enum QueryType {
    Acl = 0,
    Ddl = 1,
    Dml = 2,
    #[default]
    Dql = 3,
    Explain = 4,
}

// WARNING: keep it sync with picodata.
#[derive(Clone, Debug, Default, Deserialize_repr)]
#[repr(u8)]
pub enum CommandTag {
    AlterRole = 0,
    CreateRole = 1,
    CreateTable = 2,
    DropRole = 3,
    DropTable = 4,
    Delete = 5,
    Explain = 6,
    Grant = 7,
    GrantRole = 8,
    Insert = 9,
    Revoke = 10,
    RevokeRole = 11,
    #[default]
    Select = 12,
    Update = 13,
}

impl CommandTag {
    pub fn as_str(&self) -> &str {
        match *self {
            Self::AlterRole => "ALTER ROLE",
            Self::CreateRole => "CREATE ROLE",
            Self::CreateTable => "CREATE TABLE",
            Self::DropRole => "DROP ROLE",
            Self::DropTable => "DROP TABLE",
            Self::Delete => "DELETE",
            Self::Explain => "EXPLAIN",
            Self::Grant => "GRANT",
            Self::GrantRole => "GRANT ROLE",
            // ** from postgres sources **
            // In PostgreSQL versions 11 and earlier, it was possible to create a
            // table WITH OIDS.  When inserting into such a table, INSERT used to
            // include the Oid of the inserted record in the completion tag.  To
            // maintain compatibility in the wire protocol, we now write a "0" (for
            // InvalidOid) in the location where we once wrote the new record's Oid.
            Self::Insert => "INSERT 0",
            Self::Revoke => "REVOKE",
            Self::RevokeRole => "REVOKE ROLE",
            Self::Select => "SELECT",
            Self::Update => "UPDATE",
        }
    }
}

fn field_description(name: String, ty: Type, format: Format) -> FieldDescription {
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

    let id = ty.oid();
    // TODO: add Type::len()
    let len = 0;

    FieldDescription::new(
        name,
        resorigtbl,
        resorigcol,
        id,
        len,
        typemod,
        format as i16,
    )
}

impl Describe {
    pub fn query_type(&self) -> &QueryType {
        &self.query_type
    }

    pub fn command_tag(&self) -> &CommandTag {
        &self.command_tag
    }

    pub fn row_description(&self) -> PgResult<Option<RowDescription>> {
        match self.query_type() {
            QueryType::Acl | QueryType::Ddl | QueryType::Dml => Ok(None),
            QueryType::Dql | QueryType::Explain => {
                let row_description = self
                    .metadata
                    .iter()
                    .map(|col| {
                        let type_str = col.r#type.as_str();
                        value::type_from_name(type_str)
                            .map(|ty| field_description(col.name.clone(), ty, Format::Text))
                    })
                    .collect::<PgResult<_>>()?;
                Ok(Some(RowDescription::new(row_description)))
            }
        }
    }
}
