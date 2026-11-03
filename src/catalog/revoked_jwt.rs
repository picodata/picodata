use crate::schema::{IndexDef, IndexOption, INITIAL_SCHEMA_VERSION};
use crate::storage::{index_by_ids_unchecked, space_by_id_unchecked, SystemTable};
use tarantool::index::{FieldType as IndexFieldType, Index, IndexType, IteratorType, Part};
use tarantool::space::{Field, FieldType, Space, SpaceId, SpaceType};

/// Refresh tokens revoked cluster-wide, retained through their expiration leeway.
#[derive(Debug, Clone)]
pub struct RevokedJwt {
    pub space: Space,
    index_retain_until: Index,
}

impl Default for RevokedJwt {
    fn default() -> Self {
        Self::new()
    }
}

impl SystemTable for RevokedJwt {
    const TABLE_NAME: &'static str = "_pico_revoked_jwt";
    const TABLE_ID: SpaceId = 535;

    fn format() -> Vec<Field> {
        vec![
            Field::from(("jwt", FieldType::String)).is_nullable(false),
            Field::from(("retain_until", FieldType::Integer)).is_nullable(false),
        ]
    }

    fn index_definitions() -> Vec<IndexDef> {
        vec![
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 0,
                name: "_pico_revoked_jwt_pkey".into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(true)],
                parts: vec![Part::from(("jwt", IndexFieldType::String)).is_nullable(false)],
                operable: true,
                schema_version: INITIAL_SCHEMA_VERSION,
            },
            IndexDef {
                table_id: Self::TABLE_ID,
                id: 1,
                name: Self::RETAIN_UNTIL_INDEX_NAME.into(),
                ty: IndexType::Tree,
                opts: vec![IndexOption::Unique(false)],
                parts: vec![
                    Part::from(("retain_until", IndexFieldType::Integer)).is_nullable(false)
                ],
                operable: true,
                schema_version: INITIAL_SCHEMA_VERSION,
            },
        ]
    }
}

impl RevokedJwt {
    pub const RETAIN_UNTIL_INDEX_NAME: &'static str = "_pico_revoked_jwt_retain_until";

    pub const SQL_CREATE: &'static str = "
        CREATE TABLE _pico_revoked_jwt (
            jwt TEXT NOT NULL,
            retain_until INTEGER NOT NULL,
            PRIMARY KEY (jwt)
        ) DISTRIBUTED GLOBALLY
    ";
    pub const SQL_CREATE_INDEX: &'static str = "
        CREATE INDEX _pico_revoked_jwt_retain_until ON _pico_revoked_jwt (retain_until)
    ";

    pub const fn new() -> Self {
        Self {
            space: space_by_id_unchecked(Self::TABLE_ID),
            index_retain_until: index_by_ids_unchecked(Self::TABLE_ID, 1),
        }
    }

    pub fn create(&self) -> tarantool::Result<()> {
        Self::create_space(Self::TABLE_NAME, Some(Self::TABLE_ID), SpaceType::DataLocal)?;
        Ok(())
    }

    pub fn contains(&self, jwt: &str) -> tarantool::Result<bool> {
        Ok(self.space.get(&(jwt,))?.is_some())
    }

    /// Bound each cleanup batch so a backlog cannot monopolize the tx thread.
    pub fn expired(&self, current_ts: i64, limit: usize) -> tarantool::Result<Vec<String>> {
        self.index_retain_until
            .select_v2(IteratorType::LT, &(current_ts,))?
            .take(limit)
            .map(|tuple| {
                let (jwt, _): (String, i64) = tuple?.decode()?;
                Ok(jwt)
            })
            .collect()
    }
}
