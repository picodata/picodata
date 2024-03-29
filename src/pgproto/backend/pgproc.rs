use super::describe::{PortalDescribe, StatementDescribe};
use super::storage::{UserPortalNames, UserStatementNames};
use crate::pgproto::backend;
use crate::pgproto::{client::ClientId, error::PgResult};
use ::tarantool::proc;
use postgres_types::Oid;
use sbroad::ir::value::{LuaValue, Value};
use serde::Deserialize;
use tarantool::tuple::Tuple;

struct BindArgs {
    id: ClientId,
    stmt_name: String,
    portal_name: String,
    params: Vec<Value>,
    encoding_format: Vec<u8>,
    traceable: bool,
}

impl<'de> Deserialize<'de> for BindArgs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct EncodedBindArgs(
            ClientId,
            String,
            String,
            Option<Vec<LuaValue>>,
            Vec<u8>,
            Option<bool>,
        );

        let EncodedBindArgs(id, stmt_name, portal_name, params, encoding_format, traceable) =
            EncodedBindArgs::deserialize(deserializer)?;

        let params = params
            .unwrap_or_default()
            .into_iter()
            .map(Value::from)
            .collect::<Vec<Value>>();

        Ok(Self {
            id,
            stmt_name,
            portal_name,
            params,
            encoding_format,
            traceable: traceable.unwrap_or(false),
        })
    }
}

#[proc(packed_args)]
pub fn proc_pg_bind(args: BindArgs) -> PgResult<()> {
    let BindArgs {
        id,
        stmt_name,
        portal_name,
        params,
        encoding_format: output_format,
        traceable,
    } = args;

    backend::bind(id, stmt_name, portal_name, params, output_format, traceable)
}

#[proc]
pub fn proc_pg_describe_stmt(id: ClientId, name: String) -> PgResult<StatementDescribe> {
    backend::describe_stmt(id, name)
}

#[proc]
pub fn proc_pg_describe_portal(id: ClientId, name: String) -> PgResult<PortalDescribe> {
    backend::describe_portal(id, name)
}

#[proc]
pub fn proc_pg_execute(
    id: ClientId,
    name: String,
    max_rows: i64,
    traceable: bool,
) -> PgResult<Tuple> {
    backend::execute(id, name, max_rows, traceable)
}

#[proc]
pub fn proc_pg_parse(
    id: ClientId,
    name: String,
    query: String,
    param_oids: Vec<Oid>,
    traceable: bool,
) -> PgResult<()> {
    backend::parse(id, name, query, param_oids, traceable)
}

#[proc]
pub fn proc_pg_close_stmt(id: ClientId, name: String) {
    backend::close_stmt(id, name)
}

#[proc]
pub fn proc_pg_close_portal(id: ClientId, name: String) {
    backend::close_portal(id, name)
}

#[proc]
pub fn proc_pg_close_client_stmts(id: ClientId) {
    backend::close_client_stmts(id)
}

#[proc]
pub fn proc_pg_close_client_portals(id: ClientId) {
    backend::close_client_portals(id)
}

#[proc]
pub fn proc_pg_statements(id: ClientId) -> UserStatementNames {
    UserStatementNames::new(id)
}

#[proc]
pub fn proc_pg_portals(id: ClientId) -> UserPortalNames {
    UserPortalNames::new(id)
}
