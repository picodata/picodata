use super::{
    describe::{PortalDescribe, StatementDescribe},
    result::ExecuteResult,
    storage::{UserPortalNames, UserStatementNames},
};
use crate::pgproto::{
    backend,
    client::ClientId,
    error::{EncodingError, PgResult},
    value::FieldFormat,
};
use ::tarantool::proc;
use postgres_types::Oid;
use sbroad::ir::value::{LuaValue, Value};
use serde::{Deserialize, Serialize};
use tarantool::tuple::{Encode, Tuple};

struct BindArgs {
    id: ClientId,
    stmt_name: String,
    portal_name: String,
    params: Vec<Value>,
    encoding_format: Vec<FieldFormat>,
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
            Vec<i16>,
            Option<bool>,
        );

        let EncodedBindArgs(id, stmt_name, portal_name, params, encoding_format, traceable) =
            EncodedBindArgs::deserialize(deserializer)?;

        let params = params
            .unwrap_or_default()
            .into_iter()
            .map(Value::from)
            .collect();

        let format = encoding_format.into_iter().map(FieldFormat::from).collect();

        Ok(Self {
            id,
            stmt_name,
            portal_name,
            params,
            encoding_format: format,
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
    backend::describe_statement(id, &name)
}

#[proc]
pub fn proc_pg_describe_portal(id: ClientId, name: String) -> PgResult<PortalDescribe> {
    backend::describe_portal(id, &name)
}

#[proc]
pub fn proc_pg_execute(
    id: ClientId,
    name: String,
    max_rows: i64,
    traceable: bool,
) -> PgResult<Tuple> {
    let result = backend::execute(id, name, max_rows, traceable)?;
    let bytes = match &result {
        ExecuteResult::AclOrDdl { .. } | ExecuteResult::Dml { .. } => {
            let row_count = if let ExecuteResult::Dml { row_count, .. } = result {
                Some(row_count)
            } else {
                None
            };

            #[derive(Serialize)]
            struct ProcResult {
                row_count: Option<usize>,
            }
            impl Encode for ProcResult {}

            let result = ProcResult { row_count };
            rmp_serde::to_vec_named(&vec![result])
        }
        ExecuteResult::FinishedDql { rows, .. } | ExecuteResult::SuspendedDql { rows } => {
            #[derive(Serialize)]
            struct ProcResult {
                rows: Vec<Vec<LuaValue>>,
                is_finished: bool,
            }
            impl Encode for ProcResult {}

            let is_finished = matches!(result, ExecuteResult::FinishedDql { .. });
            let rows = rows
                .values()
                .into_iter()
                // Note: It's OK to unwrap here as this is testing code.
                .map(|values| values.into_iter().map(|v| v.try_into().unwrap()).collect())
                .collect();
            let result = ProcResult { rows, is_finished };
            rmp_serde::to_vec_named(&vec![result])
        }
    };

    let bytes = bytes.map_err(EncodingError::new)?;
    let tuple = Tuple::try_from_slice(&bytes)?;
    Ok(tuple)
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
    backend::close_statement(id, &name)
}

#[proc]
pub fn proc_pg_close_portal(id: ClientId, name: String) {
    backend::close_portal(id, &name)
}

#[proc]
pub fn proc_pg_close_client_stmts(id: ClientId) {
    backend::close_client_statements(id)
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
