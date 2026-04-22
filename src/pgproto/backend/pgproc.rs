use super::{
    describe::{PortalDescribe, StatementDescribe},
    result::ExecuteResult,
    storage::{UserPortalNames, UserStatementNames},
};
use crate::pgproto::{
    backend,
    client::{ClientId, ClientParams},
    error::{EncodingError, PgResult},
    value::FieldFormat,
};
use postgres_types::Oid;
use serde::Serialize;
use sql::ir::options::PartialOptions;
use sql::ir::value::Value;
use tarantool::msgpack;
use tarantool::proc;
use tarantool::tuple::{Decode, Encode, Tuple};

struct BindArgs {
    id: ClientId,
    stmt_name: String,
    portal_name: String,
    params: Vec<Value>,
    encoding_format: Vec<FieldFormat>,
}

impl<'de> Decode<'de> for BindArgs {
    fn decode(data: &'de [u8]) -> tarantool::Result<Self> {
        let (id, stmt_name, portal_name, params, encoding_format): (
            ClientId,
            String,
            String,
            Option<Vec<Value>>,
            Vec<i16>,
        ) = msgpack::decode(data)?;

        let params = params.unwrap_or_default();
        // FieldFormat is represented by i16 in the wire
        let encoding_format = encoding_format.into_iter().map(FieldFormat::from).collect();

        Ok(Self {
            id,
            stmt_name,
            portal_name,
            params,
            encoding_format,
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
    } = args;

    let client_params = ClientParams {
        username: "".to_string(),
        options: PartialOptions::default(),
        is_statement_invalidation: false,
        is_query_metadata: false,
        _rest: Default::default(),
    };

    backend::bind(
        id,
        stmt_name,
        portal_name,
        params,
        output_format,
        &client_params,
    )
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
pub fn proc_pg_execute(id: ClientId, name: String, max_rows: i64) -> PgResult<Tuple> {
    let bytes = match backend::execute(id, name, max_rows)? {
        ExecuteResult::AclOrDdl { .. } | ExecuteResult::Tcl { .. } | ExecuteResult::Empty => {
            #[derive(Serialize)]
            struct ProcResult {
                row_count: Option<usize>,
            }
            impl Encode for ProcResult {}

            rmp_serde::to_vec_named(&vec![ProcResult { row_count: None }])
        }
        ExecuteResult::Dml { row_count, .. } => {
            #[derive(Serialize)]
            struct ProcResult {
                row_count: Option<usize>,
            }
            impl Encode for ProcResult {}

            rmp_serde::to_vec_named(&vec![ProcResult {
                row_count: Some(row_count),
            }])
        }
        ExecuteResult::FinishedDql { rows, .. } => Ok(encode_proc_rows(rows.values(), true)),
        ExecuteResult::SuspendedDql { rows } => Ok(encode_proc_rows(rows.values(), false)),
        ExecuteResult::CopyInStart { .. } => {
            return Err(crate::pgproto::error::PgError::other(
                "COPY FROM STDIN is not supported by proc_pg_execute",
            ));
        }
    };

    let bytes = bytes.map_err(EncodingError::new)?;
    let tuple = Tuple::try_from_slice(&bytes)?;
    Ok(tuple)
}

fn encode_proc_rows(rows: Vec<Vec<crate::pgproto::value::PgValue>>, is_finished: bool) -> Vec<u8> {
    #[derive(msgpack::Encode)]
    #[encode(as_map)]
    struct ProcResult {
        rows: Vec<Vec<Value>>,
        is_finished: bool,
    }

    let rows = rows
        .into_iter()
        // Note: It's OK to unwrap here as this is testing code.
        .map(|values| values.into_iter().map(|v| v.try_into().unwrap()).collect())
        .collect();

    msgpack::encode(&vec![ProcResult { rows, is_finished }])
}

#[proc]
pub fn proc_pg_parse(
    id: ClientId,
    name: String,
    query: String,
    param_oids: Vec<Oid>,
) -> PgResult<()> {
    backend::parse(id, name, &query, param_oids)
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
