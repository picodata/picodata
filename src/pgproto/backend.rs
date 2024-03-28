use self::storage::{
    with_portals_mut, Portal, PortalDescribe, Statement, StatementDescribe, UserPortalNames,
    UserStatementNames, PG_PORTALS, PG_STATEMENTS,
};
use super::client::ClientId;
use super::error::{PgError, PgResult};
use crate::schema::ADMIN_ID;
use crate::sql::otm::TracerKind;
use crate::sql::router::RouterRuntime;
use crate::sql::with_tracer;
use crate::traft::error::Error;
use ::tarantool::proc;
use opentelemetry::sdk::trace::Tracer;
use opentelemetry::Context;
use postgres_types::Oid;
use sbroad::executor::engine::helpers::normalize_name_for_space_api;
use sbroad::executor::engine::{QueryCache, Router, TableVersionMap};
use sbroad::executor::lru::Cache;
use sbroad::frontend::Ast;
use sbroad::ir::value::{LuaValue, Value};
use sbroad::ir::Plan as IrPlan;
use sbroad::otm::{query_id, query_span, OTM_CHAR_LIMIT};
use sbroad::utils::MutexLike;
use serde::Deserialize;
use smol_str::ToSmolStr;
use std::rc::Rc;
use tarantool::session::with_su;
use tarantool::tuple::Tuple;

mod storage;

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

// helper function to get `TracerRef`
fn get_tracer_param(traceable: bool) -> &'static Tracer {
    let kind = TracerKind::from_traceable(traceable);
    kind.get_tracer()
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
    let key = (id, stmt_name.into());
    let Some(statement) = PG_STATEMENTS.with(|storage| storage.borrow().get(&key)) else {
        return Err(PgError::Other(
            format!("Couldn't find statement \'{}\'.", key.1).into(),
        ));
    };
    let mut plan = statement.plan().clone();
    let ctx = with_tracer(Context::new(), TracerKind::from_traceable(traceable));
    let portal = query_span::<PgResult<_>, _>(
        "\"api.router.bind\"",
        statement.id(),
        get_tracer_param(traceable),
        &ctx,
        statement.query_pattern(),
        || {
            if !plan.is_ddl()? && !plan.is_acl()? {
                plan.bind_params(params)?;
                plan.apply_options()?;
                plan.optimize()?;
            }
            Portal::new(plan, statement.clone(), output_format)
        },
    )?;

    PG_PORTALS.with(|storage| storage.borrow_mut().put((id, portal_name.into()), portal))?;
    Ok(())
}

#[proc]
pub fn proc_pg_statements(id: ClientId) -> UserStatementNames {
    UserStatementNames::new(id)
}

#[proc]
pub fn proc_pg_portals(id: ClientId) -> UserPortalNames {
    UserPortalNames::new(id)
}

#[proc]
pub fn proc_pg_close_stmt(id: ClientId, name: String) {
    // Close can't cause an error in PG.
    PG_STATEMENTS.with(|storage| storage.borrow_mut().remove(&(id, name.into())));
}

#[proc]
pub fn proc_pg_close_portal(id: ClientId, name: String) {
    // Close can't cause an error in PG.
    PG_PORTALS.with(|storage| storage.borrow_mut().remove(&(id, name.into())));
}

#[proc]
pub fn proc_pg_close_client_stmts(id: ClientId) {
    PG_STATEMENTS.with(|storage| storage.borrow_mut().remove_by_client_id(id))
}

#[proc]
pub fn proc_pg_close_client_portals(id: ClientId) {
    PG_PORTALS.with(|storage| storage.borrow_mut().remove_by_client_id(id))
}

#[proc]
pub fn proc_pg_describe_stmt(id: ClientId, name: String) -> PgResult<StatementDescribe> {
    let key = (id, name.into());
    let Some(statement) = PG_STATEMENTS.with(|storage| storage.borrow().get(&key)) else {
        return Err(PgError::Other(
            format!("Couldn't find statement \'{}\'.", key.1).into(),
        ));
    };
    Ok(statement.describe().clone())
}

#[proc]
pub fn proc_pg_describe_portal(id: ClientId, name: String) -> PgResult<PortalDescribe> {
    with_portals_mut((id, name.into()), |portal| Ok(portal.describe().clone()))
}

#[proc]
pub fn proc_pg_execute(
    id: ClientId,
    name: String,
    max_rows: i64,
    traceable: bool,
) -> PgResult<Tuple> {
    let max_rows = if max_rows <= 0 { i64::MAX } else { max_rows };
    let name = Rc::from(name);

    let statement = with_portals_mut((id, Rc::clone(&name)), |portal| {
        // We are cloning Rc here.
        Ok(portal.statement().clone())
    })?;
    with_portals_mut((id, name), |portal| {
        let ctx = with_tracer(Context::new(), TracerKind::from_traceable(traceable));
        query_span::<PgResult<Tuple>, _>(
            "\"api.router.execute\"",
            statement.id(),
            get_tracer_param(traceable),
            &ctx,
            statement.query_pattern(),
            || portal.execute(max_rows as usize),
        )
    })
}

#[proc]
pub fn proc_pg_parse(
    cid: ClientId,
    name: String,
    query: String,
    param_oids: Vec<Oid>,
    traceable: bool,
) -> PgResult<()> {
    let id = query_id(&query);
    // Keep the query patterns for opentelemetry spans short enough.
    let sql = query
        .char_indices()
        .filter_map(|(i, c)| if i <= OTM_CHAR_LIMIT { Some(c) } else { None })
        .collect::<String>();
    let ctx = with_tracer(Context::new(), TracerKind::from_traceable(traceable));
    query_span::<PgResult<()>, _>(
        "\"api.router.parse\"",
        &id.clone(),
        get_tracer_param(traceable),
        &ctx,
        &sql.clone(),
        || {
            let runtime = RouterRuntime::new().map_err(Error::from)?;
            let mut cache = runtime.cache().lock();
            let cache_entry = with_su(ADMIN_ID, || cache.get(&query.to_smolstr()))??;
            if let Some(plan) = cache_entry {
                let statement =
                    Statement::new(id.to_string(), sql.clone(), plan.clone(), param_oids)?;
                PG_STATEMENTS
                    .with(|cache| cache.borrow_mut().put((cid, name.into()), statement))?;
                return Ok(());
            }
            let metadata = &*runtime.metadata().lock();
            let plan = with_su(ADMIN_ID, || -> PgResult<IrPlan> {
                let mut plan =
                    <RouterRuntime as Router>::ParseTree::transform_into_plan(&query, metadata)?;
                if runtime.provides_versions() {
                    let mut table_version_map =
                        TableVersionMap::with_capacity(plan.relations.tables.len());
                    for table in plan.relations.tables.keys() {
                        let normalized = normalize_name_for_space_api(table);
                        let version = runtime.get_table_version(normalized.as_str())?;
                        table_version_map.insert(normalized, version);
                    }
                    plan.version_map = table_version_map;
                }
                Ok(plan)
            })
            .map_err(|e| PgError::Other(e.into()))??;
            if !plan.is_ddl()? && !plan.is_acl()? {
                cache.put(query.into(), plan.clone())?;
            }
            let statement = Statement::new(id.to_string(), sql, plan, param_oids)?;
            PG_STATEMENTS
                .with(|storage| storage.borrow_mut().put((cid, name.into()), statement))?;
            Ok(())
        },
    )
}
