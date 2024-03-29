use self::describe::{PortalDescribe, StatementDescribe};
use self::storage::{with_portals_mut, Portal, Statement, PG_PORTALS, PG_STATEMENTS};
use super::client::ClientId;
use super::error::{PgError, PgResult};
use crate::pgproto::storage::value::Format;
use crate::schema::ADMIN_ID;
use crate::sql::otm::TracerKind;
use crate::sql::router::RouterRuntime;
use crate::sql::with_tracer;
use crate::traft::error::Error;
use opentelemetry::sdk::trace::Tracer;
use opentelemetry::Context;
use postgres_types::Oid;
use sbroad::executor::engine::helpers::normalize_name_for_space_api;
use sbroad::executor::engine::{QueryCache, Router, TableVersionMap};
use sbroad::executor::lru::Cache;
use sbroad::frontend::Ast;
use sbroad::ir::value::Value;
use sbroad::ir::Plan as IrPlan;
use sbroad::otm::{query_id, query_span, OTM_CHAR_LIMIT};
use sbroad::utils::MutexLike;
use smol_str::ToSmolStr;
use std::rc::Rc;
use tarantool::session::with_su;
use tarantool::tuple::Tuple;

mod pgproc;
mod storage;

pub mod describe;

// helper function to get `TracerRef`
fn get_tracer_param(traceable: bool) -> &'static Tracer {
    let kind = TracerKind::from_traceable(traceable);
    kind.get_tracer()
}

pub fn bind(
    client_id: ClientId,
    stmt_name: String,
    portal_name: String,
    params: Vec<Value>,
    output_format: Vec<u8>,
    traceable: bool,
) -> PgResult<()> {
    let key = (client_id, stmt_name.into());
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
            let format = output_format
                .into_iter()
                .map(|raw| Format::try_from(raw as i16).unwrap())
                .collect();
            Portal::new(plan, statement.clone(), format)
        },
    )?;

    PG_PORTALS.with(|storage| {
        storage
            .borrow_mut()
            .put((client_id, portal_name.into()), portal)
    })?;
    Ok(())
}

pub fn execute(id: ClientId, name: String, max_rows: i64, traceable: bool) -> PgResult<Tuple> {
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

pub fn parse(
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

pub fn describe_stmt(id: ClientId, name: String) -> PgResult<StatementDescribe> {
    let key = (id, name.into());
    let Some(statement) = PG_STATEMENTS.with(|storage| storage.borrow().get(&key)) else {
        return Err(PgError::Other(
            format!("Couldn't find statement \'{}\'.", key.1).into(),
        ));
    };
    Ok(statement.describe().clone())
}

pub fn describe_portal(id: ClientId, name: String) -> PgResult<PortalDescribe> {
    with_portals_mut((id, name.into()), |portal| Ok(portal.describe().clone()))
}

pub fn close_stmt(id: ClientId, name: String) {
    // Close can't cause an error in PG.
    PG_STATEMENTS.with(|storage| storage.borrow_mut().remove(&(id, name.into())));
}

pub fn close_portal(id: ClientId, name: String) {
    // Close can't cause an error in PG.
    PG_PORTALS.with(|storage| storage.borrow_mut().remove(&(id, name.into())));
}

pub fn close_client_stmts(id: ClientId) {
    PG_STATEMENTS.with(|storage| storage.borrow_mut().remove_by_client_id(id))
}

pub fn close_client_portals(id: ClientId) {
    PG_PORTALS.with(|storage| storage.borrow_mut().remove_by_client_id(id))
}
