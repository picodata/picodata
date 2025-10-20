use crate::backend::sql::space::ADMIN_ID;
use crate::errors::SbroadError;
use crate::executor::engine::helpers::table_name;
use crate::ir::node::NodeId;
use smol_str::SmolStr;
use std::rc::Rc;
use std::time::Duration;
use tarantool::error::Error;
use tarantool::fiber::channel::Channel;
use tarantool::fiber::{self, SendError};
use tarantool::session::with_su;
use tarantool::space::Space;
use tarantool::sql::{prepare as sql_prepare, unprepare as sql_unprepare, Statement};

const SEND_TIMEOUT: Duration = Duration::from_millis(1);

#[derive(Debug)]
pub(crate) enum CacheRequest {
    Prepare((String, fiber::FiberId)),
    UnPrepare((Statement, fiber::FiberId)),
}

impl CacheRequest {
    fn fiber_id(&self) -> fiber::FiberId {
        match self {
            CacheRequest::Prepare((_, id)) => *id,
            CacheRequest::UnPrepare((_, id)) => *id,
        }
    }
}

#[derive(Debug)]
pub(crate) enum CacheResponse {
    Prepared(Result<Statement, Error>),
    UnPrepared(Result<(), Error>),
}

pub(crate) struct SqlCacheProxy {
    pub(crate) id: fiber::FiberId,
    request: Rc<Channel<CacheRequest>>,
    response: Rc<Channel<CacheResponse>>,
}

thread_local! {
    pub(crate) static SQL_CACHE_PROXY: SqlCacheProxy = SqlCacheProxy::new();
}

fn proxy_start(rq: Rc<Channel<CacheRequest>>, rsp: Rc<Channel<CacheResponse>>) -> fiber::FiberId {
    fiber::Builder::new()
        .name("sql_cache")
        .func(move || {
            'main: loop {
                match rq.recv() {
                    Some(req) => {
                        let client_fiber_id = req.fiber_id();
                        let mut result = match req {
                            CacheRequest::Prepare((query, _)) => {
                                CacheResponse::Prepared(sql_prepare(query))
                            }
                            CacheRequest::UnPrepare((stmt, _)) => {
                                CacheResponse::UnPrepared(sql_unprepare(stmt))
                            }
                        };
                        'send: loop {
                            match rsp.send_timeout(result, SEND_TIMEOUT) {
                                Ok(()) => break 'send,
                                Err(SendError::Timeout(rsp)) => {
                                    // Client fiber is still alive, so we can try to send
                                    // the response again.
                                    if fiber::wakeup(client_fiber_id) {
                                        result = rsp;
                                        continue 'send;
                                    }
                                    // Client fiber was cancelled, so there is no need
                                    // to send the response.
                                    break 'send;
                                }
                                Err(SendError::Disconnected(_)) => break 'main,
                            }
                        }
                    }
                    None => break 'main,
                }
            }
            // The channel is closed or sql_cache fiber is cancelled.
            if fiber::is_cancelled() {
                panic!("sql_cache fiber is cancelled");
            }
            panic!("sql_cache request channel is closed");
        })
        .start_non_joinable()
        .expect("Failed to start sql_cache fiber")
}

impl SqlCacheProxy {
    fn new() -> Self {
        let rq_inner = Rc::new(Channel::<CacheRequest>::new(0));
        let rq_outer = Rc::clone(&rq_inner);
        let rsp_inner = Rc::new(Channel::<CacheResponse>::new(0));
        let rsp_outer = Rc::clone(&rsp_inner);
        let id = proxy_start(rq_inner, rsp_inner);
        SqlCacheProxy {
            id,
            request: rq_outer,
            response: rsp_outer,
        }
    }

    fn send(&self, request: CacheRequest) -> Result<(), Error> {
        let id = SQL_CACHE_PROXY.with(|proxy| proxy.id);
        fiber::wakeup(id);
        if self.request.send(request).is_err() {
            if fiber::is_cancelled() {
                return Err(Error::Other("current fiber is cancelled".into()));
            }
            panic!("sql_cache request channel is closed");
        }
        Ok(())
    }

    fn receive(&self) -> Result<CacheResponse, Error> {
        match self.response.recv() {
            Some(resp) => Ok(resp),
            None => {
                if fiber::is_cancelled() {
                    return Err(Error::Other("current fiber is cancelled".into()));
                }
                panic!("sql_cache response channel is closed");
            }
        }
    }

    pub(crate) fn prepare(&self, query: String) -> Result<Statement, Error> {
        let request = CacheRequest::Prepare((query, fiber::id()));
        self.send(request)?;
        match self.receive()? {
            CacheResponse::Prepared(resp) => resp,
            CacheResponse::UnPrepared(_) => unreachable!("Unexpected unprepare response"),
        }
    }

    pub(crate) fn unprepare(&self, stmt: Statement) -> Result<(), Error> {
        let request = CacheRequest::UnPrepare((stmt, fiber::id()));
        self.send(request)?;
        match self.receive()? {
            CacheResponse::Prepared(_) => unreachable!("Unexpected prepare response"),
            CacheResponse::UnPrepared(resp) => resp,
        }
    }
}

pub fn prepare(pattern: String) -> Result<Statement, SbroadError> {
    let stmt = SQL_CACHE_PROXY
        .with(|proxy| proxy.prepare(pattern))
        .map_err(|e| {
            crate::error!(Option::from("prepare"), &format!("{e:?}"));
            SbroadError::from(e)
        })?;
    Ok(stmt)
}

pub fn unprepare(
    plan_id: &SmolStr,
    entry: &mut (Statement, Vec<NodeId>),
) -> Result<(), SbroadError> {
    let (stmt, table_ids) = std::mem::take(entry);

    // Remove the statement from the instance cache.
    SQL_CACHE_PROXY
        .with(|proxy| proxy.unprepare(stmt))
        .map_err(|e| {
            crate::error!(Option::from("unprepare"), &format!("{e:?}"));
            SbroadError::from(e)
        })?;

    // Remove temporary tables from the instance.
    for node_id in table_ids {
        let table = table_name(plan_id, node_id);
        Space::find(table.as_str()).map(|space| with_su(ADMIN_ID, || space.drop()));
    }

    Ok(())
}
