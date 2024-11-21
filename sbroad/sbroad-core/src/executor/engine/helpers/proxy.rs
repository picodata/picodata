use std::cell::OnceCell;
use std::rc::Rc;
use std::time::Duration;

use tarantool::error::Error;
use tarantool::fiber::channel::Channel;
use tarantool::fiber::{self, SendError};
use tarantool::sql::{prepare, unprepare, Statement};

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
                                CacheResponse::Prepared(prepare(query))
                            }
                            CacheRequest::UnPrepare((stmt, _)) => {
                                CacheResponse::UnPrepared(unprepare(stmt))
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
        fiber::wakeup(sql_cache_proxy().id);
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

pub(crate) fn sql_cache_proxy() -> &'static SqlCacheProxy {
    static mut PROXY: OnceCell<SqlCacheProxy> = OnceCell::new();
    unsafe { PROXY.get_or_init(SqlCacheProxy::new) }
}
