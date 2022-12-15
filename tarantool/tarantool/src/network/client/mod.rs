//! Tarantool based client.
//! Can be used only from inside tarantool.

mod recv_queue;
mod stream;

use std::cell::RefCell;
use std::net::ToSocketAddrs;
use std::rc::Rc;
use std::time::Duration;

use super::protocol::api::{Call, Eval, Execute, Ping, Request};
use super::protocol::codec;
use super::protocol::conn::Conn;
use super::protocol::options::{ConnOptions, Options};
use crate::error::Error;
use crate::tuple::{Decode, ToTupleBuffer, Tuple};

struct ClientInner {
    conn: Conn,
    sender: (),
    receiver: (),
}

impl ClientInner {
    pub fn new(addr: impl ToSocketAddrs) -> Result<Self, Error> {
        Ok(Self {
            conn: Conn::with_options(Default::default()),
            sender: todo!(),
            receiver: todo!(),
        })
    }

    pub fn connect(&mut self) -> Result<(), Error> {
        todo!()
    }
}

/// Can be cloned and moved into different fibers for connection to be reused
#[derive(Clone)]
pub struct Client(Rc<RefCell<ClientInner>>);

impl Client {
    pub fn new(addr: impl ToSocketAddrs) -> Result<Self, Error> {
        let mut inner = ClientInner::new(addr)?;
        inner.connect()?;
        Ok(Self(Rc::new(RefCell::new(inner))))
    }

    #[allow(clippy::diverging_sub_expression)]
    async fn send<R: Request>(&self, request: R) -> Result<R::Response, Error> {
        let sync = self.0.borrow_mut().conn.send_request(&request)?;
        let response = todo!("receiver.get(sync).await");
        request.decode_body(response)
    }

    /// Execute a PING command.
    pub async fn ping(&self) -> Result<(), Error> {
        self.send(Ping).await
    }

    /// Call a remote stored procedure.
    ///
    /// `conn.call("func", &("1", "2", "3"))` is the remote-call equivalent of `func('1', '2', '3')`.
    /// That is, `conn.call` is a remote stored-procedure call.
    /// The return from `conn.call` is whatever the function returns.
    pub async fn call<T: ToTupleBuffer>(
        &self,
        fn_name: &str,
        args: T,
    ) -> Result<Option<Tuple>, Error> {
        self.send(Call { fn_name, args }).await
    }

    /// Evaluates and executes the expression in Lua-string, which may be any statement or series of statements.
    ///
    /// An execute privilege is required; if the user does not have it, an administrator may grant it with
    /// `box.schema.user.grant(username, 'execute', 'universe')`.
    ///
    /// To ensure that the return from `eval` is whatever the Lua expression returns, begin the Lua-string with the
    /// word `return`.
    pub async fn eval<T: ToTupleBuffer>(
        &self,
        expr: &str,
        args: T,
    ) -> Result<Option<Tuple>, Error> {
        self.send(Eval { args, expr }).await
    }

    /// Remote execute of sql query.
    pub async fn execute<T: ToTupleBuffer>(
        &self,
        sql: &str,
        bind_params: T,
        limit: Option<usize>,
    ) -> Result<Vec<Tuple>, Error> {
        self.send(Execute {
            sql,
            bind_params,
            limit,
        })
        .await
    }
}
