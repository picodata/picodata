#![cfg(feature = "raft_node")]

use std::cell::RefCell;
use std::net::ToSocketAddrs;
use std::time::Duration;

use rand::random;

use bootstrap::{BoostrapController, BootstrapAction};
use net::{get_local_addrs, ConnectionPoll};

use crate::error::Error;
use crate::net_box::{Conn, ConnOptions, Options};
use crate::tuple::{FunctionArgs, FunctionCtx};

pub mod bootstrap;
mod fsm;
mod inner;
pub mod net;
pub mod rpc;
mod storage;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum NodeState {
    Bootstrapping,
    Active,
    Closed,
}

pub struct Node {
    bootstrap_ctrl: BoostrapController,
    connections: RefCell<ConnectionPoll>,
    rpc_function: String,
    options: NodeOptions,
}

pub struct NodeOptions {
    bootstrap_poll_interval: Duration,
    tick_interval: Duration,
    recv_queue_size: usize,
    send_queue_size: usize,
    connection_options: ConnOptions,
    rpc_call_options: Options,
}

impl Default for NodeOptions {
    fn default() -> Self {
        NodeOptions {
            bootstrap_poll_interval: Duration::from_secs(1),
            tick_interval: Duration::from_millis(100),
            recv_queue_size: 127,
            send_queue_size: 127,
            connection_options: Default::default(),
            rpc_call_options: Default::default(),
        }
    }
}

impl Node {
    pub fn new(
        rpc_function: &str,
        bootstrap_addrs: Vec<impl ToSocketAddrs>,
        options: NodeOptions,
    ) -> Result<Self, Error> {
        let id = random::<u64>();
        let local_addrs = get_local_addrs()?;

        let mut bootstrap_addrs_cfg = Vec::with_capacity(bootstrap_addrs.len());
        for addrs in bootstrap_addrs {
            bootstrap_addrs_cfg.push(addrs.to_socket_addrs()?.collect())
        }

        Ok(Node {
            bootstrap_ctrl: BoostrapController::new(id, local_addrs, bootstrap_addrs_cfg),
            connections: RefCell::new(ConnectionPoll::new(options.connection_options.clone())),
            rpc_function: rpc_function.to_string(),
            options,
        })
    }

    pub fn run(&self) -> Result<(), Error> {
        loop {
            for action in self.bootstrap_ctrl.pending_actions() {
                match action {
                    BootstrapAction::Request(to, msg) => {
                        let mut conn_pool = self.connections.borrow_mut();
                        self.send(conn_pool.get(&to).unwrap(), rpc::Request::Bootstrap(msg))?;
                    }
                    BootstrapAction::Response(_) => {}
                    BootstrapAction::Completed => {
                        return Ok(());
                    }
                    _ => {}
                };
            }
        }
    }

    pub fn handle_rpc(&self, ctx: FunctionCtx, args: FunctionArgs) -> i32 {
        unimplemented!();
    }

    pub fn wait_ready(&self, timeout: Duration) -> Result<(), Error> {
        unimplemented!();
    }

    pub fn close(&self) {
        unimplemented!();
    }

    fn send(&self, conn: &Conn, request: rpc::Request) -> Result<Option<rpc::Response>, Error> {
        let result = conn.call(
            self.rpc_function.as_str(),
            &request,
            &self.options.rpc_call_options,
        );

        match result {
            Err(Error::IO(_)) => Ok(None),
            Err(e) => Err(e),
            Ok(response) => match response {
                None => Ok(None),
                Some(response) => {
                    let ((resp,),) = response.into_struct::<((rpc::Response,),)>()?;
                    Ok(Some(resp))
                }
            },
        }
    }
}
