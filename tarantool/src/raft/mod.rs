#![cfg(feature = "raft_node")]

use std::cell::{Cell, RefCell};
use std::io;
use std::net::ToSocketAddrs;
use std::time::{Duration, Instant};

use protobuf::Message as _;
use raft::prelude::Message;
use rand::random;

use bootstrap::{BoostrapController, BootstrapAction};
use net::{get_local_addrs, ConnectionPoll};

use crate::error::{Error, TarantoolErrorCode};
use crate::fiber::{Cond, Latch};
use crate::net_box::{Conn, ConnOptions, Options};
use crate::tuple::{FunctionArgs, FunctionCtx, Tuple};

use self::inner::NodeInner;

pub mod bootstrap;
mod fsm;
mod inner;
mod net;
pub mod rpc;
mod storage;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum NodeState {
    Bootstrapping,
    Active,
    Closed,
}

pub struct Node {
    state: Cell<NodeState>,
    state_lock: Latch,
    state_cond: Cond,
    bootstrap_ctrl: BoostrapController,
    inner: RefCell<NodeInner>,
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
            state: Cell::new(NodeState::Bootstrapping),
            state_lock: Latch::new(),
            state_cond: Cond::new(),
            bootstrap_ctrl: BoostrapController::new(id, local_addrs, bootstrap_addrs_cfg),
            inner: RefCell::new(NodeInner::new(id, &options)?),
            connections: Default::default(),
            rpc_function: rpc_function.to_string(),
            options,
        })
    }

    pub fn run(&self) -> Result<(), Error> {
        loop {
            let mut is_state_changed = false;
            {
                let _lock = self.state_lock.lock();
                let next_state = match self.state.get() {
                    NodeState::Bootstrapping => {
                        for action in self.bootstrap_ctrl.pending_actions() {
                            match action {
                                BootstrapAction::Request(msg, to) => {
                                    let mut conn_pool = self.connections.borrow_mut();
                                    self.send(
                                        conn_pool.connect_or_get(None, &to)?,
                                        rpc::Request::Bootstrap(msg),
                                    )?;
                                }
                                BootstrapAction::Response(_) => {}
                                BootstrapAction::Completed => {}
                            };
                        }

                        Some(NodeState::Closed)
                    }
                    NodeState::Active => {
                        unimplemented!()
                    }
                    NodeState::Closed => break,
                };

                if let Some(next_state) = next_state {
                    self.state.replace(next_state);
                    is_state_changed = true;
                }
            }

            if is_state_changed {
                self.state_cond.broadcast();
            }
        }

        Ok(())
    }

    pub fn handle_rpc(&self, ctx: FunctionCtx, args: FunctionArgs) -> i32 {
        let args: Tuple = args.into();

        match args.into_struct::<rpc::Request>() {
            Err(e) => set_error!(TarantoolErrorCode::Protocol, "{}", e),
            Ok(request) => {
                let response = match request {
                    rpc::Request::Bootstrap(msg) => self.recv_bootstrap_request(msg),
                    rpc::Request::Raft { data: msg_data } => {
                        let mut msg = Message::default();
                        match msg.merge_from_bytes(&msg_data) {
                            Err(e) => {
                                return set_error!(TarantoolErrorCode::Protocol, "{}", e);
                            }
                            Ok(()) => {
                                let _lock = self.state_lock.lock();
                                if let NodeState::Active = self.state.get() {
                                    self.inner.borrow().handle_msg(msg);
                                }
                            }
                        }
                        rpc::Response::Ack
                    }
                    _ => unimplemented!(),
                };

                ctx.return_mp(&response)
                    .unwrap_or_else(|e| set_error!(TarantoolErrorCode::ProcC, "{}", e))
            }
        }
    }

    pub fn wait_ready(&self, timeout: Duration) -> Result<(), Error> {
        let started_at = Instant::now();
        while self.state.get() != NodeState::Active {
            let is_timeout = !match timeout.checked_sub(started_at.elapsed()) {
                None => false,
                Some(timeout) => self.state_cond.wait_timeout(timeout),
            };

            if is_timeout {
                return Err(io::Error::from(io::ErrorKind::TimedOut).into());
            }
        }

        Ok(())
    }

    pub fn close(&self) {
        let _lock = self.state_lock.lock();
        self.state.replace(NodeState::Closed);
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

    fn recv_bootstrap_request(&self, request: rpc::BootstrapMsg) -> rpc::Response {
        unimplemented!()
    }

    fn send_raft_batch(&self, msgs: &mut dyn Iterator<Item = Message>) -> Result<(), Error> {
        unimplemented!()
    }
}
