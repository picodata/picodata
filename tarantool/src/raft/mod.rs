#![cfg(feature = "raft_node")]

use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::net::ToSocketAddrs;
use std::time::Duration;

use rand::random;

use inner::{NodeAction, NodeInner, NodeState};
use net::{get_local_addrs, ConnectionPool};

use crate::error::{Error, TarantoolErrorCode};
use crate::fiber::Cond;
use crate::net_box::{Conn, ConnOptions, Options};
use crate::raft::inner::NodeEvent;
use crate::tuple::{FunctionArgs, FunctionCtx, Tuple};

mod fsm;
pub mod inner;
pub mod net;
pub mod rpc;
mod storage;

pub struct Node {
    inner: RefCell<NodeInner>,
    connections: RefCell<ConnectionPool>,
    rpc_function: String,
    events_cond: Cond,
    events_buffer: RefCell<VecDeque<NodeEvent>>,
    actions_buffer: RefCell<VecDeque<NodeAction>>,
    ready_cond: Cond,
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
            inner: RefCell::new(NodeInner::new(id, local_addrs, bootstrap_addrs_cfg)),
            connections: RefCell::new(ConnectionPool::new(options.connection_options.clone())),
            rpc_function: rpc_function.to_string(),
            events_cond: Cond::new(),
            events_buffer: RefCell::new(VecDeque::with_capacity(options.recv_queue_size)),
            actions_buffer: RefCell::new(VecDeque::with_capacity(options.send_queue_size)),
            ready_cond: Cond::new(),
            options,
        })
    }

    pub fn run(&self) -> Result<(), Error> {
        loop {
            {
                let mut actions = self.actions_buffer.borrow_mut();
                let mut events = self.events_buffer.borrow_mut();
                self.inner.borrow_mut().update(&mut events, &mut actions);
            }

            for action in self.actions_buffer.borrow_mut().drain(..) {
                match action {
                    NodeAction::Connect(id, addrs) => {
                        self.connections.borrow_mut().connect(id, &addrs[..])?;
                    }
                    NodeAction::Request(to, msg) => {
                        let mut conn_pool = self.connections.borrow_mut();
                        self.send(conn_pool.get(&to).unwrap(), rpc::Request::Bootstrap(msg))?;
                    }
                    NodeAction::Response(_) => {}
                    NodeAction::StateChangeNotification(state) => match state {
                        NodeState::Ready => {
                            self.ready_cond.signal();
                        }
                        NodeState::Done => return Ok(()),
                        _ => {}
                    },
                    _ => {}
                };
            }

            self.events_cond.wait();
        }
    }

    pub fn handle_rpc(&self, ctx: FunctionCtx, args: FunctionArgs) -> i32 {
        let args: Tuple = args.into();

        match args.into_struct::<rpc::Request>() {
            Err(e) => set_error!(TarantoolErrorCode::Protocol, "{}", e),
            Ok(request) => {
                match request {
                    rpc::Request::Bootstrap(msg) => {
                        self.events_buffer
                            .borrow_mut()
                            .push_back(NodeEvent::Request(msg));
                        self.events_cond.signal();
                    }
                    _ => unimplemented!(),
                };

                ctx.return_mp(&rpc::Response::Ack)
                    .unwrap_or_else(|e| set_error!(TarantoolErrorCode::ProcC, "{}", e))
            }
        }
    }

    pub fn wait_ready(&self, timeout: Duration) -> Result<(), Error> {
        if self.inner.borrow().state() != &NodeState::Ready {
            if !self.ready_cond.wait_timeout(timeout) {
                return Err(Error::IO(io::ErrorKind::TimedOut.into()));
            }
        }
        Ok(())
    }

    pub fn close(&self) {
        self.events_buffer.borrow_mut().push_back(NodeEvent::Stop);
        self.events_cond.signal();
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
