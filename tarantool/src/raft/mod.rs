use std::cell::{Cell, RefCell};

use bootstrap::Bootstrap;
use cluster_node::ClusterNode;
pub use rpc::ConnectionPool;

use crate::error::Error;
use crate::net_box::Conn;
use crate::tuple::{FunctionArgs, FunctionCtx};

mod bootstrap;
mod cluster_node;
mod fsm;
mod protocol;
mod rpc;
mod storage;

pub enum NodeState {
    Init,
    Bootstrapping(Bootstrap),
    ClusterNode(ClusterNode),
    Closed,
}

pub struct Node {
    is_active: Cell<bool>,
    state: RefCell<NodeState>,
}

#[derive(Default)]
pub struct NodeOptions {}

impl Node {
    pub fn new(rpc_function: &str, options: NodeOptions) -> Result<Self, Error> {
        Ok(Node {
            is_active: Cell::new(true),
            state: RefCell::new(NodeState::Init),
        })
    }

    pub fn run(&self, bootstrap_addrs: &Vec<&str>) -> Result<(), Error> {
        loop {
            match *self.state.borrow() {
                NodeState::Init => {
                    let mut connections = vec![];
                    for addr in bootstrap_addrs.into_iter() {
                        connections.push(Conn::new(addr, Default::default())?)
                    }
                    let bootstrap_state = Bootstrap::new();
                    bootstrap_state.broadcast_announce(&connections)?;

                    break;
                }
                NodeState::Bootstrapping(_) => {}
                NodeState::ClusterNode(_) => {}
                NodeState::Closed => break,
            }
        }

        Ok(())
    }

    pub fn call_rpc(&self, ctx: FunctionCtx, args: FunctionArgs) -> i32 {
        ctx.return_mp(&rpc::Response::Bootstrap(rpc::BootstrapResponse {
            nodes: vec![],
        }))
        .unwrap()
    }
}
