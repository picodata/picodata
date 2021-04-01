use std::cell::{Cell, RefCell};

use bootstrap::Bootstrap;
use cluster_node::ClusterNode;
pub use rpc::Rpc;

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
    pub fn new(rpc_function: &str, options: NodeOptions) -> Self {
        Node {
            is_active: Cell::new(true),
            state: RefCell::new(NodeState::Init),
        }
    }

    pub fn run(&self, bootstrap_addrs: Vec<&str>) {
        loop {
            match *self.state.borrow() {
                NodeState::Init => {}
                NodeState::Bootstrapping(_) => {}
                NodeState::ClusterNode(_) => {}
                NodeState::Closed => break,
            }
        }
    }

    pub fn call_rpc(&self, ctx: FunctionCtx, args: FunctionArgs) -> i32 {
        0
    }
}
