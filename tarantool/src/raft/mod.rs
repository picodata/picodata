use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ffi::CString;
use std::net::SocketAddr;
use std::time::Duration;

use rand::random;

use crate::error::Error;
use crate::fiber::sleep;
use crate::net_box::Conn;
use crate::tuple::{FunctionArgs, FunctionCtx, Tuple};

use self::cluster_node::ClusterNodeState;

mod cluster_node;
mod fsm;
mod protocol;
mod rpc;
mod storage;

pub enum NodeState {
    Init,
    Bootstrapping,
    ClusterNode(ClusterNodeState),
    Closed,
}

pub struct Node {
    id: u64,
    addr: Cell<Option<SocketAddr>>,
    state: RefCell<NodeState>,
    nodes: RefCell<BTreeMap<u64, SocketAddr>>,
    connections: RefCell<HashMap<u64, Conn>>,
    rpc_function: String,
    options: NodeOptions,
}

#[derive(Default)]
pub struct NodeOptions {}

impl Node {
    pub fn new(rpc_function: &str, options: NodeOptions) -> Result<Self, Error> {
        Ok(Node {
            id: random::<u64>(),
            addr: Cell::new(None),
            state: RefCell::new(NodeState::Init),
            nodes: RefCell::new(BTreeMap::new()),
            connections: RefCell::new(HashMap::new()),
            rpc_function: rpc_function.to_string(),
            options,
        })
    }

    pub fn run(&self, bootstrap_addrs: &Vec<&str>) -> Result<(), Error> {
        let mut send_queue = VecDeque::new();
        loop {
            let next_state = match *self.state.borrow() {
                NodeState::Init => {
                    let mut connections = vec![];
                    for addr in bootstrap_addrs.into_iter() {
                        connections.push(Conn::new(addr, Default::default())?)
                    }

                    let is_completed = self.cold_bootstrap(connections)?;
                    if is_completed {
                        Some(NodeState::Bootstrapping)
                    } else {
                        sleep(1.0);
                        None
                    }
                }
                NodeState::Bootstrapping => {
                    let new_nodes_count = self.warm_bootstrap()?;
                    if let Some(0) = new_nodes_count {
                        Some(NodeState::ClusterNode(ClusterNodeState::new(self.id)?))
                    } else {
                        sleep(1.0);
                        None
                    }
                }
                NodeState::ClusterNode(ref state) => {
                    sleep(0.5);
                    state.tick(&mut send_queue);
                    None
                }
                NodeState::Closed => break,
            };

            if let Some(next_state) = next_state {
                self.state.replace(next_state);
            }
        }

        Ok(())
    }

    pub fn handle_rpc(&self, ctx: FunctionCtx, args: FunctionArgs) -> i32 {
        let args: Tuple = args.into();
        let request = args.into_struct::<rpc::Request>().unwrap();

        match request {
            rpc::Request::Bootstrap(msg) => self.recv_bootstrap_request(ctx, msg),
            _ => unimplemented!(),
        }
    }

    pub fn close(&self) {
        self.state.replace(NodeState::Closed);
    }

    fn cold_bootstrap(&self, connections: Vec<Conn>) -> Result<bool, Error> {
        let mut is_completed = false;
        for conn in connections {
            // detect self addr/port if so far unknown
            if self.addr.get().is_none() {
                if let Ok(true) = conn.wait_connected(Some(Duration::from_secs(1))) {
                    if let Some(Ok(mut addr)) = conn.self_addr() {
                        unsafe {
                            use crate::ffi::lua::{
                                luaT_state, lua_getfield, lua_getglobal, lua_gettop, lua_settop,
                                lua_tointeger,
                            };

                            let l = luaT_state();
                            let top_idx = lua_gettop(l);

                            let s = CString::new("box").unwrap();
                            lua_getglobal(l, s.as_ptr());

                            let s = CString::new("cfg").unwrap();
                            lua_getfield(l, -1, s.as_ptr());

                            let s = CString::new("listen").unwrap();
                            lua_getfield(l, -1, s.as_ptr());

                            let port = lua_tointeger(l, -1) as u16;
                            assert!(port > 0);
                            lua_settop(l, top_idx);
                            addr.set_port(port);
                        };

                        self.addr.set(Some(addr));
                        self.nodes.borrow_mut().insert(self.id, addr);
                    }
                } else {
                    continue;
                }
            }

            let nodes = self.nodes.borrow().clone();
            let response = self.send_bootstrap_request(
                &conn,
                rpc::BootstrapMsg {
                    from: self.id,
                    nodes,
                },
            )?;

            if let Some(rpc::Response::Bootstrap(response)) = response {
                is_completed = true;
                self.merge_nodes_list(response.nodes);
            }
        }
        Ok(is_completed)
    }

    fn warm_bootstrap(&self) -> Result<Option<usize>, Error> {
        {
            let mut connections = self.connections.borrow_mut();
            for (id, addr) in self.nodes.borrow().iter() {
                if *id == self.id {
                    continue;
                }

                if !connections.contains_key(id) {
                    connections.insert(*id, Conn::new(*addr, Default::default())?);
                }
            }
        }

        let mut new_nodes_total = None;
        for conn in self.connections.borrow().values() {
            let nodes = self.nodes.borrow().clone();
            let response = self.send_bootstrap_request(
                conn,
                rpc::BootstrapMsg {
                    from: self.id,
                    nodes,
                },
            )?;

            if let Some(rpc::Response::Bootstrap(response)) = response {
                let new_nodes = self.merge_nodes_list(response.nodes);
                let new_nodes_len = new_nodes.len();
                if let Some(new_nodes_count) = new_nodes_total.as_mut() {
                    *new_nodes_count += new_nodes_len;
                } else {
                    new_nodes_total = Some(new_nodes_len);
                }
            }
        }
        Ok(new_nodes_total)
    }

    fn send_bootstrap_request(
        &self,
        conn: &Conn,
        request: rpc::BootstrapMsg,
    ) -> Result<Option<rpc::Response>, Error> {
        let result = conn.call(
            self.rpc_function.as_str(),
            &rpc::Request::Bootstrap(request),
            &Default::default(),
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

    fn recv_bootstrap_request(&self, ctx: FunctionCtx, request: rpc::BootstrapMsg) -> i32 {
        let response = rpc::Response::Bootstrap(rpc::BootstrapMsg {
            from: self.id,
            nodes: self.nodes.borrow().clone(),
        });

        if let NodeState::Init | NodeState::Bootstrapping = *self.state.borrow() {
            let _ = self.merge_nodes_list(request.nodes);
        }

        ctx.return_mp(&response).unwrap()
    }

    /// Merges `other` nodes list to already known. Returns new nodes count
    fn merge_nodes_list(&self, other: BTreeMap<u64, SocketAddr>) -> Vec<(u64, SocketAddr)> {
        let mut new_nodes = Vec::<(u64, SocketAddr)>::with_capacity(other.len());
        {
            let self_nodes = self.nodes.borrow();
            // a - already known nodes
            // b - received from peer nodes list
            let mut a_iter = self_nodes.iter();
            let mut b_iter = other.into_iter();
            let mut a = a_iter.next();
            let mut b = b_iter.next();

            while let (Some((a_id, _)), Some((b_id, b_addr))) = (a, b) {
                let a_id = *a_id;
                if b_id < a_id {
                    new_nodes.push((b_id, b_addr));
                    b = b_iter.next();
                } else if b_id > a_id {
                    a = a_iter.next();
                } else {
                    a = a_iter.next();
                    b = b_iter.next();
                }
            }

            while let Some(node) = b {
                new_nodes.push(node);
                b = b_iter.next();
            }
        }

        let mut known_nodes = self.nodes.borrow_mut();
        for (id, addr) in new_nodes.iter() {
            known_nodes.insert(*id, *addr);
        }
        new_nodes
    }
}
