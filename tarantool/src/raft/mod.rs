use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::ffi::CString;
use std::net::SocketAddr;
use std::time::Duration;

use rand::random;

use rpc::ConnectionPool;

use crate::error::Error;
use crate::net_box::Conn;
use crate::tuple::{FunctionArgs, FunctionCtx, Tuple};

mod cluster_node;
mod fsm;
mod protocol;
mod rpc;
mod storage;

pub enum NodeState {
    Init,
    // Bootstrapping,
    // ClusterNode,
    Closed,
}

pub struct Node {
    id: u64,
    addr: Cell<Option<SocketAddr>>,
    state: RefCell<NodeState>,
    nodes: RefCell<BTreeMap<u64, SocketAddr>>,
    connections: RefCell<ConnectionPool>,
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
            connections: RefCell::new(ConnectionPool::default()),
            rpc_function: rpc_function.to_string(),
            options,
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
                    self.cold_bootstrap(connections)?;
                }
                // NodeState::Bootstrapping => {}
                // NodeState::ClusterNode => {}
                NodeState::Closed => break,
            }
        }

        Ok(())
    }

    pub fn handle_rpc(&self, ctx: FunctionCtx, args: FunctionArgs) -> i32 {
        let args: Tuple = args.into();
        let request = args.into_struct::<rpc::Request>().unwrap();

        ctx.return_mp(&rpc::Response::Bootstrap(rpc::BootstrapMsg {
            from: self.id,
            nodes: self.nodes.borrow().clone(),
        }))
        .unwrap()
    }

    pub fn cold_bootstrap(&self, connections: Vec<Conn>) -> Result<(), Error> {
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

            let _ = self.send_bootstrap_request(
                &conn,
                rpc::BootstrapMsg {
                    from: self.id,
                    nodes: self.nodes.borrow().clone(),
                },
            )?;
        }

        Ok(())
    }

    pub fn send_bootstrap_request(
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
}
