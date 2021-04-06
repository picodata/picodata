use std::cell::{Cell, RefCell};
use std::ffi::CString;
use std::net::SocketAddr;
use std::time::Duration;

use rand::random;

use crate::error::Error;
use crate::ffi::lua::{
    luaT_state, lua_getfield, lua_getglobal, lua_gettop, lua_settop, lua_tointeger,
};
use crate::net_box::Conn;
use crate::raft::rpc::Response;

use super::rpc::{BootstrapRequest, Request};

pub struct Bootstrap {
    id: u64,
    addr: Cell<Option<SocketAddr>>,
    node_addrs: RefCell<Vec<NodeListItem>>,
}

struct NodeListItem {
    id: u64,
    addr: SocketAddr,
    conn: Option<Conn>,
}

impl Bootstrap {
    pub fn new() -> Self {
        Bootstrap {
            id: random::<u64>(),
            addr: Cell::new(None),
            node_addrs: RefCell::new(vec![]),
        }
    }

    pub fn cold_bootstrap(&self, connections: &Vec<Conn>) -> Result<(), Error> {
        for conn in connections {
            if self.addr.get().is_none() {
                if let Ok(true) = conn.wait_connected(Some(Duration::from_secs(1))) {
                    if let Some(ref stream) = *conn.inner().stream() {
                        let addr = unsafe {
                            // get ip addr
                            let mut addr = stream.self_addr()?;

                            // get port
                            let l = luaT_state();
                            let top_idx = lua_gettop(l);
                            lua_getglobal(l, CString::new("box").unwrap().as_ptr());
                            lua_getfield(l, -1, CString::new("cfg").unwrap().as_ptr());
                            lua_getfield(l, -1, CString::new("listen").unwrap().as_ptr());
                            let port = lua_tointeger(l, -1) as u16;
                            lua_settop(l, top_idx);
                            assert!(port > 0);
                            addr.set_port(port);
                            addr
                        };
                        self.addr.set(Some(addr));
                        self.node_addrs.borrow_mut().push(NodeListItem {
                            id: self.id,
                            addr,
                            conn: None,
                        })
                    }
                }
            }
        }

        Ok(())
    }

    pub fn broadcast_announce(&self, connections: &Vec<Conn>) -> Result<(), Error> {
        for conn in connections {
            let result = conn.call(
                "libcluster_node.rpc",
                &Request::Bootstrap(BootstrapRequest {
                    nodes: vec![(99, "127.0.0.1:3301".to_string())],
                }),
                &Default::default(),
            );
            match result {
                Err(Error::IO(_)) => continue,
                Err(e) => return Err(e),
                Ok(r) => {
                    let _ = r.unwrap().into_struct::<((Response,),)>()?;
                }
            }
        }

        Ok(())
    }
}
