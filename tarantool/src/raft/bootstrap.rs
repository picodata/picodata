use std::cell::RefCell;
use std::net::SocketAddr;

use rand::random;

use crate::error::Error;
use crate::net_box::Conn;
use crate::raft::rpc::Response;

use super::rpc::{BootstrapRequest, Request};

pub struct Bootstrap {
    id: u64,
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
            node_addrs: RefCell::new(vec![]),
        }
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
