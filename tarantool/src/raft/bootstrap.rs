use std::net::SocketAddr;

use crate::net_box::Conn;

pub struct Bootstrap {
    id: u64,
    node_addrs: Vec<NodeListItem>,
}

struct NodeListItem {
    id: u64,
    addr: SocketAddr,
    conn: Option<Conn>,
}
