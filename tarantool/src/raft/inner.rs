use std::collections::{BTreeMap, HashSet, VecDeque};
use std::net::SocketAddr;

use crate::error::Error;
use crate::raft::net::ConnectionId;
use crate::raft::rpc;

pub struct NodeInner {
    state: State,
    local_id: u64,
    peers: BTreeMap<u64, Vec<SocketAddr>>,
    responded_ids: HashSet<u64>,
    pending_actions_buffer: VecDeque<NodeAction>,
}

#[derive(Debug, Copy, Clone)]
enum State {
    Cold,
    Warm,
    Offline,
    Done,
}

pub enum NodeEvent {
    Request(rpc::BootstrapMsg),
    Response(rpc::BootstrapMsg),
    Timeout,
}

#[derive(Debug)]
pub enum NodeAction {
    Connect(ConnectionId, Vec<SocketAddr>),
    UpgradeSeed(ConnectionId, u64),
    Request(ConnectionId, rpc::BootstrapMsg),
    Response(Result<rpc::Response, Error>),
    Completed,
}

impl NodeInner {
    pub fn new(
        local_id: u64,
        local_addrs: Vec<SocketAddr>,
        bootstrap_addrs: Vec<Vec<SocketAddr>>,
    ) -> Self {
        let mut peers = BTreeMap::new();
        peers.insert(local_id, local_addrs);

        let mut node_inner = NodeInner {
            state: State::Cold,
            local_id,
            peers,
            responded_ids: Default::default(),
            pending_actions_buffer: Default::default(),
        };
        node_inner.poll_seeds(bootstrap_addrs.into_iter());
        node_inner
    }

    pub fn pending_actions(&mut self) -> Vec<NodeAction> {
        self.pending_actions_buffer
            .drain(..)
            .collect::<Vec<NodeAction>>()
    }

    pub fn handle_event(&mut self, event: NodeEvent) {
        use NodeEvent as E;
        use State as S;

        let new_state = match (self.state, event) {
            (S::Cold, E::Request(req))
            | (S::Cold, E::Response(req))
            | (S::Offline, E::Request(req)) => {
                self.handle_msg(req);
                Some(S::Warm)
            }
            (S::Warm, E::Request(req)) | (S::Warm, E::Response(req)) => {
                self.handle_msg(req);

                let num_peers = self.peers.len();
                let num_responded = self.responded_ids.len();
                if num_peers == (num_responded + 1) {
                    self.send(NodeAction::Completed);
                    Some(S::Done)
                } else {
                    None
                }
            }
            (S::Cold, E::Timeout) => Some(S::Offline),
            (S::Offline, E::Timeout) => None,
            _ => panic!("invalid state"),
        };

        if let Some(new_state) = new_state {
            self.state = new_state;
        }
    }

    fn handle_msg(&mut self, req: rpc::BootstrapMsg) {
        if req.from_id == self.local_id {
            return;
        }

        if !self.responded_ids.contains(&req.from_id) {
            let new_nodes = self.merge_nodes_list(&req.nodes);
            for (id, addrs) in new_nodes {
                let id = ConnectionId::Peer(id);
                self.send(NodeAction::Connect(id.clone(), addrs));
                self.send_bootstrap_request(id);
            }
            self.responded_ids.insert(req.from_id);
        }
    }

    #[inline]
    fn poll_seeds(&mut self, addrs: impl Iterator<Item = Vec<SocketAddr>>) {
        for (id, seed_addrs) in addrs.enumerate() {
            let id = ConnectionId::Seed(id);
            self.send(NodeAction::Connect(id.clone(), seed_addrs));
            self.send_bootstrap_request(id);
        }
    }

    #[inline]
    fn send_bootstrap_request(&mut self, to: ConnectionId) {
        let nodes = self
            .peers
            .iter()
            .map(|(id, addrs)| (*id, addrs.clone()))
            .collect();

        self.send(NodeAction::Request(
            to,
            rpc::BootstrapMsg {
                from_id: self.local_id,
                nodes,
            },
        ));
    }

    #[inline]
    fn send(&mut self, action: NodeAction) {
        self.pending_actions_buffer.push_back(action)
    }

    /// Merges `other` nodes list to already known. Returns new nodes count
    fn merge_nodes_list(
        &mut self,
        nodes_from: &Vec<(u64, Vec<SocketAddr>)>,
    ) -> Vec<(u64, Vec<SocketAddr>)> {
        let mut new_nodes = Vec::<(u64, Vec<SocketAddr>)>::with_capacity(nodes_from.len());
        {
            for (id, addrs) in nodes_from.into_iter() {
                if !self.peers.contains_key(id) {
                    self.peers.insert(*id, addrs.clone());
                    new_nodes.push((*id, addrs.clone()));
                }
            }
        }
        new_nodes
    }
}
