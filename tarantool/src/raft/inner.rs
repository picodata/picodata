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
    bootstrap_addrs: Vec<Vec<SocketAddr>>,
}

#[derive(Debug, Copy, Clone)]
enum State {
    Init,
    ColdBootstrap,
    WarmBootstrap,
    Offline,
    Done,
}

#[derive(Debug)]
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

        NodeInner {
            state: State::Init,
            local_id,
            peers,
            responded_ids: Default::default(),
            bootstrap_addrs,
        }
    }

    pub fn update(&mut self, events: &mut VecDeque<NodeEvent>, actions: &mut VecDeque<NodeAction>) {
        if let State::Init = self.state {
            self.init(actions);
        }

        while let Some(event) = events.pop_front() {
            self.handle_event(event, actions);
        }
    }

    fn init(&mut self, actions_buf: &mut VecDeque<NodeAction>) {
        for (id, seed_addrs) in self.bootstrap_addrs.clone().into_iter().enumerate() {
            let id = ConnectionId::Seed(id);
            actions_buf.push_back(NodeAction::Connect(id.clone(), seed_addrs));
            self.send_bootstrap_request(id, actions_buf);
        }

        self.state = State::ColdBootstrap;
    }

    fn handle_event(&mut self, event: NodeEvent, actions_buf: &mut VecDeque<NodeAction>) {
        use NodeEvent as E;
        use State as S;

        let new_state = match (self.state, event) {
            (S::ColdBootstrap, E::Request(req))
            | (S::ColdBootstrap, E::Response(req))
            | (S::Offline, E::Request(req)) => {
                self.handle_msg(req, actions_buf);
                Some(S::WarmBootstrap)
            }
            (S::WarmBootstrap, E::Request(req)) | (S::WarmBootstrap, E::Response(req)) => {
                self.handle_msg(req, actions_buf);

                let num_peers = self.peers.len();
                let num_responded = self.responded_ids.len();
                if num_peers == (num_responded + 1) {
                    actions_buf.push_back(NodeAction::Completed);
                    Some(S::Done)
                } else {
                    None
                }
            }
            (S::ColdBootstrap, E::Timeout) => Some(S::Offline),
            (S::Offline, E::Timeout) => None,
            _ => panic!("invalid state"),
        };

        if let Some(new_state) = new_state {
            self.state = new_state;
        }
    }

    fn handle_msg(&mut self, req: rpc::BootstrapMsg, actions_buf: &mut VecDeque<NodeAction>) {
        if req.from_id == self.local_id {
            return;
        }

        if !self.responded_ids.contains(&req.from_id) {
            let new_nodes = self.merge_nodes_list(&req.nodes);
            for (id, addrs) in new_nodes {
                let id = ConnectionId::Peer(id);
                actions_buf.push_back(NodeAction::Connect(id.clone(), addrs));
                self.send_bootstrap_request(id, actions_buf);
            }
            self.responded_ids.insert(req.from_id);
        }
    }

    #[inline]
    fn send_bootstrap_request(&mut self, to: ConnectionId, actions_buf: &mut VecDeque<NodeAction>) {
        let nodes = self
            .peers
            .iter()
            .map(|(id, addrs)| (*id, addrs.clone()))
            .collect();

        actions_buf.push_back(NodeAction::Request(
            to,
            rpc::BootstrapMsg {
                from_id: self.local_id,
                nodes,
            },
        ));
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
