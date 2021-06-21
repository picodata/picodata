use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::net::SocketAddr;

use crate::error::Error;

use super::net::ConnectionId;
use super::rpc;

pub struct BoostrapController {
    state: Cell<BootstrapState>,
    local_id: u64,
    peers: RefCell<BTreeMap<u64, Vec<SocketAddr>>>,
    responded_ids: RefCell<HashSet<u64>>,
    pending_actions_buffer: RefCell<VecDeque<BootstrapAction>>,
}

#[derive(Debug, Copy, Clone)]
enum BootstrapState {
    Cold,
    Warm,
    Offline,
    Done,
}

pub enum BootstrapEvent {
    Request(rpc::BootstrapMsg),
    Response(rpc::BootstrapMsg),
    Timeout,
}

#[derive(Debug)]
pub enum BootstrapAction {
    Connect(ConnectionId, Vec<SocketAddr>),
    UpgradeSeed(ConnectionId, u64),
    Request(ConnectionId, rpc::BootstrapMsg),
    Response(Result<rpc::Response, Error>),
    Completed,
}

impl BoostrapController {
    pub fn new(
        local_id: u64,
        local_addrs: Vec<SocketAddr>,
        bootstrap_addrs: Vec<Vec<SocketAddr>>,
    ) -> Self {
        let mut peers = BTreeMap::new();
        peers.insert(local_id, local_addrs);

        let bootstrap_controller = BoostrapController {
            state: Cell::new(BootstrapState::Cold),
            local_id,
            peers: RefCell::new(peers),
            responded_ids: Default::default(),
            pending_actions_buffer: Default::default(),
        };
        bootstrap_controller.poll_seeds(bootstrap_addrs.into_iter());
        bootstrap_controller
    }

    pub fn pending_actions(&self) -> Vec<BootstrapAction> {
        self.pending_actions_buffer
            .borrow_mut()
            .drain(..)
            .collect::<Vec<BootstrapAction>>()
    }

    pub fn handle_event(&self, event: BootstrapEvent) {
        use BootstrapEvent as E;
        use BootstrapState as S;

        let new_state = match (self.state.get(), event) {
            (S::Cold, E::Request(req))
            | (S::Cold, E::Response(req))
            | (S::Offline, E::Request(req)) => {
                self.handle_msg(req);
                Some(S::Warm)
            }
            (S::Warm, E::Request(req)) | (S::Warm, E::Response(req)) => {
                self.handle_msg(req);

                let num_peers = self.peers.borrow().len();
                let num_responded = self.responded_ids.borrow().len();
                if num_peers == (num_responded + 1) {
                    self.send(BootstrapAction::Completed);
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
            self.state.set(new_state);
        }
    }

    fn handle_msg(&self, req: rpc::BootstrapMsg) {
        if req.from_id == self.local_id {
            return;
        }

        let mut responded_ids = self.responded_ids.borrow_mut();
        if !responded_ids.contains(&req.from_id) {
            let new_nodes = self.merge_nodes_list(&req.nodes);
            for (id, addrs) in new_nodes {
                let id = ConnectionId::Peer(id);
                self.send(BootstrapAction::Connect(id.clone(), addrs));
                self.send_bootstrap_request(id);
            }
            responded_ids.insert(req.from_id);
        }
    }

    #[inline]
    fn poll_seeds(&self, addrs: impl Iterator<Item = Vec<SocketAddr>>) {
        for (id, seed_addrs) in addrs.enumerate() {
            let id = ConnectionId::Seed(id);
            self.send(BootstrapAction::Connect(id.clone(), seed_addrs));
            self.send_bootstrap_request(id);
        }
    }

    #[inline]
    fn send_bootstrap_request(&self, to: ConnectionId) {
        let nodes = self
            .peers
            .borrow()
            .iter()
            .map(|(id, addrs)| (*id, addrs.clone()))
            .collect();

        self.send(BootstrapAction::Request(
            to,
            rpc::BootstrapMsg {
                from_id: self.local_id,
                nodes,
            },
        ));
    }

    #[inline]
    fn send(&self, action: BootstrapAction) {
        self.pending_actions_buffer.borrow_mut().push_back(action)
    }

    /// Merges `other` nodes list to already known. Returns new nodes count
    fn merge_nodes_list(
        &self,
        nodes_from: &Vec<(u64, Vec<SocketAddr>)>,
    ) -> Vec<(u64, Vec<SocketAddr>)> {
        let mut new_nodes = Vec::<(u64, Vec<SocketAddr>)>::with_capacity(nodes_from.len());
        {
            let mut nodes_into = self.peers.borrow_mut();
            for (id, addrs) in nodes_from.into_iter() {
                if !nodes_into.contains_key(id) {
                    nodes_into.insert(*id, addrs.clone());
                    new_nodes.push((*id, addrs.clone()));
                }
            }
        }
        new_nodes
    }
}
