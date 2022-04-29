use ::tarantool::fiber::{mutex::MutexGuard, Mutex};
use ::tarantool::proc;
use ::tarantool::uuid::Uuid;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::error::Error as StdError;

use crate::stringify_cfunc;
use crate::tarantool;
use crate::traft;

type Address = String;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Role {
    Leader { address: Address },
    NonLeader { leader: Address },
}

impl Role {
    fn new(address: Address, is_leader: bool) -> Self {
        if is_leader {
            Self::Leader { address }
        } else {
            Self::NonLeader { leader: address }
        }
    }

    fn leader_address(&self) -> &Address {
        match self {
            Self::Leader { address } => address,
            Self::NonLeader { leader } => leader,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaderElection {
    tmp_id: String,
    peers: BTreeSet<Address>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum State {
    LeaderElection(LeaderElection),
    Done(Role),
}

pub type Request = LeaderElection;
pub type Response = State;

#[derive(Debug)]
pub struct Discovery {
    // BTreeSet for determinism
    pending_request: bool,
    visited: BTreeSet<Address>,
    address: Option<Address>,
    state: State,
}

impl Discovery {
    fn new(tmp_id: impl Into<String>, peers: impl IntoIterator<Item = impl Into<Address>>) -> Self {
        // BTree and sorting for deterministic results and simpler asserts tests.
        let peers: BTreeSet<Address> = peers.into_iter().map(Into::into).collect();
        assert!(!peers.is_empty(), "peers should not be empty");
        Self {
            pending_request: false,
            visited: [].into(),
            address: None,
            state: State::LeaderElection(LeaderElection {
                tmp_id: tmp_id.into(),
                peers,
            }),
        }
    }

    fn handle_request(&mut self, request: Request, to: Address) -> &Response {
        match &mut self.state {
            State::Done(_) => {} // done we are
            State::LeaderElection(LeaderElection { tmp_id, peers }) => {
                if !request.peers.is_subset(peers) {
                    // found a new peer
                    self.visited.clear()
                }
                peers.extend(request.peers);

                if tmp_id == &request.tmp_id {
                    match &self.address {
                        Some(address) if address != &to => {
                            todo!("current peer is reachable by multiple addresses")
                        }
                        Some(_) => {}
                        None => self.address = Some(to),
                    };
                }
            }
        }
        &self.state
    }

    fn handle_response(&mut self, response: Response) {
        match (&mut self.state, response) {
            (
                State::LeaderElection(LeaderElection { peers, .. }),
                Response::LeaderElection(LeaderElection {
                    peers: response_peers,
                    ..
                }),
            ) => {
                if !response_peers.is_subset(peers) {
                    // found a new peer
                    self.visited.clear()
                }
                peers.extend(response_peers);

                if let Some(address) = &self.address {
                    if peers.is_subset(&self.visited)
                        && peers
                            .iter()
                            .next()
                            .expect("not expected peer_addresses to be empty")
                            == address
                    {
                        self.state = State::Done(Role::Leader {
                            address: address.clone(),
                        });
                        self.visited.clear();
                        self.address = None;
                    }
                }
            }
            (State::LeaderElection { .. }, Response::Done(role)) => {
                self.state = State::Done(Role::NonLeader {
                    leader: role.leader_address().into(),
                });
                self.visited.clear();
                self.address = None;
            }
            (State::Done(_), _) => {}
        }
        self.pending_request = false;
    }

    fn next(&mut self) -> Option<(Request, Address)> {
        if self.pending_request {
            return None;
        }
        match &self.state {
            State::LeaderElection(le @ LeaderElection { peers, .. }) => {
                if self.pending_request {
                    return None;
                }
                let res = peers.difference(&self.visited).next().cloned();
                let addr = match &res {
                    Some(addr) => {
                        self.visited.insert(addr.clone());
                        addr
                    }
                    None => peers.iter().next().unwrap(), // peers is not empty
                };
                self.pending_request = true;
                Some((le.clone(), addr.clone()))
            }
            State::Done(_) => None,
        }
    }
}
static mut DISCOVERY: Option<Box<Mutex<Discovery>>> = None;

fn discovery() -> Option<MutexGuard<'static, Discovery>> {
    unsafe { DISCOVERY.as_ref() }.map(|d| d.lock())
}

pub fn init_global(peers: impl IntoIterator<Item = impl Into<Address>>) {
    let d = Discovery::new(Uuid::random().to_string(), peers);
    unsafe { DISCOVERY = Some(Box::new(Mutex::new(d))) }
}

pub fn wait_global() -> Role {
    loop {
        let mut d = discovery().expect("discovery uninitialized");
        if let State::Done(role) = &d.state {
            return role.clone();
        }
        let step = d.next();
        drop(d); // release the lock before doing i/o
        if let Some((request, address)) = &step {
            let fn_name = stringify_cfunc!(proc_discover);
            let response = tarantool::net_box_call_retry(address, fn_name, &(request, address));
            discovery()
                .expect("discovery uninitialized")
                .handle_response(response);
        }
    }
}

#[proc]
fn proc_discover<'a>(
    #[inject(&mut discovery())] discovery: &'a mut Option<MutexGuard<'a, Discovery>>,
    request: Request,
    request_to: Address,
) -> Result<Cow<'a, Response>, Box<dyn StdError>> {
    if let Ok(node) = traft::node::global() {
        let status = node.status();
        let leader = traft::Storage::peer_by_raft_id(status.leader_id)?
            .ok_or("leader id is present, but it's address is unknown")?;
        Ok(Cow::Owned(Response::Done(Role::new(
            leader.peer_address,
            status.am_leader(),
        ))))
    } else {
        let discovery = discovery.as_mut().ok_or("discovery uninitialized")?;
        Ok(Cow::Borrowed(discovery.handle_request(request, request_to)))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use std::collections::{BTreeMap, HashMap};

    use super::*;
    use rand::prelude::*;

    fn run(
        instances: impl IntoIterator<Item = (impl Into<Address>, Discovery)>,
    ) -> HashMap<Address, Role> {
        let mut instances: BTreeMap<Address, Discovery> =
            instances.into_iter().map(|(k, v)| (k.into(), v)).collect();
        let mut done = HashMap::<Address, Role>::new();
        let len = instances.len();
        let addrs = instances.keys().cloned().collect_vec();

        let mut rng = rand::thread_rng();

        enum Event {
            Request(Address, Request, Address),
            Response(Response, Address),
        }

        let mut network: Vec<Event> = [].into();

        while done.len() != len {
            if rng.gen_bool(0.5) {
                let addr = addrs.choose(&mut rng).unwrap();
                let discovery = instances.get_mut(addr).unwrap();
                if let Some((request, peer_addr)) = discovery.next() {
                    network.push(Event::Request(addr.clone(), request, peer_addr));
                }
            } else {
                match network.pop() {
                    Some(Event::Request(src, request, dst)) => {
                        let peer = instances.get_mut(&dst).unwrap();
                        let response = peer.handle_request(request, dst).clone();
                        network.push(Event::Response(response, src))
                    }
                    Some(Event::Response(response, dst)) => {
                        let peer = instances.get_mut(&dst).unwrap();
                        peer.handle_response(response);
                        if let State::Done(role) = &instances.get_mut(&dst).unwrap().state {
                            done.insert(dst.clone(), role.clone());
                        }
                    }
                    None => {}
                };
            }
        }

        done
    }

    #[test]
    fn test_discovery_1() {
        for _ in 0..999 {
            let instances = [
                ("host1:1", Discovery::new("1", ["host1:1"])),
                ("host2:2", Discovery::new("2", ["host1:1"])),
                ("host3:3", Discovery::new("3", ["host1:1"])),
            ];
            let res = run(instances);
            assert!(
                res.values().map(Role::leader_address).all_equal(),
                "multiple leaders: {:#?}",
                res
            );
        }
    }

    #[test]
    fn test_discovery_2() {
        for _ in 0..999 {
            let instances = [
                ("host1:1", Discovery::new("1", ["host2:2"])),
                ("host2:2", Discovery::new("2", ["host2:2"])),
                ("host3:3", Discovery::new("3", ["host2:2"])),
            ];
            let res = run(instances);
            assert!(
                res.values().map(Role::leader_address).all_equal(),
                "multiple leaders: {:#?}",
                res
            );
        }
    }

    #[test]
    fn test_discovery_3() {
        for _ in 0..999 {
            let instances = [
                (
                    "host1:1",
                    Discovery::new("1", ["host1:1", "host2:2", "host3:3"]),
                ),
                ("host2:2", Discovery::new("2", ["host2:2", "host3:3"])),
                ("host3:3", Discovery::new("3", ["host3:3"])),
            ];
            let res = run(instances);
            assert!(
                res.values().map(Role::leader_address).all_equal(),
                "multiple leaders: {:#?}",
                res
            );
        }
    }
}
