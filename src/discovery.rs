use ::tarantool::fiber::{mutex::MutexGuard, sleep, Mutex};
use ::tarantool::proc;
use ::tarantool::uuid::Uuid;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::error::Error as StdError;
use std::time::{Duration, Instant};

use crate::stringify_cfunc;
use crate::tarantool;
use crate::traft;
use crate::util::Either::{self, Left, Right};

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

    fn handle_response(&mut self, from: Address, response: Response) {
        self.visited.insert(from);
        match (&mut self.state, response) {
            (
                State::LeaderElection(LeaderElection { peers, .. }),
                Response::LeaderElection(response),
            ) => {
                if !response.peers.is_subset(peers) {
                    // found a new peer
                    self.visited.clear()
                }
                peers.extend(response.peers);

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
    }

    fn next_or_role(&self) -> Either<(Request, Vec<Address>), Role> {
        match &self.state {
            State::LeaderElection(election) => {
                let mut next_peers = election
                    .peers
                    .difference(&self.visited)
                    .cloned()
                    .collect::<Vec<_>>();
                if next_peers.is_empty() {
                    next_peers.extend(election.peers.iter().next().cloned())
                }
                assert!(!next_peers.is_empty());
                Left((election.clone(), next_peers))
            }
            State::Done(role) => Right(role.clone()),
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
        let d = discovery().expect("discovery uninitialized");
        let (request, curr_peers) = match d.next_or_role() {
            Left(l) => l,
            Right(role) => break role,
        };
        drop(d); // release the lock before doing i/o
        let round_start = Instant::now();
        for address in curr_peers {
            if let Some(response) = tarantool::net_box_call_or_log(
                &address,
                stringify_cfunc!(proc_discover),
                (&request, &address),
                Duration::from_secs(2),
            ) {
                discovery()
                    .expect("discovery deinitialized")
                    .handle_response(address, response)
            }
        }
        sleep(Duration::from_millis(200).saturating_sub(round_start.elapsed()))
    }
}

crate::collect_proc!(proc_discover);
#[proc]
fn proc_discover<'a>(request: Request, request_to: Address) -> Result<Response, Box<dyn StdError>> {
    crate::tarantool::fiber_name("proc_discover");

    let ready_ids = traft::node::global().ok().and_then(|node| {
        let status = node.status();
        status
            .leader_id
            .map(|leader_id| (&node.storage.peer_addresses, leader_id, status.id))
    });
    if let Some((peers_addresses, leader_id, id)) = ready_ids {
        let leader_address = peers_addresses.try_get(leader_id)?;
        Ok(Response::Done(Role::new(leader_address, leader_id == id)))
    } else {
        let mut discovery = discovery();
        let discovery = discovery.as_mut().ok_or("discovery uninitialized")?;
        Ok(discovery.handle_request(request, request_to).clone())
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use std::collections::{BTreeMap, HashMap, HashSet};

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
        let mut pending_requests: HashMap<_, _> = addrs
            .iter()
            .cloned()
            .zip(std::iter::repeat(HashSet::new()))
            .collect();

        let mut rng = rand::thread_rng();

        #[derive(Debug)]
        enum Event {
            Request(Address, Request, Address),
            Response(Address, Response, Address),
        }

        let mut network: Vec<Event> = [].into();

        while done.len() != len {
            if rng.gen_bool(0.5) {
                let src = addrs.choose(&mut rng).unwrap();
                if !pending_requests.get(src).unwrap().is_empty() {
                    continue;
                }
                let discovery = instances.get_mut(src).unwrap();
                if let Left((request, peer_addrs)) = discovery.next_or_role() {
                    for dst in peer_addrs {
                        pending_requests.get_mut(src).unwrap().insert(dst.clone());
                        network.push(Event::Request(src.clone(), request.clone(), dst))
                    }
                }
            } else {
                match network.pop() {
                    Some(Event::Request(src, request, dst)) => {
                        let peer = instances.get_mut(&dst).unwrap();
                        let response = peer.handle_request(request, dst.clone()).clone();
                        network.push(Event::Response(dst, response, src))
                    }
                    Some(Event::Response(src, response, dst)) => {
                        let peer = instances.get_mut(&dst).unwrap();
                        pending_requests.get_mut(&dst).unwrap().remove(&src);
                        peer.handle_response(src, response);
                        if let State::Done(role) = &peer.state {
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
