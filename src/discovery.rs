use ::tarantool::fiber::{mutex::MutexGuard, Mutex};
use ::tarantool::proc;
use ::tarantool::uuid::Uuid;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::error::Error as StdError;

use crate::stringify_cfunc;
use crate::tarantool;

type Address = String;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Role {
    Leader { address: Address },
    NonLeader { leader: Address },
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
    fn new(tmp_id: impl AsRef<str>, peers: impl IntoIterator<Item = impl Into<Address>>) -> Self {
        // BTree and sorting for deterministic results and simpler asserts tests.
        let peers: BTreeSet<Address> = peers.into_iter().map(Into::into).sorted().collect();
        assert!(!peers.is_empty(), "peers should not be empty");
        Self {
            pending_request: false,
            visited: [].into(),
            address: None,
            state: State::LeaderElection(LeaderElection {
                tmp_id: tmp_id.as_ref().into(),
                peers,
            }),
        }
    }

    fn handle_request(&mut self, request: Request, to: &Address) -> Response {
        match (&mut self.state, request) {
            (State::Done(_), _) => {} // done we are
            (
                State::LeaderElection(LeaderElection { tmp_id, peers }),
                Request {
                    tmp_id: request_tmp_id,
                    peers: request_peers,
                },
            ) => {
                if !request_peers.is_subset(peers) {
                    // found a new peer
                    self.visited.clear()
                }
                peers.extend(request_peers.iter().cloned());

                if tmp_id == &request_tmp_id {
                    match &self.address {
                        Some(address) => {
                            if address != to {
                                todo!("current peer is reachable by multiple addresses")
                            }
                        }
                        None => self.address = Some(to.clone()),
                    };
                }
            }
        }
        self.state.clone()
    }

    fn handle_response(&mut self, response: Response) {
        //     def handle_resp(self_id, resp):
        //         self_state = instances[self_id]

        //         if resp['leader']:
        //             leader = resp['leader']
        //             self_state['leader'] =  leader
        //             diag.append(f"note over {self_id}: DONE (early) {leader}")
        //             return leader
        //         else:
        //             if resp['peers'] - self_state['peers']:
        //                 self_state['visited'].clear()
        //                 diag.append(f"note over {self_id}: reset visits")
        //             self_state['peers'].update(resp['peers'])
        //             diag.append(f"note over {self_id}: (after resp) {state_fmt(self_state)}")

        //         if not (self_state['peers'] - self_state['visited']):
        //             leader = sorted(self_state['peers'])[0]
        //             self_state['leader'] = leader
        //             diag.append(f"note over {self_id}: DONE {leader}")
        //             return leader
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
                            .sorted()
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
            (
                State::LeaderElection { .. },
                Response::Done(
                    Role::Leader {
                        address: leader_address,
                    }
                    | Role::NonLeader {
                        leader: leader_address,
                    },
                ),
            ) => {
                self.state = State::Done(Role::NonLeader {
                    leader: leader_address,
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
                let res = peers.difference(&self.visited).cloned().next();
                let addr = match &res {
                    Some(addr) => {
                        self.visited.insert(addr.clone());
                        addr
                    }
                    None => peers.iter().sorted().next().unwrap(), // peers is not empty
                };
                self.pending_request = true;
                Some((le.clone(), addr.clone()))
            }
            State::Done(_) => None,
        }
    }
}

// TODO Мутекс здесь не нужен, пусть даже он тарантульный.
// Здесь достаточно просто
// static mut RAFT_DISCOVERY: Option<&'static Discovery> = None;
// Мутекс - это потенциальный йилд, но алгоритм дискавери не предполагает
// никаких йилдов. Обработка запросов и ответов и так должна быть атомарной.
// Если же это не так - то это некорректный алгоритм,
// а мутекс - не больше чем попытка замести грязь под ковёр.
static mut DISCOVERY: &Option<Mutex<Discovery>> = &None;

fn discovery() -> MutexGuard<'static, Discovery> {
    unsafe { DISCOVERY }
        .as_ref()
        .expect("discovery error: expected DISCOVERY to be set on instance startup")
        .lock()
}

pub fn init_global(peers: impl IntoIterator<Item = impl Into<Address>>) {
    let d = Discovery::new(Uuid::random().to_string(), peers);
    unsafe { DISCOVERY = Box::leak(Box::new(Some(Mutex::new(d)))) }
}

pub fn wait_global() -> Role {
    loop {
        let mut d = discovery();
        if let State::Done(role) = &d.state {
            return role.clone();
        }
        let step = d.next();
        drop(d); // release the lock before doing i/o
        if let Some((request, address)) = &step {
            let fn_name = stringify_cfunc!(proc_discover);
            let response = tarantool::net_box_call_retry(address, fn_name, &(request, address));
            discovery().handle_response(response);
        }
    }
}

#[proc]
fn proc_discover(request: Request, request_to: Address) -> Result<Response, Box<dyn StdError>> {
    let mut discovery = discovery();
    Ok(discovery.handle_request(request, &request_to))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use super::*;
    use rand::prelude::*;

    impl Role {
        fn leader(&self) -> Address {
            (match self {
                Role::Leader { address: l } => l,
                Role::NonLeader { leader: l } => l,
            })
            .clone()
        }
    }

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
                        let response = peer.handle_request(request, &dst).clone();
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
                res.values().map(Role::leader).all_equal(),
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
                res.values().map(Role::leader).all_equal(),
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
                res.values().map(Role::leader).all_equal(),
                "multiple leaders: {:#?}",
                res
            );
        }
    }
}
