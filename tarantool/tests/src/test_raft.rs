use std::collections::VecDeque;

use tarantool::raft::inner::{NodeAction, NodeEvent, NodeInner};
use tarantool::raft::net::ConnectionId;
use tarantool::raft::rpc;

pub fn test_bootstrap_solo() {
    let local_addrs = vec!["127.0.0.1:3301".parse().unwrap()];
    let remote_addrs = vec!["127.0.0.1:3302".parse().unwrap()];

    let mut events = VecDeque::new();
    let mut actions = VecDeque::new();

    let mut node = NodeInner::new(1, local_addrs.clone(), vec![remote_addrs.clone()]);
    node.update(&mut events, &mut actions);

    assert_eq!(actions.len(), 2);
    assert!(matches!(
        &actions[0],
        NodeAction::Connect(ConnectionId::Seed(_), addrs) if addrs == &remote_addrs
    ));
    assert!(matches!(
        &actions[1],
        NodeAction::Request(_, req) if req == &rpc::BootstrapMsg {
            from_id: 1,
            nodes: vec![(1, local_addrs.clone())],
        }
    ));
    actions.clear();

    events.push_back(NodeEvent::Response(rpc::BootstrapMsg {
        from_id: 2,
        nodes: vec![(2, remote_addrs.clone())],
    }));
    node.update(&mut events, &mut actions);

    assert_eq!(actions.len(), 2);
    assert!(matches!(
        &actions[0],
        NodeAction::Connect(ConnectionId::Peer(2), addrs) if addrs == &remote_addrs
    ));
    assert!(matches!(
        &actions[1],
        NodeAction::Request(_, req) if req == &rpc::BootstrapMsg {
            from_id: 1,
            nodes: vec![
                (1, local_addrs.clone()),
                (2, remote_addrs.clone())
            ],
        }
    ));
    actions.clear();

    events.push_back(NodeEvent::Response(rpc::BootstrapMsg {
        from_id: 2,
        nodes: vec![(1, local_addrs.clone()), (2, remote_addrs.clone())],
    }));
    node.update(&mut events, &mut actions);

    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], NodeAction::Completed))
}

pub fn test_bootstrap_2n() {
    let n1_addrs = vec!["127.0.0.1:3301".parse().unwrap()];
    let n2_addrs = vec!["127.0.0.1:3302".parse().unwrap()];

    let mut n1_events = VecDeque::new();
    let mut n1_actions = VecDeque::new();
    let mut n1_ctrl = NodeInner::new(1, n1_addrs.clone(), vec![n2_addrs.clone()]);

    let mut n2_events = VecDeque::new();
    let mut n2_actions = VecDeque::new();
    let mut n2_ctrl = NodeInner::new(2, n2_addrs.clone(), vec![n1_addrs.clone()]);

    let mut n1_is_completed = false;
    let mut n2_is_completed = false;

    for _ in 0..3 {
        n1_ctrl.update(&mut n1_events, &mut n1_actions);
        n2_ctrl.update(&mut n2_events, &mut n2_actions);

        n1_is_completed = n1_is_completed || communicate(&mut n1_actions, &mut n2_events);
        n2_is_completed = n2_is_completed || communicate(&mut n2_actions, &mut n1_events);
    }

    assert!(n1_is_completed);
    assert!(n2_is_completed);
}

fn communicate(from: &mut VecDeque<NodeAction>, to: &mut VecDeque<NodeEvent>) -> bool {
    let mut is_completed = false;
    for action in from.drain(..) {
        if let NodeAction::Completed = action {
            is_completed = true;
        }

        if let Some(event) = forward_action(action) {
            to.push_back(event);
        }
    }
    is_completed
}

fn forward_action(action: NodeAction) -> Option<NodeEvent> {
    match action {
        NodeAction::Request(_, msg) => Some(NodeEvent::Request(msg)),
        NodeAction::Response(resp) => match resp.unwrap() {
            rpc::Response::Bootstrap(msg) => Some(NodeEvent::Response(msg)),
            _ => None,
        },
        _ => None,
    }
}
