use tarantool::raft::inner::{NodeAction, NodeEvent, NodeInner};
use tarantool::raft::net::ConnectionId;
use tarantool::raft::rpc;

pub fn test_bootstrap_solo() {
    let local_addrs = vec!["127.0.0.1:3301".parse().unwrap()];
    let remote_addrs = vec!["127.0.0.1:3302".parse().unwrap()];

    let mut node = NodeInner::new(1, local_addrs.clone(), vec![remote_addrs.clone()]);

    let actions = node.pending_actions();
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

    node.handle_event(NodeEvent::Response(rpc::BootstrapMsg {
        from_id: 2,
        nodes: vec![(2, remote_addrs.clone())],
    }));

    let actions = node.pending_actions();
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

    node.handle_event(NodeEvent::Response(rpc::BootstrapMsg {
        from_id: 2,
        nodes: vec![(1, local_addrs.clone()), (2, remote_addrs.clone())],
    }));

    let actions = node.pending_actions();
    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], NodeAction::Completed))
}

pub fn test_bootstrap_2n() {
    let n1_addrs = vec!["127.0.0.1:3301".parse().unwrap()];
    let n2_addrs = vec!["127.0.0.1:3302".parse().unwrap()];

    let mut n1_ctrl = NodeInner::new(1, n1_addrs.clone(), vec![n2_addrs.clone()]);
    let mut n2_ctrl = NodeInner::new(2, n2_addrs.clone(), vec![n1_addrs.clone()]);

    assert_eq!(communicate(&mut n1_ctrl, &mut n2_ctrl), (false, false));
    assert_eq!(communicate(&mut n1_ctrl, &mut n2_ctrl), (false, false));
    assert_eq!(communicate(&mut n1_ctrl, &mut n2_ctrl), (true, true));
}

fn communicate(n1_ctrl: &mut NodeInner, n2_ctrl: &mut NodeInner) -> (bool, bool) {
    let n1_actions = n1_ctrl.pending_actions();
    let n2_actions = n2_ctrl.pending_actions();

    let mut n1_is_completed = false;
    for action in n1_actions {
        if let NodeAction::Completed = action {
            n1_is_completed = true;
        }
        forward_action(action, n2_ctrl);
    }

    let mut n2_is_completed = false;
    for action in n2_actions {
        if let NodeAction::Completed = action {
            n2_is_completed = true;
        }
        forward_action(action, n1_ctrl);
    }

    (n1_is_completed, n2_is_completed)
}

fn forward_action(action: NodeAction, node_ctrl: &mut NodeInner) {
    match action {
        NodeAction::Request(_, msg) => node_ctrl.handle_event(NodeEvent::Request(msg)),
        NodeAction::Response(resp) => match resp.unwrap() {
            rpc::Response::Bootstrap(msg) => node_ctrl.handle_event(NodeEvent::Response(msg)),
            _ => {}
        },
        _ => {}
    };
}
