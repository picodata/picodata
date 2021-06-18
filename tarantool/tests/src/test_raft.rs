use tarantool::raft::bootstrap::{BoostrapController, BootstrapAction, BootstrapEvent};
use tarantool::raft::net::ConnectionId;
use tarantool::raft::rpc;

pub fn test_bootstrap_solo() {
    let local_addrs = vec!["127.0.0.1:3301".parse().unwrap()];
    let remote_addrs = vec!["127.0.0.1:3302".parse().unwrap()];

    let mut node = BoostrapController::new(1, local_addrs.clone(), vec![remote_addrs.clone()]);

    let actions = node.pending_actions();
    assert_eq!(actions.len(), 2);
    assert!(matches!(
        &actions[0],
        BootstrapAction::Connect(ConnectionId::Seed(_), addrs) if addrs == &remote_addrs
    ));
    assert!(matches!(
        &actions[1],
        BootstrapAction::Request(_, req) if req == &rpc::BootstrapMsg {
            from_id: 1,
            nodes: vec![(1, local_addrs.clone())],
        }
    ));

    node.handle_event(BootstrapEvent::Response(rpc::BootstrapMsg {
        from_id: 2,
        nodes: vec![(2, remote_addrs.clone())],
    }));

    let actions = node.pending_actions();
    assert_eq!(actions.len(), 2);
    assert!(matches!(
        &actions[0],
        BootstrapAction::Connect(ConnectionId::Peer(2), addrs) if addrs == &remote_addrs
    ));
    assert!(matches!(
        &actions[1],
        BootstrapAction::Request(_, req) if req == &rpc::BootstrapMsg {
            from_id: 1,
            nodes: vec![
                (1, local_addrs.clone()),
                (2, remote_addrs.clone())
            ],
        }
    ));

    node.handle_event(BootstrapEvent::Response(rpc::BootstrapMsg {
        from_id: 2,
        nodes: vec![(1, local_addrs.clone()), (2, remote_addrs.clone())],
    }));

    let actions = node.pending_actions();
    assert_eq!(actions.len(), 1);
    assert!(matches!(&actions[0], BootstrapAction::Completed))
}

pub fn test_bootstrap_2n() {
    let n1_addrs = vec!["127.0.0.1:3301".parse().unwrap()];
    let n2_addrs = vec!["127.0.0.1:3302".parse().unwrap()];

    let mut n1_ctrl = BoostrapController::new(1, n1_addrs.clone(), vec![n2_addrs.clone()]);
    let mut n2_ctrl = BoostrapController::new(2, n2_addrs.clone(), vec![n1_addrs.clone()]);

    assert_eq!(communicate(&n1_ctrl, &n2_ctrl), (false, false));
    assert_eq!(communicate(&n1_ctrl, &n2_ctrl), (false, false));
    assert_eq!(communicate(&n1_ctrl, &n2_ctrl), (true, true));
}

fn communicate(n1_ctrl: &BoostrapController, n2_ctrl: &BoostrapController) -> (bool, bool) {
    let n1_actions = n1_ctrl.pending_actions();
    let n2_actions = n2_ctrl.pending_actions();

    let mut n1_is_completed = false;
    for action in n1_actions {
        if let BootstrapAction::Completed = action {
            n1_is_completed = true;
        }
        forward_action(action, n2_ctrl);
    }

    let mut n2_is_completed = false;
    for action in n2_actions {
        if let BootstrapAction::Completed = action {
            n2_is_completed = true;
        }
        forward_action(action, n1_ctrl);
    }

    (n1_is_completed, n2_is_completed)
}

fn forward_action(action: BootstrapAction, node_ctrl: &BoostrapController) {
    match action {
        BootstrapAction::Request(_, msg) => node_ctrl.handle_event(BootstrapEvent::Request(msg)),
        BootstrapAction::Response(resp) => match resp.unwrap() {
            rpc::Response::Bootstrap(msg) => node_ctrl.handle_event(BootstrapEvent::Response(msg)),
            _ => {}
        },
        _ => {}
    };
}
