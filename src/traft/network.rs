use ::raft::prelude as raft;
use ::tarantool::fiber;
use ::tarantool::net_box::Conn;
use ::tarantool::net_box::ConnOptions;
use ::tarantool::net_box::Options;
use ::tarantool::util::IntoClones;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::Duration;

use crate::error::PoolSendError;
use crate::tlog;
use crate::traft::row;

type RaftId = u64;

#[derive(Debug)]
pub struct ConnectionPool {
    workers: HashMap<RaftId, PoolWorker>,
    timeout: Duration,
}

struct PoolWorker {
    id: RaftId,
    uri: String,
    channel: fiber::Channel<row::Message>,
    fiber: fiber::UnitJoinHandle<'static>,
}

impl PoolWorker {
    pub fn run_with_timeout(id: RaftId, uri: &str, timeout: Duration) -> PoolWorker {
        let (tx, rx) = fiber::Channel::new(0).into_clones();
        let worker_fn = {
            let uri = uri.to_owned();
            move || {
                let call_opts = Options {
                    timeout: Some(timeout),
                    ..Default::default()
                };

                for msg in &rx {
                    let conn_opts = ConnOptions {
                        connect_timeout: timeout,
                        ..Default::default()
                    };

                    let conn = match Conn::new(uri.clone(), conn_opts, None) {
                        Ok(conn) => conn,
                        Err(e) => {
                            tlog!(Error, "Interact with {uri} -> {e}");
                            continue;
                        }
                    };

                    for msg in std::iter::once(msg).chain(&rx) {
                        if let Err(e) = conn.call(".raft_interact", &msg, &call_opts) {
                            tlog!(Error, "Interact with {uri} -> {e}");
                            break;
                        };
                    }
                }
            }
        };

        Self {
            id,
            uri: uri.to_owned(),
            fiber: fiber::start_proc(worker_fn),
            channel: tx,
        }
    }

    pub fn send(&self, msg: row::Message) -> Result<(), PoolSendError> {
        match self.channel.try_send(msg) {
            Ok(_) => Ok(()),
            Err(fiber::TrySendError::Full(_)) => Err(PoolSendError::WorkerBusy),
            Err(fiber::TrySendError::Disconnected(_)) => unreachable!(),
        }
    }

    fn stop(self) {
        self.channel.close();
        self.fiber.join();
    }
}

impl std::fmt::Debug for PoolWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolWorker")
            .field("id", &self.id)
            .field("uri", &self.uri)
            .finish()
    }
}

impl ConnectionPool {
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            workers: HashMap::new(),
            timeout,
        }
    }

    /// Create a worker for communicating with another node.
    /// Connection is established lazily at the first request.
    /// It's also re-established automatically upon any error.
    pub fn connect(&mut self, id: RaftId, uri: &str) {
        self.workers
            .insert(id, PoolWorker::run_with_timeout(id, uri, self.timeout));
    }

    #[allow(dead_code)]
    #[allow(unused_variables)]
    pub fn disconnect(&mut self, id: RaftId) {
        panic!("not implemented yet");
    }

    /// Send a message to `msg.to` asynchronously.
    /// If the massage can't be sent, it's a responsibility
    /// of the raft node to re-send it later.
    ///
    /// This function never yields.
    pub fn send(&self, msg: &raft::Message) -> Result<(), PoolSendError> {
        tlog!(Debug, "Sending {msg:?}");

        let wrk = self
            .workers
            .get(&msg.to)
            .ok_or(PoolSendError::UnknownRecipient)?;
        let msg = row::Message::try_from(msg.clone())?;
        wrk.send(msg)
    }
}

impl Drop for ConnectionPool {
    fn drop(&mut self) {
        for (_, worker) in self.workers.drain() {
            worker.stop();
        }
    }
}

inventory::submit!(crate::InnerTest {
    name: "test_traft_pool",
    body: || {
        use std::rc::Rc;
        use tarantool::tlua;

        let l = tarantool::lua_state();

        // Monkeypatch the handler
        let (tx, rx) = fiber::Channel::new(0).into_clones();
        l.set(
            "",
            vec![(
                "raft_interact",
                tlua::function3(move |msg_type: String, to: u64, from: u64| {
                    // It's hard to fully check traft::row::Message because
                    // netbox sends its fields as a flat tuple.
                    // So we only check three fields.
                    tx.send((msg_type, to, from)).unwrap();
                    // lock forever, never respond
                    fiber::Cond::new().wait()
                }),
            )],
        );
        let () = l.eval("box.schema.func.drop('.raft_interact')").unwrap();

        // Connect to the current Tarantool instance
        let mut pool = ConnectionPool::with_timeout(Duration::from_millis(50));
        let listen: String = l.eval("return box.info.listen").unwrap();
        tlog!(Info, "TEST: connecting {listen}");
        pool.connect(1337, &listen);

        let heartbeat_to_from = |to: u64, from: u64| raft::Message {
            msg_type: raft::MessageType::MsgHeartbeat,
            to,
            from,
            ..Default::default()
        };

        // Send a request
        // TODO: assert there's no yield
        pool.send(&heartbeat_to_from(1337, 1)).unwrap();

        // Assert it arrives
        // Assert equality
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(10)),
            Ok(("MsgHeartbeat".to_owned(), 1337u64, 1u64))
        );

        // Assert the worker is still busy
        assert!(matches!(
            pool.send(&heartbeat_to_from(1337, 2)).unwrap_err(),
            PoolSendError::WorkerBusy
        ));

        // Assert unknown recepient error
        assert!(matches!(
            pool.send(&heartbeat_to_from(9999, 3)).unwrap_err(),
            PoolSendError::UnknownRecipient
        ));

        // Set up on_disconnect trigger
        let on_disconnect_cond = Rc::new(fiber::Cond::new());
        let on_disconnect: tlua::LuaFunction<_> =
            l.eval("return box.session.on_disconnect").unwrap();
        let () = on_disconnect
            .call_with_args({
                let cond = on_disconnect_cond.clone();
                tlua::function0(move || cond.broadcast())
            })
            .unwrap();

        // Wait for it
        on_disconnect_cond
            .wait_timeout(Duration::from_millis(100))
            .then(|| (tlog!(Info, "TEST: on_disconnect triggered")))
            .or_else(|| panic!("on_disconnect timed out"));

        // Send the second request
        // TODO: assert there's no yield
        pool.send(&heartbeat_to_from(1337, 4)).unwrap();

        // Assert it arrives too
        // Assert equality
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(10)),
            Ok(("MsgHeartbeat".to_owned(), 1337u64, 4u64))
        );
    }
});
