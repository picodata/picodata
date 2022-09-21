use ::raft::prelude as raft;
use ::tarantool::fiber;
use ::tarantool::net_box::Conn;
use ::tarantool::net_box::ConnOptions;
use ::tarantool::net_box::Options;
use ::tarantool::util::IntoClones;
use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use crate::mailbox::Mailbox;
use crate::tlog;
use crate::traft;
use crate::traft::error::PoolSendError;
use crate::traft::storage::peer_field::{self, PeerAddress};
use crate::traft::RaftId;
use crate::traft::{PeerStorage, Storage};

#[derive(Clone, Debug)]
struct WorkerOptions {
    handler_name: &'static str,
    call_timeout: Duration,
    connect_timeout: Duration,
    inactivity_timeout: Duration,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            handler_name: "",
            call_timeout: Duration::ZERO,
            connect_timeout: Duration::ZERO,
            inactivity_timeout: Duration::MAX,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// PoolWorker
////////////////////////////////////////////////////////////////////////////////

struct PoolWorker {
    id: RaftId,
    inbox: Mailbox<traft::MessagePb>,
    fiber: fiber::LuaUnitJoinHandle<'static>,
    stop_flag: Rc<Cell<Option<()>>>,
}

impl PoolWorker {
    pub fn run(id: RaftId, storage: PeerStorage, opts: WorkerOptions) -> PoolWorker {
        let inbox = Mailbox::new();
        let stop_flag: Rc<Cell<Option<()>>> = Default::default();
        let fiber = fiber::defer_proc({
            let inbox = inbox.clone();
            let stop_flag = stop_flag.clone();
            move || Self::worker_loop(id, storage, inbox, stop_flag, &opts)
        });

        Self {
            id,
            fiber,
            inbox,
            stop_flag,
        }
    }

    fn worker_loop(
        raft_id: RaftId,
        storage: PeerStorage,
        inbox: Mailbox<traft::MessagePb>,
        stop_flag: Rc<Cell<Option<()>>>,
        opts: &WorkerOptions,
    ) {
        struct ConnCache {
            address: String,
            conn: Conn,
        }
        let cache: Cell<Option<ConnCache>> = Cell::default();

        loop {
            // implicit yield
            let messages = inbox.receive_all(opts.inactivity_timeout);
            if stop_flag.take().is_some() {
                return;
            }

            if messages.is_empty() {
                // Connection has long been unused. Close it.
                cache.take();
                continue;
            }

            let ConnCache { address, conn } = ::tarantool::unwrap_or!(cache.take(), {
                let address = storage.field_by_raft_id::<PeerAddress>(raft_id);
                let address = crate::unwrap_ok_or!(address,
                    Err(e) => {
                        tlog!(Warning, "failed getting peer address: {e}";
                            "raft_id" => raft_id,
                        );
                        continue
                    }
                );

                let conn_opts = ConnOptions {
                    connect_timeout: opts.connect_timeout,
                    ..Default::default()
                };
                let conn = Conn::new(&address, conn_opts, None);
                let conn = crate::unwrap_ok_or!(conn,
                    Err(e) => {
                        tlog!(Debug, "failed establishing connection to peer: {e}";
                            "peer" => address,
                            "raft_id" => raft_id,
                        );
                        continue
                    }
                );
                ConnCache { address, conn }
            });

            let call_opts = Options {
                timeout: Some(opts.call_timeout),
                ..Default::default()
            };

            // implicit yield
            match conn.call(opts.handler_name, &messages, &call_opts) {
                Ok(_) => cache.set(Some(ConnCache { address, conn })),
                Err(e) => tlog!(Debug, "failed sending messages to peer: {e}";
                    "peer" => address,
                ),
            }

            if stop_flag.take().is_some() {
                return;
            }
        }
    }

    pub fn send(&self, msg: raft::Message) -> Result<(), PoolSendError> {
        self.inbox.send(msg.into());
        Ok(())
    }

    fn stop(self) {
        self.stop_flag.set(Some(()));
        self.fiber.join();
    }
}

impl std::fmt::Debug for PoolWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolWorker").field("id", &self.id).finish()
    }
}

////////////////////////////////////////////////////////////////////////////////
// ConnectionPoolBuilder
////////////////////////////////////////////////////////////////////////////////

pub struct ConnectionPoolBuilder {
    worker_options: WorkerOptions,
    storage: PeerStorage,
}

macro_rules! builder_option {
    ($opt:ident, $t:ty) => {
        pub fn $opt(mut self, val: $t) -> Self {
            self.worker_options.$opt = val;
            self
        }
    };
}

impl ConnectionPoolBuilder {
    builder_option!(handler_name, &'static str);
    builder_option!(call_timeout, Duration);
    builder_option!(connect_timeout, Duration);
    builder_option!(inactivity_timeout, Duration);

    pub fn build(self) -> ConnectionPool {
        ConnectionPool {
            worker_options: self.worker_options,
            workers: HashMap::new(),
            storage: self.storage,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ConnectionPool
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ConnectionPool {
    worker_options: WorkerOptions,
    workers: HashMap<RaftId, PoolWorker>,
    storage: PeerStorage,
}

impl ConnectionPool {
    pub fn builder(storage: PeerStorage) -> ConnectionPoolBuilder {
        ConnectionPoolBuilder {
            storage,
            worker_options: Default::default(),
        }
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
    pub fn send(&mut self, msg: raft::Message) -> Result<(), PoolSendError> {
        let raft_id = msg.to;
        if !self.workers.contains_key(&msg.to) {
            let storage = self.storage.clone();
            let worker_options = self.worker_options.clone();
            // check that peer exists
            storage
                .field_by_raft_id::<peer_field::RaftId>(raft_id)
                .map_err(|_| PoolSendError::UnknownRecipient(raft_id))?;
            let wrk = PoolWorker::run(raft_id, storage, worker_options);
            self.workers.insert(raft_id, wrk);
        }

        let wrk = self.workers.get(&raft_id).expect("just inserted it");
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

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

// TODO test connecting twice (reconnecting)
// thread 'main' panicked at 'UnitJoinHandle dropped before being joined',
// picodata::traft::network::ConnectionPool::connect

inventory::submit!(crate::InnerTest {
    name: "test_traft_pool",
    body: || {
        use std::rc::Rc;
        use tarantool::tlua;

        let l = tarantool::lua_state();

        // Mock the handler
        let (tx, rx) = fiber::Channel::new(0).into_clones();
        l.set(
            "test_interact",
            tlua::function1(move |pb: tlua::AnyLuaString| {
                use protobuf::Message as _;
                let mut msg = raft::Message::default();
                msg.merge_from_bytes(pb.as_bytes()).unwrap();
                tx.send((msg.msg_type, msg.to, msg.from)).unwrap();

                // Lock forever, never respond. This trick allows to check
                // how pool behaves in case of the irresponsive TCP connection.
                fiber::Cond::new().wait()
            }),
        );

        let storage = Storage::peers_access();
        // Connect to the current Tarantool instance
        let mut pool = ConnectionPool::builder(storage.clone())
            .handler_name("test_interact")
            .call_timeout(Duration::from_millis(50))
            .connect_timeout(Duration::from_millis(50))
            .build();
        let listen: String = l.eval("return box.info.listen").unwrap();

        storage
            .persist_peer(&traft::Peer {
                raft_id: 1337,
                peer_address: listen.clone(),
                ..Default::default()
            })
            .unwrap();
        tlog!(Info, "TEST: connecting {listen}");
        // pool.connect(1337, listen);

        let heartbeat_to_from = |to: RaftId, from: RaftId| raft::Message {
            msg_type: raft::MessageType::MsgHeartbeat,
            to,
            from,
            ..Default::default()
        };

        // Send a request
        // TODO: assert there's no yield
        pool.send(heartbeat_to_from(1337, 1)).unwrap();

        // Assert it arrives
        // Assert equality
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(10)),
            Ok((raft::MessageType::MsgHeartbeat, 1337u64, 1u64))
        );

        // Assert unknown recepient error
        assert!(matches!(
            pool.send(heartbeat_to_from(9999, 3)).unwrap_err(),
            PoolSendError::UnknownRecipient(9999)
        ));

        // Set up on_disconnect trigger
        let on_disconnect_cond = Rc::new(fiber::Cond::new());
        l.exec_with("box.session.on_disconnect(...)", {
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
        pool.send(heartbeat_to_from(1337, 4)).unwrap();

        // Assert it arrives too
        // Assert equality
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(10)),
            Ok((raft::MessageType::MsgHeartbeat, 1337u64, 4u64))
        );
    }
});
