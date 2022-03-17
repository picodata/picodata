use ::raft::prelude as raft;
use ::tarantool::fiber;
use ::tarantool::net_box::Conn;
use ::tarantool::net_box::ConnOptions;
use ::tarantool::net_box::Options;
use ::tarantool::util::IntoClones;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::Duration;

use crate::mailbox::Mailbox;
use crate::tlog;
use crate::traft;
use crate::traft::error::PoolSendError;

type RaftId = u64;

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

struct PoolWorker {
    id: RaftId,
    uri: Rc<RefCell<String>>,
    inbox: Mailbox<traft::MessagePb>,
    fiber: fiber::LuaUnitJoinHandle<'static>,
    stop_flag: Rc<Cell<Option<()>>>,
}

impl PoolWorker {
    pub fn run(id: RaftId, uri: String, opts: WorkerOptions) -> PoolWorker {
        let uri = Rc::new(RefCell::new(uri));
        let inbox = Mailbox::new();
        let stop_flag: Rc<Cell<Option<()>>> = Default::default();
        let fiber = fiber::defer_proc({
            let uri = uri.clone();
            let inbox = inbox.clone();
            let stop_flag = stop_flag.clone();
            move || Self::worker_loop(uri, inbox, stop_flag, &opts)
        });

        Self {
            id,
            uri,
            fiber,
            inbox,
            stop_flag,
        }
    }

    fn worker_loop(
        uri: Rc<RefCell<String>>,
        inbox: Mailbox<traft::MessagePb>,
        stop_flag: Rc<Cell<Option<()>>>,
        opts: &WorkerOptions,
    ) {
        struct ConnCache {
            uri: String,
            conn: Conn,
        }
        let cache: Cell<Option<ConnCache>> = Cell::default();

        loop {
            // implicit yield
            let messages = inbox.recv_timeout(opts.inactivity_timeout);
            if stop_flag.take().is_some() {
                return;
            }

            if messages.is_empty() {
                cache.take();
                continue;
            }

            let mut conn_cache = match cache.take() {
                Some(v) if uri.borrow().eq(&v.uri) => Some(v),
                _ => None,
            };

            if conn_cache.is_none() {
                let uri = uri.borrow();
                let conn_opts = ConnOptions {
                    connect_timeout: opts.connect_timeout,
                    ..Default::default()
                };
                conn_cache = Conn::new(uri.clone(), conn_opts, None)
                    .map(|conn| ConnCache {
                        uri: uri.clone(),
                        conn,
                    })
                    .map_err(|e| tlog!(Error, "{e}"))
                    .ok();
            }

            let (uri, conn) = match &conn_cache {
                Some(v) => (&v.uri, &v.conn),
                None => continue,
            };

            let call_opts = Options {
                timeout: Some(opts.call_timeout),
                ..Default::default()
            };

            // implicit yield
            match conn.call(opts.handler_name, &messages, &call_opts) {
                Ok(_) => cache.set(conn_cache),
                Err(e) => tlog!(Debug, "Interact with {uri} -> {e}"),
            }

            if stop_flag.take().is_some() {
                return;
            }
        }
    }

    pub fn set_uri(&self, uri: String) {
        self.uri.replace(uri);
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
        f.debug_struct("PoolWorker")
            .field("id", &self.id)
            .field("uri", &self.uri)
            .finish()
    }
}

#[derive(Default)]
pub struct ConnectionPoolBuilder {
    worker_options: WorkerOptions,
}

macro_rules! builder_option {
    ($opt:ident, $t:ty) => {
        pub fn $opt(&'_ mut self, val: $t) -> &'_ mut Self {
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

    pub fn build(&self) -> ConnectionPool {
        ConnectionPool {
            worker_options: self.worker_options.clone(),
            workers: HashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct ConnectionPool {
    worker_options: WorkerOptions,
    workers: HashMap<RaftId, PoolWorker>,
}

impl ConnectionPool {
    pub fn builder() -> ConnectionPoolBuilder {
        ConnectionPoolBuilder::default()
    }

    /// Create a worker for communicating with another node.
    /// Connection is established lazily at the first request.
    /// It's also re-established automatically upon any error.
    pub fn connect(&mut self, id: RaftId, uri: String) {
        let worker_options = self.worker_options.clone();
        self.workers
            .entry(id)
            .and_modify(|wrk| wrk.set_uri(uri.clone()))
            .or_insert_with(|| PoolWorker::run(id, uri.clone(), worker_options));
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
        // tlog!(Debug, "Sending {msg:?}");

        let wrk = self
            .workers
            .get(&msg.to)
            .ok_or(PoolSendError::UnknownRecipient)?;
        // let msg = Message::try_from(msg.clone())?;
        wrk.send(msg.clone())
    }
}

impl Drop for ConnectionPool {
    fn drop(&mut self) {
        for (_, worker) in self.workers.drain() {
            worker.stop();
        }
    }
}

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
                msg.merge_from_bytes(&pb.as_bytes()).unwrap();
                tx.send((msg.msg_type, msg.to, msg.from)).unwrap();

                // Lock forever, never respond. This trick allows to check
                // how pool behaves in case of the irresponsive TCP connection.
                fiber::Cond::new().wait()
            }),
        );

        // Connect to the current Tarantool instance
        let mut pool = ConnectionPool::builder()
            .handler_name("test_interact")
            .call_timeout(Duration::from_millis(50))
            .connect_timeout(Duration::from_millis(50))
            .build();
        let listen: String = l.eval("return box.info.listen").unwrap();
        tlog!(Info, "TEST: connecting {listen}");
        pool.connect(1337, listen);

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
            Ok((raft::MessageType::MsgHeartbeat, 1337u64, 1u64))
        );

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
            Ok((raft::MessageType::MsgHeartbeat, 1337u64, 4u64))
        );
    }
});
