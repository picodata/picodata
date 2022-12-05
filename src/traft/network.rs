use ::raft::prelude as raft;
use ::tarantool::fiber;
use ::tarantool::net_box::promise::Promise;
use ::tarantool::net_box::promise::TryGet;
use ::tarantool::net_box::Conn;
use ::tarantool::net_box::ConnOptions;
use ::tarantool::tuple::Decode;
use ::tarantool::tuple::{RawByteBuf, ToTupleBuffer, TupleBuffer};
use ::tarantool::util::IntoClones;
use std::cell::Cell;
use std::collections::{hash_map::Entry, HashMap};
use std::io;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::mailbox::Mailbox;
use crate::storage::{instance_field, Clusterwide, Instances, PeerAddresses};
use crate::tlog;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::rpc::Request;
use crate::traft::Result;
use crate::traft::{InstanceId, RaftId};
use crate::unwrap_ok_or;
use crate::util::Either::{self, Left, Right};

#[derive(Clone, Debug)]
pub struct WorkerOptions {
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

type Callback = Box<dyn FnOnce(::tarantool::Result<RawByteBuf>)>;
type Queue = Mailbox<(Callback, &'static str, TupleBuffer)>;

pub struct PoolWorker {
    // Despite instances are usually identified by `instance_id` in
    // picodata, raft commutication relies on `raft_id`, so it is
    // primary for worker.
    raft_id: RaftId,
    // Store instance_id for the debugging purposes only.
    instance_id: Option<InstanceId>,
    inbox: Queue,
    fiber: fiber::UnitJoinHandle<'static>,
    cond: Rc<fiber::Cond>,
    stop_flag: Rc<Cell<bool>>,
    handler_name: &'static str,
}

impl PoolWorker {
    #[inline]
    pub fn run(
        raft_id: RaftId,
        instance_id: impl Into<Option<InstanceId>>,
        storage: PeerAddresses,
        opts: WorkerOptions,
    ) -> PoolWorker {
        let cond = Rc::new(fiber::Cond::new());
        let inbox = Mailbox::with_cond(cond.clone());
        let stop_flag = Rc::new(Cell::default());
        let handler_name = opts.handler_name;
        let instance_id = instance_id.into();
        let fiber = fiber::Builder::new()
            .name(
                instance_id
                    .as_ref()
                    .map(|instance_id| format!("to:{instance_id}"))
                    .unwrap_or_else(|| format!("to:raft:{raft_id}")),
            )
            .proc({
                let cond = cond.clone();
                let inbox = inbox.clone();
                let stop_flag = stop_flag.clone();
                move || Self::worker_loop(raft_id, storage, cond, inbox, stop_flag, &opts)
            })
            .start()
            .unwrap();

        Self {
            raft_id,
            instance_id,
            fiber,
            cond,
            inbox,
            stop_flag,
            handler_name,
        }
    }

    /// `cond` is a single shared conditional variable that can be signaled by
    /// `inbox`, `promises`, or [`Self::stop`].
    fn worker_loop(
        raft_id: RaftId,
        storage: PeerAddresses,
        cond: Rc<fiber::Cond>,
        inbox: Queue,
        stop_flag: Rc<Cell<bool>>,
        opts: &WorkerOptions,
    ) {
        struct ConnCache {
            address: String,
            conn: Conn,
        }
        let cache: Cell<Option<ConnCache>> = Cell::default();

        let mut promises: Vec<(Promise<RawByteBuf>, Callback, Instant)> = vec![];
        while !stop_flag.get() {
            let mut had_errors = false;
            let iter_start = Instant::now();
            let mut closest_promise_deadline = None;
            // Process existing promises
            promises = promises
                .into_iter()
                .filter_map(|(promise, callback, deadline)| match into_either(promise) {
                    Left(res) => {
                        // had_errors = true if res.is_err()
                        callback(res);
                        None
                    }
                    _ if deadline <= iter_start => {
                        had_errors = true;
                        callback(Err(From::from(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "response didn't arive in time",
                        ))));
                        None
                    }
                    Right(promise) => {
                        if &deadline < closest_promise_deadline.get_or_insert(deadline) {
                            closest_promise_deadline = Some(deadline);
                        }
                        Some((promise, callback, deadline))
                    }
                })
                .collect();

            let closest_promise_timeout_or = |timeout| {
                if let Some(closest_promise_deadline) = closest_promise_deadline {
                    closest_promise_deadline.saturating_duration_since(Instant::now())
                } else {
                    timeout
                }
            };

            // Make a connection, if not already connected
            let ConnCache { address, conn } = ::tarantool::unwrap_or!(cache.take(), {
                let address = crate::unwrap_ok_or!(storage.try_get(raft_id),
                    Err(e) => {
                        tlog!(Warning, "failed getting peer address: {e}";
                            "raft_id" => raft_id,
                        );
                        cond.wait_timeout(closest_promise_timeout_or(opts.connect_timeout));
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
                        let timeout = opts.connect_timeout.saturating_sub(iter_start.elapsed());
                        cond.wait_timeout(closest_promise_timeout_or(timeout));
                        continue
                    }
                );
                tlog!(Debug, "established connection to peer";
                    "peer" => &address,
                    "raft_id" => raft_id,
                );
                ConnCache { address, conn }
            });

            // Process incomming requests
            let requests = inbox.try_receive_all();
            let mut new_promises = false;
            for (callback, proc, req) in requests {
                let mut promise = unwrap_ok_or!(conn.call_async(proc, req),
                    Err(e) => {
                        tlog!(Debug, "failed sending request to peer: {e}";
                            "peer" => &address,
                            "proc" => proc,
                        );
                        callback(Err(e));
                        had_errors = true;
                        continue
                    }
                );
                // Put our `cond` into the promise,
                // so that we are signalled once the result is awailable
                promise.replace_cond(cond.clone());
                let deadline = Instant::now() + opts.call_timeout;
                promises.push((promise, callback, deadline));
                new_promises = true;
            }
            cache.set(Some(ConnCache { address, conn }));

            // Should check deadlines of new promises
            if new_promises {
                continue;
            }

            // There were some errors and there are no pending promises,
            // so we try reconnecting
            if had_errors && promises.is_empty() {
                let ConnCache { conn, address } = cache.take().expect("was just set");
                tlog!(Debug, "some requests to peer failed, will attempt to reconnect";
                    "peer" => address,
                    "raft_id" => raft_id,
                );
                drop(conn);
                cond.wait_timeout(opts.connect_timeout.saturating_sub(iter_start.elapsed()));
                continue;
            }

            // Wait for new activity (new requests or kept promises)
            if let Some(deadline) = closest_promise_deadline {
                cond.wait_timeout(deadline.saturating_duration_since(Instant::now()));
            } else {
                // We couldn't get here if there were new_promises
                // And if there were old promises
                // then closest_promise_deadline wouldn't be None
                assert!(promises.is_empty());
                let timed_out = !cond.wait_timeout(opts.inactivity_timeout);
                if timed_out {
                    // Connection has long been unused. Close it.
                    let ConnCache { conn, address } = cache.take().expect("was just set");
                    tlog!(Debug, "connection to peer has long been inactive";
                        "peer" => address,
                        "raft_id" => raft_id,
                    );
                    drop(conn);
                }
            }
        }

        // Notify the remaining waiters
        for (promise, callback, _) in promises {
            match into_either(promise) {
                Left(res) => callback(res),
                Right(_) => callback(Err(From::from(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "connection pool worker stopped",
                )))),
            }
        }
    }

    pub fn send(&self, msg: raft::Message) -> Result<()> {
        let raft_id = msg.to;
        let msg = traft::MessagePb::from(msg);
        let args = [msg].to_tuple_buffer()?;
        let on_result = move |res| match res {
            Ok(_) => (),
            Err(e) => tlog!(Debug, "error when sending message to peer: {e}";
                "raft_id" => raft_id,
            ),
        };
        self.inbox
            .send((Box::new(on_result), self.handler_name, args));
        Ok(())
    }

    /// Send an RPC `request` and invoke `cb` whenever the result is ready.
    ///
    /// An error will be passed to `cb` in one of the following situations:
    /// - in case `request` failed to serialize
    /// - in case peer was disconnected
    /// - in case response failed to deserialize
    /// - in case peer responded with an error
    pub fn rpc<R>(&mut self, request: R, cb: impl FnOnce(Result<R::Response>) + 'static)
    where
        R: Request,
    {
        let args = unwrap_ok_or!(request.to_tuple_buffer(),
            Err(e) => { return cb(Err(e.into())) }
        );
        let convert_result = |bytes| {
            let bytes: RawByteBuf = bytes?;
            let ((res,),) = Decode::decode(&bytes)?;
            Ok(res)
        };
        self.inbox.send((
            Box::new(move |res| cb(convert_result(res))),
            R::PROC_NAME,
            args,
        ));
    }

    fn stop(self) {
        self.stop_flag.set(true);
        self.cond.signal();
        self.fiber.join();
    }
}

impl std::fmt::Debug for PoolWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolWorker")
            .field("raft_id", &self.raft_id)
            .field("instance_id", &self.instance_id)
            .finish()
    }
}

fn into_either<T>(p: Promise<T>) -> Either<::tarantool::Result<T>, Promise<T>> {
    match p.try_get() {
        TryGet::Ok(res) => Left(Ok(res)),
        TryGet::Err(e) => Left(Err(e)),
        TryGet::Pending(p) => Right(p),
    }
}

////////////////////////////////////////////////////////////////////////////////
// ConnectionPoolBuilder
////////////////////////////////////////////////////////////////////////////////

pub struct ConnectionPoolBuilder {
    worker_options: WorkerOptions,
    storage: Clusterwide,
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
            raft_ids: HashMap::new(),
            peer_addresses: self.storage.peer_addresses,
            instances: self.storage.instances,
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
    raft_ids: HashMap<InstanceId, RaftId>,
    peer_addresses: PeerAddresses,
    instances: Instances,
}

impl ConnectionPool {
    pub fn builder(storage: Clusterwide) -> ConnectionPoolBuilder {
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

    fn get_or_create_by_raft_id(&mut self, raft_id: RaftId) -> Result<&mut PoolWorker> {
        match self.workers.entry(raft_id) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let instance_id = self
                    .instances
                    .field::<instance_field::InstanceId>(&raft_id)
                    .map_err(|_| Error::NoInstanceWithRaftId(raft_id))
                    .ok();
                // Check if address of this peer is known.
                // No need to store the result,
                // because it will be updated in the loop
                let _ = self.peer_addresses.try_get(raft_id)?;
                let worker = PoolWorker::run(
                    raft_id,
                    instance_id.clone(),
                    self.peer_addresses.clone(),
                    self.worker_options.clone(),
                );
                if let Some(instance_id) = instance_id {
                    self.raft_ids.insert(instance_id, raft_id);
                }
                Ok(entry.insert(worker))
            }
        }
    }

    fn get_or_create_by_instance_id(&mut self, instance_id: &str) -> Result<&mut PoolWorker> {
        match self.raft_ids.entry(InstanceId(instance_id.into())) {
            Entry::Occupied(entry) => {
                let worker = self
                    .workers
                    .get_mut(entry.get())
                    .expect("instance_id is present, but the worker isn't");
                Ok(worker)
            }
            Entry::Vacant(entry) => {
                let instance_id = entry.key();
                let raft_id = self
                    .instances
                    .field::<instance_field::RaftId>(instance_id)
                    .map_err(|_| Error::NoInstanceWithInstanceId(instance_id.clone()))?;
                let worker = PoolWorker::run(
                    raft_id,
                    instance_id.clone(),
                    self.peer_addresses.clone(),
                    self.worker_options.clone(),
                );
                entry.insert(raft_id);
                Ok(self.workers.entry(raft_id).or_insert(worker))
            }
        }
    }

    /// Send a message to `msg.to` asynchronously.
    /// If the massage can't be sent, it's a responsibility
    /// of the raft node to re-send it later.
    ///
    /// This function never yields.
    #[inline]
    pub fn send(&mut self, msg: raft::Message) -> Result<()> {
        self.get_or_create_by_raft_id(msg.to)?.send(msg)
    }

    /// Send a request to instance with `id` (see `IdOfInstance`) and wait for the result.
    ///
    /// If the request failed, it's a responsibility of the caller
    /// to re-send it later.
    ///
    /// **This function yields.**
    #[allow(dead_code)]
    pub fn call_and_wait_timeout<R>(
        &mut self,
        id: &impl IdOfInstance,
        req: R,
        timeout: Duration,
    ) -> Result<R::Response>
    where
        R: Request,
    {
        let (rx, tx) = fiber::Channel::new(1).into_clones();
        id.get_or_create_in(self)?
            .rpc(req, move |res| tx.send(res).unwrap());
        rx.recv_timeout(timeout).map_err(|_| Error::Timeout)?
    }

    /// Send a request to instance with `id` (see `InstanceId`) and wait for the result.
    ///
    /// If the request failed, it's a responsibility of the caller
    /// to re-send it later.
    ///
    /// **This function yields.**
    #[allow(dead_code)]
    #[inline(always)]
    pub fn call_and_wait<R>(&mut self, id: &impl IdOfInstance, req: R) -> Result<R::Response>
    where
        R: Request,
    {
        self.call_and_wait_timeout(id, req, Duration::MAX)
    }

    /// Send a request to instance with `id` (see `InstanceId`) and wait for the result.
    ///
    /// If the request failed, it's a responsibility of the caller
    /// to re-send it later.
    ///
    /// **This function never yields.**
    pub fn call<R>(
        &mut self,
        id: &impl IdOfInstance,
        req: R,
        cb: impl FnOnce(Result<R::Response>) + 'static,
    ) -> Result<()>
    where
        R: Request,
    {
        id.get_or_create_in(self)?.rpc(req, cb);
        Ok(())
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
// IdOfInstance
////////////////////////////////////////////////////////////////////////////////

/// Types implementing this trait can be used to identify a `Instance` when
/// accessing ConnectionPool.
pub trait IdOfInstance: std::hash::Hash + Clone + std::fmt::Debug {
    fn get_or_create_in<'p>(&self, pool: &'p mut ConnectionPool) -> Result<&'p mut PoolWorker>;
}

impl IdOfInstance for RaftId {
    #[inline(always)]
    fn get_or_create_in<'p>(&self, pool: &'p mut ConnectionPool) -> Result<&'p mut PoolWorker> {
        pool.get_or_create_by_raft_id(*self)
    }
}

impl IdOfInstance for InstanceId {
    #[inline(always)]
    fn get_or_create_in<'p>(&self, pool: &'p mut ConnectionPool) -> Result<&'p mut PoolWorker> {
        pool.get_or_create_by_instance_id(self)
    }
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

// TODO test connecting twice (reconnecting)
// thread 'main' panicked at 'UnitJoinHandle dropped before being joined',
// picodata::traft::network::ConnectionPool::connect

inventory::submit!(crate::InnerTest {
    name: "test_connection_pool",
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

        let storage = Clusterwide::new().unwrap();
        // Connect to the current Tarantool instance
        let mut pool = ConnectionPool::builder(storage.clone())
            .handler_name("test_interact")
            .call_timeout(Duration::from_millis(50))
            .connect_timeout(Duration::from_millis(50))
            .build();
        let listen: String = l.eval("return box.info.listen").unwrap();

        let instance = traft::Instance {
            raft_id: 1337,
            ..traft::Instance::default()
        };
        storage.instances.put(&instance).unwrap();
        storage
            .peer_addresses
            .put(instance.raft_id, &listen)
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
        assert_eq!(
            pool.send(heartbeat_to_from(9999, 3))
                .unwrap_err()
                .to_string(),
            "address of peer with id 9999 not found",
        );

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
