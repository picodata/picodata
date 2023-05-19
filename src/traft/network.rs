use ::raft::prelude as raft;
use ::tarantool::fiber;
use ::tarantool::fiber::r#async::oneshot;
use ::tarantool::fiber::r#async::timeout::IntoTimeout as _;
use ::tarantool::fiber::r#async::watch;
use ::tarantool::network;
use ::tarantool::network::AsClient as _;
use ::tarantool::network::ReconnClient;
use ::tarantool::tuple::{ToTupleBuffer, Tuple, TupleBuffer};
use ::tarantool::util::IntoClones;
use futures::future::poll_fn;
use futures::Future;
use futures::FutureExt as _;
use std::collections::VecDeque;
use std::collections::{hash_map::Entry, HashMap};
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;
use tarantool::fiber::r#async::timeout;

use crate::instance::InstanceId;
use crate::mailbox::Mailbox;
use crate::rpc;
use crate::storage::{instance_field, Clusterwide, Instances, PeerAddresses};
use crate::tlog;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::RaftId;
use crate::traft::Result;
use crate::unwrap_ok_or;

pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(3);
pub const DEFAULT_CUNCURRENT_FUTURES: usize = 10;

#[derive(Clone, Debug)]
pub struct WorkerOptions {
    handler_name: &'static str,
    call_timeout: Duration,
    max_concurrent_futs: usize,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            handler_name: "",
            call_timeout: DEFAULT_CALL_TIMEOUT,
            max_concurrent_futs: DEFAULT_CUNCURRENT_FUTURES,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// PoolWorker
////////////////////////////////////////////////////////////////////////////////

type Callback = Box<dyn FnOnce(Result<Option<Tuple>>)>;
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
    inbox_ready: watch::Sender<()>,
    stop: oneshot::Sender<()>,
    handler_name: &'static str,
}

impl PoolWorker {
    #[inline]
    pub fn run(
        raft_id: RaftId,
        instance_id: impl Into<Option<InstanceId>>,
        storage: PeerAddresses,
        opts: WorkerOptions,
    ) -> Result<PoolWorker> {
        let inbox = Mailbox::new();
        let (stop_sender, stop_receiver) = oneshot::channel();
        let (inbox_ready_sender, inbox_ready_receiver) = watch::channel(());
        let instance_id = instance_id.into();
        let full_address = storage.try_get(raft_id)?;
        let (address, port) = full_address
            .rsplit_once(':')
            .ok_or_else(|| Error::AddressParseFailure(full_address.clone()))?;
        let (address, port) = (address.to_owned(), port.to_owned());
        let port: u16 = port
            .parse()
            .map_err(|_| Error::AddressParseFailure(full_address.clone()))?;
        let fiber = fiber::Builder::new()
            .name(
                instance_id
                    .as_ref()
                    .map(|instance_id| format!("to:{instance_id}"))
                    .unwrap_or_else(|| format!("to:raft:{raft_id}")),
            )
            .proc_async({
                let inbox = inbox.clone();
                async move {
                    futures::select! {
                        _ = Self::worker_loop(
                                inbox,
                                inbox_ready_receiver,
                                address,
                                port,
                                opts.call_timeout,
                                opts.max_concurrent_futs
                            ).fuse() => (),
                        _ = stop_receiver.fuse() => ()
                    }
                }
            })
            .start()
            .unwrap();

        Ok(Self {
            raft_id,
            instance_id,
            fiber,
            inbox,
            inbox_ready: inbox_ready_sender,
            stop: stop_sender,
            handler_name: opts.handler_name,
        })
    }

    async fn worker_loop(
        inbox: Queue,
        mut inbox_ready: watch::Receiver<()>,
        address: String,
        port: u16,
        call_timeout: Duration,
        max_concurrent_fut: usize,
    ) {
        let client = ReconnClient::new(address.clone(), port);
        let mut client_ver: usize = 0;
        let mut futures = VecDeque::new();
        loop {
            let messages = inbox.try_receive_n(max_concurrent_fut - futures.len());
            // If there are no new messages and no messages are being sent - wait.
            if messages.is_empty() && futures.is_empty() {
                inbox_ready
                    .changed()
                    .await
                    .expect("sender cannot be dropped at this point");
                continue;
            }

            // Generate futures for new messages.
            for (callback, proc, request) in messages {
                let client = client.clone();
                futures.push_back((
                    client_ver,
                    callback,
                    Box::pin(
                        async move { client.call(proc, &request).timeout(call_timeout).await },
                    ),
                ));
            }

            // Poll all futures until at least one completes, remove the ones which completed.
            // If there were errors `highest_client_ver` will contain the highest client version that had errors.
            let mut highest_client_ver = None;
            poll_fn(|cx| {
                let mut has_ready: bool = false;
                let mut cursor = 0;
                while cursor < futures.len() {
                    let poll_result = Future::poll(futures[cursor].2.as_mut(), cx);
                    if let Poll::Ready(result) = poll_result {
                        let (client_ver, callback, _) = futures.remove(cursor).unwrap();
                        if let Err(timeout::Error::Failed(network::Error::Protocol(_))) | Ok(_) =
                            result
                        {
                        } else {
                            match highest_client_ver {
                                Some(ref mut ver) => {
                                    if client_ver > *ver {
                                        *ver = client_ver
                                    }
                                }
                                None => highest_client_ver = Some(client_ver),
                            }
                        }
                        callback(result.map_err(Error::from));
                        has_ready = true;
                    } else {
                        cursor += 1;
                    }
                }
                if has_ready {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;

            // Reconnect if there were errors and the future which completed with error used the latest client version.
            if let Some(ver) = highest_client_ver {
                if ver >= client_ver {
                    client.reconnect();
                    client_ver = client_ver.wrapping_add(1);
                    tlog!(Debug, "reconnecting to {address}:{port}"; "client_version" => client_ver);
                }
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
        if self.inbox_ready.send(()).is_err() {
            tlog!(Warning, "failed sending request to peer, worker loop receiver dropped";
                "raft_id" => raft_id,
            );
        }
        Ok(())
    }

    /// Send an RPC `request` and invoke `cb` whenever the result is ready.
    ///
    /// An error will be passed to `cb` in one of the following situations:
    /// - in case `request` failed to serialize
    /// - in case peer was disconnected
    /// - in case response failed to deserialize
    /// - in case peer responded with an error
    pub fn rpc<R>(&mut self, request: &R, cb: impl FnOnce(Result<R::Response>) + 'static)
    where
        R: rpc::Request,
    {
        let args = unwrap_ok_or!(request.to_tuple_buffer(),
            Err(e) => { return cb(Err(e.into())) }
        );
        let convert_result = |bytes: Result<Option<Tuple>>| {
            let tuple: Tuple = bytes?.ok_or(Error::EmptyRpcAnswer)?;
            let ((res,),) = tuple.decode()?;
            Ok(res)
        };
        self.inbox.send((
            Box::new(move |res| cb(convert_result(res))),
            R::PROC_NAME,
            args,
        ));
        if self.inbox_ready.send(()).is_err() {
            tlog!(
                Warning,
                "failed sending request to peer, worker loop receiver dropped"
            );
        }
    }

    fn stop(self) {
        let _ = self.stop.send(());
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
    builder_option!(max_concurrent_futs, usize);

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

// TODO: restart worker on `PeerAddress` changes
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
        todo!();
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
                )?;
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
                )?;
                entry.insert(raft_id);
                Ok(self.workers.entry(raft_id).or_insert(worker))
            }
        }
    }

    /// Send a message to `msg.to` asynchronously. If the massage can't
    /// be sent, it's a responsibility of the raft node to re-send it
    /// later.
    ///
    /// Calling this function may result in **fiber rescheduling**, so
    /// it's not appropriate for use inside a transaction. Anyway,
    /// sending a message inside a transaction is always a bad idea.
    #[inline]
    pub fn send(&mut self, msg: raft::Message) -> Result<()> {
        self.get_or_create_by_raft_id(msg.to)?.send(msg)
    }

    /// Send a request to instance with `id` (see `IdOfInstance`) returning a
    /// future.
    ///
    /// If the request failed, it's a responsibility of the caller
    /// to re-send it later.
    pub fn call<R>(
        &mut self,
        id: &impl IdOfInstance,
        req: &R,
    ) -> Result<impl Future<Output = Result<R::Response>>>
    where
        R: rpc::Request,
    {
        let (tx, mut rx) = oneshot::channel();
        let id_dbg = format!("{id:?}");
        id.get_or_create_in(self)?.rpc(req, move |res| {
            if tx.send(res).is_err() {
                tlog!(
                    Warning,
                    "skipping call: pool worker receiver dropped for id: {id_dbg}"
                )
            }
        });

        // We use an explicit type implementing Future instead of defining an
        // async fn, because we need to tell rust explicitly that the `id` &
        // `req` arguments are not borrowed by the returned future.
        let f = poll_fn(move |cx| {
            let rx = Pin::new(&mut rx);
            Future::poll(rx, cx).map(|r| r.unwrap_or_else(|_| Err(Error::other("disconnected"))))
        });
        Ok(f)
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

mod tests {
    use super::*;
    use std::rc::Rc;
    use tarantool::fiber;
    use tarantool::tlua;

    fn heartbeat_to_from(to: RaftId, from: RaftId) -> raft::Message {
        raft::Message {
            msg_type: raft::MessageType::MsgHeartbeat,
            to,
            from,
            ..Default::default()
        }
    }

    #[::tarantool::test]
    fn unresponsive_connection() {
        use tarantool::fiber::YieldResult::*;

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

        // Send a request
        assert_eq!(
            fiber::check_yield(|| pool.send(heartbeat_to_from(1337, 1)).unwrap()),
            Yielded(()) // because no worker exists so a fiber is started.
        );

        // Assert it arrives
        // Assert equality
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1)),
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
            .wait_timeout(Duration::from_secs(1))
            .then(|| (tlog!(Info, "TEST: on_disconnect triggered")))
            .or_else(|| panic!("on_disconnect timed out"));

        // Send the second request
        assert_eq!(
            fiber::check_yield(|| pool.send(heartbeat_to_from(1337, 4)).unwrap()),
            DidntYield(()) // because the worker already exists
        );

        // Gets the latest message
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(1)),
            Ok((raft::MessageType::MsgHeartbeat, 1337u64, 4u64))
        );
    }

    #[::tarantool::test]
    fn multiple_messages() {
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
            }),
        );

        let storage = Clusterwide::new().unwrap();
        // Connect to the current Tarantool instance
        let mut pool = ConnectionPool::builder(storage.clone())
            .handler_name("test_interact")
            .call_timeout(Duration::from_millis(50))
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

        // Send several messages one by one
        for i in 0..10 {
            // Send a request
            pool.send(heartbeat_to_from(1337, i)).unwrap();

            // Assert it arrives
            // Assert equality
            assert_eq!(
                rx.recv_timeout(Duration::from_secs(1)),
                Ok((raft::MessageType::MsgHeartbeat, 1337u64, i))
            );
        }

        // Send multiple messages concurrently
        // Send first batch
        for i in 0..10 {
            pool.send(heartbeat_to_from(1337, i)).unwrap();
        }
        for i in 0..10 {
            assert_eq!(
                rx.recv_timeout(Duration::from_secs(1)),
                Ok((raft::MessageType::MsgHeartbeat, 1337u64, i))
            );
        }
        // Send second batch
        for i in 10..20 {
            pool.send(heartbeat_to_from(1337, i)).unwrap();
        }
        for i in 10..20 {
            assert_eq!(
                rx.recv_timeout(Duration::from_secs(1)),
                Ok((raft::MessageType::MsgHeartbeat, 1337u64, i))
            );
        }
    }

    #[cfg(feature = "load_test")]
    #[::tarantool::test]
    fn high_load() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::Instant;

        let l = tarantool::lua_state();

        // Mock the handler
        let counter = Rc::new(AtomicU64::new(0));
        let counter_moved = counter.clone();
        l.set(
            "test_interact",
            tlua::function1(move |pb: tlua::AnyLuaString| {
                use protobuf::Message as _;
                let mut msg = raft::Message::default();
                msg.merge_from_bytes(pb.as_bytes()).unwrap();
                counter_moved.fetch_add(1, Ordering::Relaxed);
            }),
        );

        let storage = Clusterwide::new().unwrap();
        // Connect to the current Tarantool instance
        let mut pool = ConnectionPool::builder(storage.clone())
            .handler_name("test_interact")
            .call_timeout(Duration::from_millis(3000))
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

        let timeout = Duration::from_secs(10);
        let mut expected = 0;
        // Up to 32768 batch
        for batch_exp in 0..15 {
            let batch_size = 2_u64.pow(batch_exp);
            for i in 0..batch_size {
                pool.send(heartbeat_to_from(1337, i)).unwrap();
            }
            expected += batch_size;
            let start = Instant::now();
            while counter.load(Ordering::Relaxed) != expected {
                if Instant::now().duration_since(start) > timeout {
                    panic!("Max batch is {batch_size} with timeout {timeout:?}");
                }
                fiber::r#yield().unwrap();
            }
        }
    }
}
