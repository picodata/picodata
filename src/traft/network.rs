use crate::instance::Instance;
use crate::instance::InstanceId;
use crate::mailbox::Mailbox;
use crate::pico_service::pico_service_password;
use crate::reachability::InstanceReachabilityManagerRef;
use crate::rpc;
use crate::schema::PICO_SERVICE_USER_NAME;
use crate::storage::{Clusterwide, Instances, PeerAddresses};
use crate::tlog;
use crate::traft;
use crate::traft::error::Error;
use crate::traft::RaftId;
use crate::traft::Result;
use crate::unwrap_ok_or;
#[cfg(debug_assertions)]
use crate::util::NoYieldsGuard;
use ::raft::prelude as raft;
use ::tarantool::fiber;
use ::tarantool::fiber::r#async::oneshot;
use ::tarantool::fiber::r#async::timeout::Error as TOError;
use ::tarantool::fiber::r#async::timeout::IntoTimeout as _;
use ::tarantool::fiber::r#async::watch;
use ::tarantool::network::AsClient as _;
use ::tarantool::network::ClientError;
use ::tarantool::network::Config;
use ::tarantool::network::ReconnClient;
use ::tarantool::tuple::{ToTupleBuffer, Tuple, TupleBuffer};
use ::tarantool::util::IntoClones;
use futures::future::poll_fn;
use futures::Future;
use futures::FutureExt as _;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

pub const DEFAULT_CALL_TIMEOUT: Duration = Duration::from_secs(3);
pub const DEFAULT_CUNCURRENT_FUTURES: usize = 10;

#[derive(Clone, Debug)]
pub struct WorkerOptions {
    pub raft_msg_handler: &'static str,
    pub call_timeout: Duration,
    pub max_concurrent_futs: usize,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            raft_msg_handler: crate::proc_name!(crate::traft::node::proc_raft_interact),
            call_timeout: DEFAULT_CALL_TIMEOUT,
            max_concurrent_futs: DEFAULT_CUNCURRENT_FUTURES,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// PoolWorker
////////////////////////////////////////////////////////////////////////////////

struct Request {
    proc: &'static str,
    args: TupleBuffer,
    timeout: Option<Duration>,
    on_result: OnRequestResult,
}

impl Request {
    #[inline(always)]
    fn with_callback<H>(proc: &'static str, args: TupleBuffer, on_result: H) -> Self
    where
        H: FnOnce(Result<Tuple>) + 'static,
    {
        Self {
            proc,
            args,
            timeout: None,
            on_result: OnRequestResult::Callback(Box::new(on_result)),
        }
    }

    #[inline(always)]
    fn raft_msg(proc: &'static str, args: TupleBuffer) -> Self {
        Self {
            proc,
            args,
            timeout: None,
            on_result: OnRequestResult::ReportUnreachable,
        }
    }
}

enum OnRequestResult {
    Callback(Box<dyn FnOnce(Result<Tuple>)>),
    ReportUnreachable,
}

type Queue = Mailbox<Request>;

pub struct PoolWorker {
    // Despite instances are usually identified by `instance_id` in
    // picodata, raft commutication relies on `raft_id`, so it is
    // primary for worker.
    raft_id: RaftId,
    // Store instance_id for the debugging purposes only.
    instance_id: Option<InstanceId>,
    inbox: Queue,
    fiber: fiber::JoinHandle<'static, ()>,
    inbox_ready: watch::Sender<()>,
    stop: oneshot::Sender<()>,

    /// Stored proc name which is called to pass raft messages between nodes.
    /// This should always be ".proc_raft_interact".
    ///
    /// The only reason this is a parameter at all is because it is used in a
    /// couple of unit tests, which is stupid and we should probably fix this.
    raft_msg_handler: &'static str,
}

impl PoolWorker {
    #[inline]
    pub fn run(
        raft_id: RaftId,
        instance_id: impl Into<Option<InstanceId>>,
        storage: PeerAddresses,
        opts: WorkerOptions,
        instance_reachability: InstanceReachabilityManagerRef,
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
            .func_async({
                let inbox = inbox.clone();
                async move {
                    futures::select! {
                        _ = Self::worker_loop(
                                raft_id,
                                inbox,
                                inbox_ready_receiver,
                                address,
                                port,
                                opts.call_timeout,
                                opts.max_concurrent_futs,
                                instance_reachability
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
            raft_msg_handler: opts.raft_msg_handler,
        })
    }

    async fn worker_loop(
        raft_id: RaftId,
        inbox: Queue,
        mut inbox_ready: watch::Receiver<()>,
        address: String,
        port: u16,
        call_timeout: Duration,
        max_concurrent_fut: usize,
        instance_reachability: InstanceReachabilityManagerRef,
    ) {
        let mut config = Config::default();
        config.creds = Some((
            PICO_SERVICE_USER_NAME.into(),
            pico_service_password().into(),
        ));
        let client = ReconnClient::with_config(address.clone(), port, config);

        let mut client_ver: usize = 0;
        let mut futures = VecDeque::new();
        loop {
            let requests = inbox.try_receive_n(max_concurrent_fut - futures.len());
            // If there are no new requests and no requests are being sent - wait.
            if requests.is_empty() && futures.is_empty() {
                inbox_ready
                    .changed()
                    .await
                    .expect("sender cannot be dropped at this point");
                continue;
            }

            // Generate futures for new requests.
            for request in requests {
                let client = client.clone();
                futures.push_back((
                    client_ver,
                    request.on_result,
                    Box::pin(async move {
                        client
                            .call(request.proc, &request.args)
                            // TODO: it would be better to get a deadline from
                            // the caller instead of the timeout, so we can more
                            // accurately limit the time of the given rpc request.
                            .timeout(request.timeout.unwrap_or(call_timeout))
                            .await
                    }),
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
                        let (client_ver, on_result, _) = futures.remove(cursor).unwrap();

                        let is_connected;
                        match result {
                            Ok(_)
                            | Err(TOError::Failed(ClientError::ErrorResponse(_)))
                            | Err(TOError::Failed(ClientError::RequestEncode(_)))
                            | Err(TOError::Failed(ClientError::ResponseDecode(_))) => {
                                is_connected = true;
                            }
                            Err(TOError::Failed(ClientError::ConnectionClosed(_)))
                            | Err(TOError::Expired) => {
                                is_connected = false;
                            }
                        }

                        if !is_connected {
                            match highest_client_ver {
                                Some(ref mut ver) => {
                                    if client_ver > *ver {
                                        *ver = client_ver
                                    }
                                }
                                None => highest_client_ver = Some(client_ver),
                            }
                        }

                        match on_result {
                            OnRequestResult::Callback(cb) => {
                                cb(result.map_err(Error::from));
                            }
                            OnRequestResult::ReportUnreachable => {
                                if let Err(e) = result {
                                    tlog!(Warning, "error when sending message to peer: {e}";
                                        "raft_id" => raft_id,
                                    );
                                }
                                instance_reachability
                                    .borrow_mut()
                                    .report_result(raft_id, is_connected);
                            }
                        }
                        has_ready = true;
                    } else {
                        cursor += 1;
                    }
                }
                if has_ready {
                    Poll::Ready(())
                } else {
                    // Must check if there's something in the inbox (actually
                    // it's more of an outbox, you put stuff in it, which you
                    // want to be sent to someone else).
                    let mut f = inbox_ready.changed();
                    // Don't you just love async rust!
                    f.poll_unpin(cx).map(|_| ())
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
        self.inbox
            .send(Request::raft_msg(self.raft_msg_handler, args));
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
    #[inline(always)]
    pub fn rpc<R>(
        &self,
        request: &R,
        timeout: Option<Duration>,
        cb: impl FnOnce(Result<R::Response>) + 'static,
    ) where
        R: rpc::RequestArgs,
    {
        self.rpc_raw(R::PROC_NAME, request, timeout, cb)
    }

    /// Send an RPC `request` and invoke `cb` whenever the result is ready.
    ///
    /// An error will be passed to `cb` in one of the following situations:
    /// - in case `args` failed to serialize
    /// - in case peer was disconnected
    /// - in case response failed to deserialize
    /// - in case peer responded with an error
    pub fn rpc_raw<Args, Response>(
        &self,
        proc: &'static str,
        args: &Args,
        timeout: Option<Duration>,
        cb: impl FnOnce(Result<Response>) + 'static,
    ) where
        Args: ToTupleBuffer + ?Sized,
        Response: tarantool::tuple::DecodeOwned,
    {
        let args = unwrap_ok_or!(args.to_tuple_buffer(),
            Err(e) => { return cb(Err(e.into())) }
        );
        let convert_result = |bytes: Result<Tuple>| {
            let tuple: Tuple = bytes?;
            let res = crate::rpc::decode_iproto_return_value(tuple)?;
            Ok(res)
        };
        let mut request = Request::with_callback(proc, args, move |res| cb(convert_result(res)));
        request.timeout = timeout;
        self.inbox.send(request);
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
// ConnectionPool
////////////////////////////////////////////////////////////////////////////////

// TODO: restart worker on `PeerAddress` changes
#[derive(Debug)]
pub struct ConnectionPool {
    worker_options: WorkerOptions,
    workers: UnsafeCell<HashMap<RaftId, PoolWorker>>,
    raft_ids: UnsafeCell<HashMap<InstanceId, RaftId>>,
    peer_addresses: PeerAddresses,
    instances: Instances,
    pub(crate) instance_reachability: InstanceReachabilityManagerRef,
}

impl ConnectionPool {
    #[inline(always)]
    pub fn new(storage: Clusterwide, worker_options: WorkerOptions) -> Self {
        Self {
            worker_options,
            workers: Default::default(),
            raft_ids: Default::default(),
            peer_addresses: storage.peer_addresses,
            instances: storage.instances,
            instance_reachability: Default::default(),
        }
    }

    #[allow(dead_code)]
    #[allow(unused_variables)]
    pub fn disconnect(&mut self, id: RaftId) {
        todo!();
    }

    fn get_or_create_by_raft_id(&self, raft_id: RaftId) -> Result<&PoolWorker> {
        // TODO(gmoshkin): Check if there's a possible race here. Put a comment
        // here that there isn't one when done, or write a good test.
        // SAFETY: shared state mutations in this function are guarded by no yield guards
        // which makes them safe in context of tx thread.
        {
            // We're mutating shared state here which may lead to errors
            // if we yield in an inapropriate moment.
            #[cfg(debug_assertions)]
            let _guard = NoYieldsGuard::new();

            let workers = unsafe { &*self.workers.get() };
            if let Some(worker) = workers.get(&raft_id) {
                return Ok(worker);
            }
        }

        let mut instance_id: Option<InstanceId> = None;
        if let Ok(tuple) = self.instances.get_raw(&raft_id) {
            instance_id = tuple.field(Instance::FIELD_INSTANCE_ID)?;
        }
        // Check if address of this peer is known.
        // No need to store the result,
        // because it will be updated in the loop
        let _ = self.peer_addresses.try_get(raft_id)?;
        let worker = PoolWorker::run(
            raft_id,
            instance_id.clone(),
            self.peer_addresses.clone(),
            self.worker_options.clone(),
            self.instance_reachability.clone(),
        )?;

        {
            // We're mutating shared state here which may lead to errors
            // if we yield in an inapropriate moment.
            #[cfg(debug_assertions)]
            let _guard = NoYieldsGuard::new();

            if let Some(instance_id) = instance_id {
                let raft_ids = unsafe { &mut *self.raft_ids.get() };
                raft_ids.insert(instance_id, raft_id);
            }

            let workers = unsafe { &mut *self.workers.get() };
            Ok(workers.entry(raft_id).or_insert(worker))
        }
    }

    fn get_or_create_by_instance_id(&self, instance_id: &str) -> Result<&PoolWorker> {
        // SAFETY: shared state mutations in this function are guarded by no yield guards
        // which makes them safe in context of tx thread.
        {
            // We're mutating shared state here which may lead to errors
            // if we yield in an inapropriate moment.
            #[cfg(debug_assertions)]
            let _guard = NoYieldsGuard::new();

            let raft_ids = unsafe { &*self.raft_ids.get() };
            if let Some(raft_id) = raft_ids.get(instance_id) {
                let workers = unsafe { &*self.workers.get() };
                let worker = workers
                    .get(raft_id)
                    .expect("instance_id is present, but the worker isn't");
                return Ok(worker);
            }
        }

        let instance_id = InstanceId::from(instance_id);
        let tuple = self.instances.get_raw(&instance_id)?;
        let Some(raft_id) = tuple.field(Instance::FIELD_RAFT_ID)? else {
            #[rustfmt::skip]
            return Err(Error::other("storage corrupted: couldn't decode instance's raft id"));
        };
        self.get_or_create_by_raft_id(raft_id)
    }

    /// Send a message to `msg.to` asynchronously. If the massage can't
    /// be sent, it's a responsibility of the raft node to re-send it
    /// later.
    ///
    /// Calling this function may result in **fiber rescheduling**, so
    /// it's not appropriate for use inside a transaction. Anyway,
    /// sending a message inside a transaction is always a bad idea.
    #[inline]
    pub fn send(&self, msg: raft::Message) -> Result<()> {
        self.get_or_create_by_raft_id(msg.to)?.send(msg)
    }

    /// Send a request to instance with `id` (see `IdOfInstance`) returning a
    /// future.
    ///
    /// If `timeout` is None, the `WorkerOptions::call_timeout` is used.
    ///
    /// If the request failed, it's a responsibility of the caller
    /// to re-send it later.
    #[inline(always)]
    pub fn call<R>(
        &self,
        id: &impl IdOfInstance,
        req: &R,
        timeout: impl Into<Option<Duration>>,
    ) -> Result<impl Future<Output = Result<R::Response>>>
    where
        R: rpc::RequestArgs,
    {
        self.call_raw(id, R::PROC_NAME, req, timeout.into())
    }

    /// Call an rpc on instance with `id` (see `IdOfInstance`) returning a
    /// future.
    ///
    /// This method is similar to [`Self::call`] but allows to call rpcs
    /// without using [`crate::rpc::RequestArgs`] trait.
    ///
    /// If `timeout` is None, the `WorkerOptions::call_timeout` is used.
    ///
    /// If the request failed, it's a responsibility of the caller
    /// to re-send it later.
    pub fn call_raw<Args, Response>(
        &self,
        id: &impl IdOfInstance,
        proc: &'static str,
        args: &Args,
        timeout: Option<Duration>,
    ) -> Result<impl Future<Output = Result<Response>>>
    where
        Response: tarantool::tuple::DecodeOwned + 'static,
        Args: ToTupleBuffer + ?Sized,
    {
        let (tx, mut rx) = oneshot::channel();
        id.get_or_create_in(self)?
            .rpc_raw(proc, args, timeout, move |res| {
                if tx.send(res).is_err() {
                    tlog!(
                        Debug,
                        "rpc response ignored because caller dropped the future"
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
        for (_, worker) in self.workers.get_mut().drain() {
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
    fn get_or_create_in<'p>(&self, pool: &'p ConnectionPool) -> Result<&'p PoolWorker>;
}

impl IdOfInstance for RaftId {
    #[inline(always)]
    fn get_or_create_in<'p>(&self, pool: &'p ConnectionPool) -> Result<&'p PoolWorker> {
        pool.get_or_create_by_raft_id(*self)
    }
}

impl IdOfInstance for InstanceId {
    #[inline(always)]
    fn get_or_create_in<'p>(&self, pool: &'p ConnectionPool) -> Result<&'p PoolWorker> {
        pool.get_or_create_by_instance_id(self)
    }
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

// TODO test connecting twice (reconnecting)
// thread 'main' panicked at 'JoinHandle dropped before being joined',
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
    fn call_raw() {
        let l = tarantool::lua_state();
        l.exec(
            r#"
            function test_stored_proc(a, b)
                return a + b
            end

            box.schema.func.create('test_stored_proc')
            "#,
        )
        .unwrap();

        let storage = Clusterwide::for_tests();
        // Connect to the current Tarantool instance
        let pool = ConnectionPool::new(storage.clone(), Default::default());
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

        let result: u32 = fiber::block_on(
            pool.call_raw(&instance.raft_id, "test_stored_proc", &(1u32, 2u32), None)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(result, 3u32);
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

        let storage = Clusterwide::for_tests();
        // Connect to the current Tarantool instance
        let opts = WorkerOptions {
            raft_msg_handler: "test_interact",
            call_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let pool = ConnectionPool::new(storage.clone(), opts);
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

        let storage = Clusterwide::for_tests();
        // Connect to the current Tarantool instance
        let opts = WorkerOptions {
            raft_msg_handler: "test_interact",
            call_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let pool = ConnectionPool::new(storage.clone(), opts);
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

        let storage = Clusterwide::for_tests();
        // Connect to the current Tarantool instance
        let opts = WorkerOptions {
            raft_msg_handler: "test_interact",
            call_timeout: Duration::from_secs(3),
            ..Default::default()
        };
        let pool = ConnectionPool::new(storage.clone(), opts);
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
