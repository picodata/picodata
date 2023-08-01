use ::raft::prelude as raft;
use ::tarantool::fiber;
use ::tarantool::fiber::r#async::oneshot;
use ::tarantool::fiber::r#async::timeout::Error as TOError;
use ::tarantool::fiber::r#async::timeout::IntoTimeout as _;
use ::tarantool::fiber::r#async::watch;
use ::tarantool::network;
use ::tarantool::network::AsClient as _;
use ::tarantool::network::Error as NetError;
use ::tarantool::network::ReconnClient;
use ::tarantool::tuple::{ToTupleBuffer, Tuple, TupleBuffer};
use ::tarantool::util::IntoClones;
use futures::future::poll_fn;
use futures::Future;
use futures::FutureExt as _;
use std::cell::RefCell;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;
use tarantool::fiber::r#async::timeout;
use tarantool::time::Instant;

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
    pub raft_msg_handler: &'static str,
    pub call_timeout: Duration,
    pub max_concurrent_futs: usize,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            raft_msg_handler: crate::stringify_cfunc!(crate::traft::node::proc_raft_interact),
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
    fiber: fiber::UnitJoinHandle<'static>,
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
            .proc_async({
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
        let client = ReconnClient::new(address.clone(), port);
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
                // NOTE: must not yield until this is dropped.
                let mut reachability = instance_reachability.borrow_mut();
                while cursor < futures.len() {
                    let poll_result = Future::poll(futures[cursor].2.as_mut(), cx);
                    if let Poll::Ready(result) = poll_result {
                        let (client_ver, on_result, _) = futures.remove(cursor).unwrap();
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
                        match on_result {
                            OnRequestResult::Callback(cb) => {
                                cb(result.map_err(Error::from));
                            }
                            OnRequestResult::ReportUnreachable => {
                                let mut success = true;
                                if let Err(e) = result {
                                    tlog!(Warning, "error when sending message to peer: {e}";
                                        "raft_id" => raft_id,
                                    );
                                    success = !matches!(
                                        e,
                                        TOError::Expired
                                            | TOError::Failed(NetError::Tcp(_))
                                            | TOError::Failed(NetError::Io(_))
                                    );
                                }
                                reachability.report_result(raft_id, success);
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
                    Poll::Pending
                }
            })
            .await;

            // Reconnect if there were errors and the future which completed with error used the latest client version.
            if let Some(ver) = highest_client_ver {
                if ver >= client_ver {
                    client.reconnect();
                    client_ver = client_ver.wrapping_add(1);
                    tlog!(Warning, "reconnecting to {address}:{port}"; "client_version" => client_ver);
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
        Args: ToTupleBuffer,
        Response: serde::de::DeserializeOwned,
    {
        let args = unwrap_ok_or!(args.to_tuple_buffer(),
            Err(e) => { return cb(Err(e.into())) }
        );
        let convert_result = |bytes: Result<Tuple>| {
            let tuple: Tuple = bytes?;
            // NOTE: this double layer of single element tuple here is
            // intentional. The thing is that tarantool wraps all the returned
            // values from stored procs into an msgpack array. This is true for
            // both native and lua procs. However in case of lua this outermost
            // array helps with multiple return values such that if a lua proc
            // returns 2 values, the messagepack representation would be an
            // array of 2 elements. But for some reason native procs get a
            // different treatment: their return values are always wrapped in an
            // outermost array of one element (!) which contains an array whose
            // elements are actual values returned by proc. To be exact, each
            // element of this inner array is a value passed to box_return_mp
            // therefore there's exactly as many elements as there were calls to
            // box_return_mp during execution of the proc.
            // The way #[tarantool::proc] is implemented we always call
            // box_return_mp exactly once therefore procs defined such way
            // always have their return values wrapped in 2 layers of one
            // element arrays.
            // For that reason we unwrap those 2 layers here and pass to the
            // user just the value they returned from their #[tarantool::proc].
            let ((res,),) = tuple.decode()?;
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
// InstanceReachabilityManager
////////////////////////////////////////////////////////////////////////////////

/// A struct holding information about reported attempts to communicate with
/// all known instances.
#[derive(Debug, Default)]
pub struct InstanceReachabilityManager {
    // TODO: Will be used to read configuration from
    #[allow(unused)]
    storage: Option<Clusterwide>,
    infos: HashMap<RaftId, InstanceReachabilityInfo>,
}

pub type InstanceReachabilityManagerRef = Rc<RefCell<InstanceReachabilityManager>>;

impl InstanceReachabilityManager {
    // TODO: make these configurable via _pico_property
    const MAX_HEARTBEAT_PERIOD: Duration = Duration::from_secs(5);

    pub fn new(storage: Clusterwide) -> Self {
        Self {
            storage: Some(storage),
            infos: Default::default(),
        }
    }

    /// Is called from a connection pool worker loop to report results of raft
    /// messages sent to other instances. Updates info for the given instance.
    pub fn report_result(&mut self, raft_id: RaftId, success: bool) {
        let now = fiber::clock();
        let info = self.infos.entry(raft_id).or_insert_with(Default::default);
        info.last_attempt = Some(now);

        // Even if it was previously reported as unreachable another message was
        // sent to it, so raft node state may have changed and another
        // report_unreachable me be needed.
        info.is_reported = false;

        if success {
            info.last_success = Some(now);
            info.fail_streak = 0;
            // If was previously reported as unreachable, it's now reachable so
            // next time it should again be reported as unreachable.
        } else {
            info.fail_streak += 1;
        }
    }

    /// Is called at the beginning of the raft main loop to get information
    /// about which instances should be reported as unreachable to the raft node.
    pub fn take_unreachables_to_report(&mut self) -> Vec<RaftId> {
        let mut res = Vec::with_capacity(16);
        for (raft_id, info) in &mut self.infos {
            if info.last_success.is_none() {
                // Don't report nodes which didn't previously respond once,
                // so that they can safely boot atleast.
                continue;
            }
            if info.is_reported {
                // Don't report nodes repeatedly.
                continue;
            }
            // TODO: add configurable parameters, for example number of attempts
            // before report.
            if info.fail_streak > 0 {
                res.push(*raft_id);
                info.is_reported = true;
            }
        }
        res
    }

    /// Is called from raft main loop when handling raft messages, passing a
    /// raft id of an instance which was previously determined to be unreachable.
    /// This function makes a decision about how often raft hearbeat messages
    /// should be sent to such instances.
    pub fn should_send_heartbeat_this_tick(&self, to: RaftId) -> bool {
        let Some(info) = self.infos.get(&to) else {
            // No attempts were registered yet.
            return true;
        };

        if info.fail_streak == 0 {
            // Last attempt was successful, keep going.
            return true;
        }

        let Some(last_success) = info.last_success else {
            // Didn't succeed once, keep trying.
            return true;
        };

        let last_attempt = info
            .last_attempt
            .expect("this should be set if info was reported");

        // Expontential decay.
        // time: -----*---------*---------*-------------------*---------------->
        //            ^         ^         ^                   ^
        // last_success         attempt1  attempt2            attempt3   ...
        //
        //            |<------->|<------->|
        //                D1         D1
        //            |<----------------->|<----------------->|
        //                     D2                   D2
        //            |<------------------------------------->|
        //                                D3
        //                                ...
        // DN == attemptN.duration_since(last_success)
        //
        let now = fiber::clock();
        let wait_timeout = last_attempt.duration_since(last_success).min(Self::MAX_HEARTBEAT_PERIOD);
        if now > last_attempt + wait_timeout {
            return true;
        }

        return false;
    }
}

/// Information about recent attempts to communicate with a single given instance.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InstanceReachabilityInfo {
    pub last_success: Option<Instant>,
    pub last_attempt: Option<Instant>,
    pub fail_streak: u32,
    pub is_reported: bool,
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
        // SAFETY: everything here is safe, because we only use ConnectionPool
        // from tx thread
        let workers = unsafe { &*self.workers.get() };
        if let Some(worker) = workers.get(&raft_id) {
            Ok(worker)
        } else {
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
                self.instance_reachability.clone(),
            )?;
            if let Some(instance_id) = instance_id {
                let raft_ids = unsafe { &mut *self.raft_ids.get() };
                raft_ids.insert(instance_id, raft_id);
            }

            let workers = unsafe { &mut *self.workers.get() };
            Ok(workers.entry(raft_id).or_insert(worker))
        }
    }

    fn get_or_create_by_instance_id(&self, instance_id: &str) -> Result<&PoolWorker> {
        // SAFETY: everything here is safe, because we only use ConnectionPool
        // from tx thread
        let raft_ids = unsafe { &*self.raft_ids.get() };
        if let Some(raft_id) = raft_ids.get(instance_id) {
            let workers = unsafe { &*self.workers.get() };
            let worker = workers
                .get(raft_id)
                .expect("instance_id is present, but the worker isn't");
            Ok(worker)
        } else {
            let instance_id = InstanceId::from(instance_id);
            let raft_id = self
                .instances
                .field::<instance_field::RaftId>(&instance_id)
                .map_err(|_| Error::NoInstanceWithInstanceId(instance_id.clone()))?;
            self.get_or_create_by_raft_id(raft_id)
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
        Response: serde::de::DeserializeOwned + 'static,
        Args: ToTupleBuffer,
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
    fn call_raw() {
        let l = tarantool::lua_state();
        l.exec(
            r#"
            function test_stored_proc(a, b)
                -- Tarantool always wraps return values from native stored procs
                -- into an additional array, while it doesn't do that for lua
                -- procs. Our network module is implemented to expect that
                -- additional layer of array, so seeing how this test proc is
                -- intended to emulate one of our rust procs, we explicitly
                -- add a table layer.
                return {a + b}
            end

            box.schema.func.create('test_stored_proc')
            "#,
        )
        .unwrap();

        let storage = Clusterwide::new().unwrap();
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

        let storage = Clusterwide::new().unwrap();
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

        let storage = Clusterwide::new().unwrap();
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

        let storage = Clusterwide::new().unwrap();
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
