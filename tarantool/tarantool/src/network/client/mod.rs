//! Tarantool based async [`Client`].
//!
//! Can be used only from inside Tarantool as it makes heavy use of fibers and coio.
//!
//! # Example
//! ```no_run
//! # async {
//! use tarantool::network::client::Client;
//! // Most of the client's methods are in the `AsClient` trait
//! use tarantool::network::client::AsClient as _;
//!
//! let client = Client::connect("localhost", 3301).await.unwrap();
//! client.ping().await.unwrap();
//!
//! // Requests can also be easily combined with fiber::r#async::timeout
//! use tarantool::fiber::r#async::timeout::IntoTimeout as _;
//! use std::time::Duration;
//!
//! client.ping().timeout(Duration::from_secs(10)).await.unwrap();
//! # };
//! ```
//!
//! # Reusing Connection
//! Client can be cloned, and safely moved to a different fiber if needed, to reuse the same connection.
//! When multiple fibers use the same connection, all requests are pipelined through the same network socket, but each fiber
//! gets back a correct response. Reducing the number of active sockets lowers the overhead of system calls and increases
//! the overall server performance.
//!
//! # Implementation
//! Internally the client uses [`Protocol`] to get bytes that it needs to send
//! and push bytes that it gets from the network.
//!
//! On creation the client spawns sender and receiver worker threads. Which in turn
//! use coio based [`TcpStream`] as the transport layer.

pub mod reconnect;
pub mod tcp;

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{self, Cursor};
use std::rc::Rc;
use std::sync::Arc;

use self::tcp::{Error as TcpError, TcpStream};

use super::protocol::api::{Call, Eval, Execute, Ping, Request};
use super::protocol::{self, Error as ProtocolError, Protocol, SyncIndex};
use crate::fiber;
use crate::fiber::r#async::IntoOnDrop as _;
use crate::fiber::r#async::{oneshot, watch};
use crate::tuple::{ToTupleBuffer, Tuple};

use futures::io::{ReadHalf, WriteHalf};
use futures::{AsyncReadExt, AsyncWriteExt};

/// Error returned by [`Client`].
#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    /// The error is wrapped in a [`Arc`], because some libraries require
    /// error types to implement [`Sync`], which isn't implemented for [`Rc`].
    #[error("{0}")]
    Tcp(Arc<TcpError>),

    /// The error is wrapped in a [`Arc`], because some libraries require
    /// error types to implement [`Sync`], which isn't implemented for [`Rc`].
    #[error("{0}")]
    Io(Arc<io::Error>),

    /// The error is wrapped in a [`Arc`], because some libraries require
    /// error types to implement [`Sync`], which isn't implemented for [`Rc`].
    #[error("protocol error: {0}")]
    Protocol(Arc<ProtocolError>),
}

impl From<Error> for crate::error::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Tcp(err) => crate::error::Error::Tcp(err),
            Error::Io(err) => crate::error::Error::IO(err.kind().into()),
            Error::Protocol(err) => crate::error::Error::Protocol(err),
        }
    }
}

impl From<TcpError> for Error {
    fn from(err: TcpError) -> Self {
        Error::Tcp(Arc::new(err))
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(Arc::new(err))
    }
}

impl From<ProtocolError> for Error {
    fn from(err: ProtocolError) -> Self {
        Error::Protocol(Arc::new(err))
    }
}

#[derive(Clone, Debug)]
enum State {
    Alive,
    ClosedManually,
    ClosedWithError(Error),
}

impl State {
    fn is_alive(&self) -> bool {
        matches!(self, Self::Alive)
    }

    fn is_closed(&self) -> bool {
        !self.is_alive()
    }
}

type WorkerHandle = fiber::JoinHandle<'static, ()>;

#[derive(Debug)]
struct ClientInner {
    protocol: Protocol,
    awaiting_response: HashMap<SyncIndex, oneshot::Sender<Result<(), Error>>>,
    state: State,
    close_token: Option<tcp::CloseToken>,
    worker_handles: Vec<WorkerHandle>,
    sender_waker: watch::Sender<()>,
    clients_count: usize,
}

impl ClientInner {
    pub fn new(config: protocol::Config, sender_waker: watch::Sender<()>) -> Self {
        Self {
            protocol: Protocol::with_config(config),
            awaiting_response: HashMap::new(),
            state: State::Alive,
            close_token: None,
            worker_handles: Vec::new(),
            sender_waker,
            clients_count: 1,
        }
    }
}

/// Wakes sender if `protocol` has new outgoing data.
///
/// # Errors
/// Returns an error if `sender_waker` channel receivers are holding a reference to the previous value.
/// Which generally shouldn't be the case as it is an empty value.
fn wake_sender(client: &RefCell<ClientInner>) -> Result<(), watch::SendError<()>> {
    let len = client.borrow().protocol.ready_outgoing_len();
    if len > 0 {
        client.borrow().sender_waker.send(())?;
    }
    Ok(())
}

/// Actual client that can be used to send and receive messages to tarantool instance.
///
/// Can be cloned and moved into different fibers for connection to be reused.
///
/// See [`super::client`] for examples and [`AsClient`] trait for API.
// WARNING: Attention should be payed not to borrow inner client across await and yield points.
#[derive(Debug)]
pub struct Client(Rc<RefCell<ClientInner>>);

impl Client {
    /// Creates a new client and tries to establish connection
    /// to `url:port`
    ///
    /// # Errors
    /// Error is returned if an attempt to connect failed.
    /// See [`Error`].
    pub async fn connect(url: &str, port: u16) -> Result<Self, Error> {
        Self::connect_with_config(url, port, Default::default()).await
    }

    /// Creates a new client and tries to establish connection
    /// to `url:port`
    ///
    /// Takes explicit `config` in comparison to [`Client::connect`]
    /// where default values are used.
    ///
    /// # Errors
    /// Error is returned if an attempt to connect failed.
    /// See [`Error`].
    pub async fn connect_with_config(
        url: &str,
        port: u16,
        config: protocol::Config,
    ) -> Result<Self, Error> {
        let (sender_waker_tx, sender_waker_rx) = watch::channel(());
        let mut client = ClientInner::new(config, sender_waker_tx);
        let stream = TcpStream::connect(url, port).await?;
        client.close_token = Some(stream.close_token());

        let (reader, writer) = stream.split();
        let client = Rc::new(RefCell::new(client));

        // start receiver in a separate fiber
        let receiver_handle = fiber::Builder::new()
            .func_async(receiver(client.clone(), reader))
            .name("network-client-receiver")
            .start()
            .unwrap();

        // start sender in a separate fiber
        let sender_handle = fiber::Builder::new()
            .func_async(sender(client.clone(), writer, sender_waker_rx))
            .name("network-client-sender")
            .start()
            .unwrap();
        client.borrow_mut().worker_handles = vec![receiver_handle, sender_handle];
        Ok(Self(client))
    }

    fn check_state(&self) -> Result<(), Error> {
        match self.0.borrow().state.clone() {
            State::Alive => Ok(()),
            State::ClosedManually => unreachable!("All client handles are dropped at this point"),
            State::ClosedWithError(err) => Err(err),
        }
    }
}

/// Generic API for an entity that behaves as Tarantool Client.
#[async_trait::async_trait(?Send)]
pub trait AsClient {
    /// Send [`Request`] and wait for response.
    /// This function yields.
    ///
    /// # Errors
    /// In case of `ClosedWithErr` it is suggested to recreate the connection.
    /// Other errors are self-descriptive.
    async fn send<R: Request>(&self, request: &R) -> Result<R::Response, Error>;

    /// Execute a PING command.
    async fn ping(&self) -> Result<(), Error> {
        self.send(&Ping).await
    }

    /// Call a remote stored procedure.
    ///
    /// `conn.call("func", &("1", "2", "3"))` is the remote-call equivalent of `func('1', '2', '3')`.
    /// That is, `conn.call` is a remote stored-procedure call.
    /// The return from `conn.call` is whatever the function returns.
    async fn call<T>(&self, fn_name: &str, args: &T) -> Result<Tuple, Error>
    where
        T: ToTupleBuffer + ?Sized,
    {
        self.send(&Call { fn_name, args }).await
    }

    /// Evaluates and executes the expression in Lua-string, which may be any statement or series of statements.
    ///
    /// An execute privilege is required; if the user does not have it, an administrator may grant it with
    /// `box.schema.user.grant(username, 'execute', 'universe')`.
    ///
    /// To ensure that the return from `eval` is whatever the Lua expression returns, begin the Lua-string with the
    /// word `return`.
    async fn eval<T>(&self, expr: &str, args: &T) -> Result<Tuple, Error>
    where
        T: ToTupleBuffer + ?Sized,
    {
        self.send(&Eval { args, expr }).await
    }

    /// Execute sql query remotely.
    async fn execute<T>(
        &self,
        sql: &str,
        bind_params: &T,
        limit: Option<usize>,
    ) -> Result<Vec<Tuple>, Error>
    where
        T: ToTupleBuffer + ?Sized,
    {
        self.send(&Execute {
            sql,
            bind_params,
            limit,
        })
        .await
    }
}

#[async_trait::async_trait(?Send)]
impl AsClient for Client {
    async fn send<R: Request>(&self, request: &R) -> Result<R::Response, Error> {
        self.check_state()?;
        let sync = self.0.borrow_mut().protocol.send_request(request)?;
        let (tx, rx) = oneshot::channel();
        self.0.borrow_mut().awaiting_response.insert(sync, tx);
        wake_sender(&self.0).unwrap();
        // Cleanup `awaiting_response` entry in case of `send` future cancelation
        // at this `.await`.
        // `send` can be canceled for example with `Timeout`.
        rx.on_drop(|| {
            let _ = self.0.borrow_mut().awaiting_response.remove(&sync);
        })
        .await
        .expect("Channel should be open")?;
        Ok(self
            .0
            .borrow_mut()
            .protocol
            .take_response(sync, request)
            .expect("Is present at this point")?)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let clients_count = self.0.borrow().clients_count;
        if clients_count == 1 {
            let mut client = self.0.borrow_mut();
            // Stop fibers
            client.state = State::ClosedManually;

            let close_token = client.close_token.take();
            let handles: Vec<_> = client.worker_handles.drain(..).collect();
            // Wake sender so it can exit loop
            client.sender_waker.send(()).unwrap();

            // Drop ref before executing code that switches fibers.
            drop(client);
            if let Some(close_token) = close_token {
                // Close TCP stream to wake fibers waiting on coio events
                let _ = close_token.close();
            }
            // Join fibers
            for handle in handles {
                handle.join();
            }
        } else {
            self.0.borrow_mut().clients_count -= 1;
        }
    }
}

impl Clone for Client {
    fn clone(&self) -> Self {
        self.0.borrow_mut().clients_count += 1;
        Self(self.0.clone())
    }
}

macro_rules! handle_result {
    ($client:expr, $e:expr) => {
        match $e {
            Ok(value) => value,
            Err(err) => {
                let err: Error = err.into();
                $client.state = State::ClosedWithError(err.clone());
                // Notify all subscribers on closing
                let subscriptions: HashMap<_, _> = $client.awaiting_response.drain().collect();
                for (_, subscription) in subscriptions {
                    // We don't care about errors at this point
                    let _ = subscription.send(Err(err.clone()));
                }
                return;
            }
        }
    };
}

/// Sender work loop. Yields on each iteration and during awaits.
async fn sender(
    client: Rc<RefCell<ClientInner>>,
    mut writer: WriteHalf<TcpStream>,
    mut waker: watch::Receiver<()>,
) {
    loop {
        if client.borrow().state.is_closed() {
            return;
        }
        // TODO: limit max send size
        let data = client.borrow_mut().protocol.take_outgoing_data();
        if data.is_empty() {
            // Wait for explicit wakeup, it should happen when there is new outgoing data
            waker.changed().await.expect("channel should be open");
        } else {
            let result = writer.write_all(&data).await;
            handle_result!(client.borrow_mut(), result);
        }
    }
}

/// Receiver work loop. Yields on each iteration and during awaits.
async fn receiver(client: Rc<RefCell<ClientInner>>, mut reader: ReadHalf<TcpStream>) {
    loop {
        if client.borrow().state.is_closed() {
            return;
        }
        let size = client.borrow().protocol.read_size_hint();
        let mut buf = vec![0; size];
        handle_result!(client.borrow_mut(), reader.read_exact(&mut buf).await);
        let result = client
            .borrow_mut()
            .protocol
            .process_incoming(&mut Cursor::new(buf));
        let result = handle_result!(client.borrow_mut(), result);
        if let Some(sync) = result {
            let subscription = client.borrow_mut().awaiting_response.remove(&sync);
            if let Some(subscription) = subscription {
                subscription
                    .send(Ok(()))
                    .expect("cannot be closed at this point");
            } else {
                log::warn!("received unwaited message for {sync:?}");
            }
        }
        wake_sender(&client).unwrap();
    }
}

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;
    use crate::fiber::r#async::timeout::IntoTimeout as _;
    use crate::space::Space;
    use crate::test::util::listen_port;
    use std::time::Duration;

    async fn test_client() -> Client {
        Client::connect_with_config(
            "localhost",
            listen_port(),
            protocol::Config {
                creds: Some(("test_user".into(), "password".into())),
            },
        )
        .timeout(Duration::from_secs(3))
        .await
        .unwrap()
    }

    #[crate::test(tarantool = "crate")]
    async fn connect() {
        let _client = Client::connect("localhost", listen_port()).await.unwrap();
    }

    #[crate::test(tarantool = "crate")]
    async fn connect_failure() {
        // Can be any other unused port
        let err = Client::connect("localhost", 0).await.unwrap_err();
        assert!(matches!(dbg!(err), Error::Tcp(_)))
    }

    #[crate::test(tarantool = "crate")]
    async fn ping() {
        let client = test_client().await;

        for _ in 0..5 {
            client.ping().timeout(Duration::from_secs(3)).await.unwrap();
        }
    }

    #[crate::test(tarantool = "crate")]
    fn ping_concurrent() {
        let client = fiber::block_on(test_client());
        let fiber_a = fiber::start_async(async {
            client.ping().timeout(Duration::from_secs(3)).await.unwrap()
        });
        let fiber_b = fiber::start_async(async {
            client.ping().timeout(Duration::from_secs(3)).await.unwrap()
        });
        fiber_a.join();
        fiber_b.join();
    }

    #[crate::test(tarantool = "crate")]
    async fn execute() {
        Space::find("test_s1")
            .unwrap()
            .insert(&(6001, "6001"))
            .unwrap();
        Space::find("test_s1")
            .unwrap()
            .insert(&(6002, "6002"))
            .unwrap();

        let client = test_client().await;

        let lua = crate::lua_state();
        // Error is silently ignored on older versions, before 'compat' was introduced.
        _ = lua.exec("require'compat'.sql_seq_scan_default = 'old'");

        let result = client
            .execute(r#"SELECT * FROM "test_s1""#, &(), None)
            .timeout(Duration::from_secs(3))
            .await
            .unwrap();
        assert!(result.len() >= 2);

        let result = client
            .execute(r#"SELECT * FROM "test_s1" WHERE "id" = ?"#, &(6002,), None)
            .timeout(Duration::from_secs(3))
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(
            result.get(0).unwrap().decode::<(u64, String)>().unwrap(),
            (6002, "6002".into())
        );
    }

    #[crate::test(tarantool = "crate")]
    async fn call() {
        let client = test_client().await;

        let result = client
            .call("test_stored_proc", &(1, 2))
            .timeout(Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!(result.decode::<(i32,)>().unwrap(), (3,));
    }

    #[crate::test(tarantool = "crate")]
    async fn invalid_call() {
        let client = test_client().await;

        let err = client
            .call("unexistent_proc", &())
            .timeout(Duration::from_secs(3))
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(err, "protocol error: service responded with error: Procedure 'unexistent_proc' is not defined");
    }

    #[crate::test(tarantool = "crate")]
    async fn eval() {
        let client = test_client().await;

        let result = client
            .eval("return ...", &(1, 2))
            .timeout(Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!(result.decode::<(i32, i32)>().unwrap(), (1, 2));
    }

    /// A regression test for https://git.picodata.io/picodata/picodata/tarantool-module/-/merge_requests/302
    #[crate::test(tarantool = "crate")]
    async fn client_count_regression() {
        let client = test_client().await;
        // Should close sender and receiver fibers
        let close_token = client.0.borrow_mut().close_token.take();
        close_token.unwrap().close().unwrap();
        // Receiver wakes and closes
        fiber::r#yield().unwrap();
        client.0.borrow().sender_waker.send(()).unwrap();
        // Sender wakes and closes
        fiber::r#yield().unwrap();
        // Sender and receiver stopped and dropped their refs
        assert_eq!(Rc::strong_count(&client.0), 1);

        // Cloning a client produces 2 refs
        let client_clone = client.clone();
        assert_eq!(Rc::strong_count(&client.0), 2);
        // Here if client checked by Rc refs <= 3 it would assume it is the last and set state to ClosedManually
        drop(client_clone);
        assert_eq!(Rc::strong_count(&client.0), 1);

        // This would panic on unreachable if previous drop have set the state
        client.check_state().unwrap_err();
    }

    #[crate::test(tarantool = "crate")]
    async fn concurrent_messages_one_fiber() {
        let client = test_client().await;
        let mut ping_futures = vec![];
        for _ in 0..10 {
            ping_futures.push(client.ping());
        }
        for res in futures::future::join_all(ping_futures).await {
            res.unwrap();
        }
    }

    #[crate::test(tarantool = "crate")]
    async fn data_always_present_in_response() {
        let client = test_client().await;

        // Even though we do a return without value,
        // error `ResponseDataNotFound` is never returned, the result is Ok(_) instead.
        client.eval("return", &()).await.unwrap();
        client.call("LUA", &("return",)).await.unwrap();
    }

    #[crate::test(tarantool = "crate")]
    async fn big_data() {
        // NOTE: random looking constants in this test are random.
        // I'm just increasing the entropy for good luck.
        use crate::tuple::RawByteBuf;

        #[crate::proc(tarantool = "crate")]
        fn proc_big_data<'a>(s: &'a serde_bytes::Bytes) -> usize {
            s.len() + 17
        }

        let path = crate::proc::module_path(&big_data as *const _ as _).unwrap();
        let module = path.file_stem().unwrap();
        let module = module.to_str().unwrap();
        let proc = format!("{module}.proc_big_data");

        let lua = crate::lua_state();
        lua.exec_with("box.schema.func.create(..., { language = 'C' })", &proc)
            .unwrap();

        let client = test_client().await;

        const N: u32 = 0x6fff_ff69;
        // SAFETY: this is basically a generation of a random array
        #[allow(clippy::uninit_vec)]
        let s = unsafe {
            let buf_size = (N + 6) as usize;
            let mut data = Vec::<u8>::with_capacity(buf_size);
            data.set_len(buf_size);
            data[0] = b'\x91';
            data[1] = b'\xc6'; // BIN32
            data[2..6].copy_from_slice(&N.to_be_bytes());
            RawByteBuf::from(data)
        };

        let t0 = std::time::Instant::now();
        let t = client.call(&proc, &s).await.unwrap();
        dbg!(t0.elapsed());

        if let Ok((len,)) = t.decode::<(u32,)>() {
            assert_eq!(len, N + 17);
        } else {
            let ((len,),): ((u32,),) = t.decode().unwrap();
            assert_eq!(len, N + 17);
        }
    }
}
