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
use std::io::Cursor;
use std::rc::Rc;
use std::sync::Arc;

use self::tcp::TcpStream;

use super::protocol::api::{Call, Eval, Execute, Ping, Request};
use super::protocol::{self, Protocol, SyncIndex};
use crate::error;
use crate::error::TarantoolError;
use crate::fiber;
use crate::fiber::r#async::oneshot;
use crate::fiber::r#async::IntoOnDrop as _;
use crate::fiber::FiberId;
use crate::tuple::{ToTupleBuffer, Tuple};
use crate::unwrap_ok_or;

use futures::{AsyncReadExt, AsyncWriteExt};

#[deprecated = "use `ClientError` instead"]
pub type Error = ClientError;

/// Error returned by [`Client`].
#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    /// The connection was closed because of this error.
    ///
    /// The error is wrapped in a [`Arc`], because some libraries require
    /// error types to implement [`Sync`], which isn't implemented for [`Rc`].
    #[error("{0}")]
    ConnectionClosed(Arc<crate::error::Error>),

    /// Error happened during encoding of the request.
    ///
    /// The error is wrapped in a [`Arc`], because some libraries require
    /// error types to implement [`Sync`], which isn't implemented for [`Rc`].
    #[error("{0}")]
    RequestEncode(crate::error::Error),

    /// Error happened during decoding of the response.
    ///
    /// The error is wrapped in a [`Arc`], because some libraries require
    /// error types to implement [`Sync`], which isn't implemented for [`Rc`].
    #[error("{0}")]
    ResponseDecode(crate::error::Error),

    /// Service responded with an error.
    ///
    /// The error is wrapped in a [`Arc`], because some libraries require
    /// error types to implement [`Sync`], which isn't implemented for [`Rc`].
    #[error("{0}")]
    ErrorResponse(TarantoolError),
}

impl From<ClientError> for crate::error::Error {
    #[inline(always)]
    fn from(err: ClientError) -> Self {
        match err {
            ClientError::ConnectionClosed(err) => crate::error::Error::ConnectionClosed(err),
            ClientError::RequestEncode(err) => err,
            ClientError::ResponseDecode(err) => err,
            ClientError::ErrorResponse(err) => crate::error::Error::Remote(err),
        }
    }
}

#[derive(Clone, Debug)]
enum State {
    Alive,
    ClosedManually,
    /// This can only be [`Error::ConnectionClosed`].
    ClosedWithError(Arc<error::Error>),
}

impl State {
    fn is_alive(&self) -> bool {
        matches!(self, Self::Alive)
    }

    fn is_closed(&self) -> bool {
        !self.is_alive()
    }
}

#[derive(Debug)]
struct ClientInner {
    protocol: Protocol,
    awaiting_response: HashMap<SyncIndex, oneshot::Sender<Result<(), Arc<error::Error>>>>,
    state: State,
    /// The same tcp stream sender & receiver fibers a working with. Only stored
    /// here for closing.
    stream: TcpStream,
    sender_fiber_id: Option<FiberId>,
    receiver_fiber_id: Option<FiberId>,
    clients_count: usize,
}

impl ClientInner {
    pub fn new(config: protocol::Config, stream: TcpStream) -> Self {
        #[cfg(feature = "picodata")]
        if config.auth_method == crate::auth::AuthMethod::Ldap {
            crate::say_warn!(
                "You're using the 'ldap' authentication method, which implies sending the password UNENCRYPTED over the TCP connection. TLS is not yet implemented for IPROTO connections so make sure your communication channel is secure by other means."
            )
        }
        Self {
            protocol: Protocol::with_config(config),
            awaiting_response: HashMap::new(),
            state: State::Alive,
            stream,
            sender_fiber_id: None,
            receiver_fiber_id: None,
            clients_count: 1,
        }
    }
}

/// Wakes sender if `protocol` has new outgoing data.
fn maybe_wake_sender(client: &ClientInner) {
    if client.protocol.ready_outgoing_len() == 0 {
        // No point in waking the sender if there's nothing to send
        return;
    }
    if let Some(id) = client.sender_fiber_id {
        fiber::wakeup(id);
    }
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
    pub async fn connect(url: &str, port: u16) -> Result<Self, ClientError> {
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
    pub async fn connect_with_config(
        url: &str,
        port: u16,
        config: protocol::Config,
    ) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(url, port)
            .await
            .map_err(|e| ClientError::ConnectionClosed(Arc::new(e.into())))?;
        let client = ClientInner::new(config, stream.clone());
        let client = Rc::new(RefCell::new(client));

        let receiver_fiber_id = fiber::Builder::new()
            .func_async(receiver(client.clone(), stream.clone()))
            .name(format!("iproto-in/{url}:{port}"))
            .start_non_joinable()
            .unwrap();

        let sender_fiber_id = fiber::Builder::new()
            .func_async(sender(client.clone(), stream))
            .name(format!("iproto-out/{url}:{port}"))
            .start_non_joinable()
            .unwrap();

        {
            let mut client_mut = client.borrow_mut();
            client_mut.receiver_fiber_id = Some(receiver_fiber_id);
            client_mut.sender_fiber_id = Some(sender_fiber_id);
        }

        Ok(Self(client))
    }

    fn check_state(&self) -> Result<(), Arc<error::Error>> {
        match &self.0.borrow().state {
            State::Alive => Ok(()),
            State::ClosedManually => unreachable!("All client handles are dropped at this point"),
            State::ClosedWithError(err) => Err(err.clone()),
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
    async fn send<R: Request>(&self, request: &R) -> Result<R::Response, ClientError>;

    /// Execute a PING command.
    async fn ping(&self) -> Result<(), ClientError> {
        self.send(&Ping).await
    }

    /// Call a remote stored procedure.
    ///
    /// `conn.call("func", &("1", "2", "3"))` is the remote-call equivalent of `func('1', '2', '3')`.
    /// That is, `conn.call` is a remote stored-procedure call.
    /// The return from `conn.call` is whatever the function returns.
    async fn call<T>(&self, fn_name: &str, args: &T) -> Result<Tuple, ClientError>
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
    async fn eval<T>(&self, expr: &str, args: &T) -> Result<Tuple, ClientError>
    where
        T: ToTupleBuffer + ?Sized,
    {
        self.send(&Eval { args, expr }).await
    }

    /// Execute sql query remotely.
    async fn execute<T>(&self, sql: &str, bind_params: &T) -> Result<Vec<Tuple>, ClientError>
    where
        T: ToTupleBuffer + ?Sized,
    {
        self.send(&Execute { sql, bind_params }).await
    }
}

#[async_trait::async_trait(?Send)]
impl AsClient for Client {
    async fn send<R: Request>(&self, request: &R) -> Result<R::Response, ClientError> {
        if let Err(e) = self.check_state() {
            return Err(ClientError::ConnectionClosed(e));
        }

        let res = self.0.borrow_mut().protocol.send_request(request);
        let sync = unwrap_ok_or!(res,
            Err(e) => {
                return Err(ClientError::RequestEncode(e));
            }
        );

        let (tx, rx) = oneshot::channel();
        self.0.borrow_mut().awaiting_response.insert(sync, tx);
        maybe_wake_sender(&self.0.borrow());
        // Cleanup `awaiting_response` entry in case of `send` future cancelation
        // at this `.await`.
        // `send` can be canceled for example with `Timeout`.
        let res = rx
            .on_drop(|| {
                let _ = self.0.borrow_mut().awaiting_response.remove(&sync);
            })
            .await
            .expect("Channel should be open");
        if let Err(e) = res {
            return Err(ClientError::ConnectionClosed(e));
        }

        let res = self
            .0
            .borrow_mut()
            .protocol
            .take_response::<R>(sync)
            .expect("Is present at this point");
        let response = unwrap_ok_or!(res,
            Err(error::Error::Remote(response)) => {
                return Err(ClientError::ErrorResponse(response));
            }
            Err(e) => {
                return Err(ClientError::ResponseDecode(e));
            }
        );
        Ok(response)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let clients_count = self.0.borrow().clients_count;
        if clients_count == 1 {
            let mut client = self.0.borrow_mut();
            // Stop fibers
            client.state = State::ClosedManually;

            let receiver_fiber_id = client.receiver_fiber_id;
            let sender_fiber_id = client.sender_fiber_id;

            // We need to close the stream here, because otherwise receiver will
            // never wake up, because our async runtime blocks forever until the
            // future is ready.
            if let Err(e) = client.stream.close() {
                crate::say_error!("Client::drop: failed closing tcp stream: {e}");
            }

            // Drop ref before executing code that switches fibers.
            drop(client);

            // Cancel the worker fibers and wake them up so they can exit their loops
            if let Some(id) = receiver_fiber_id {
                fiber::cancel(id);
                fiber::wakeup(id);
            }

            if let Some(id) = sender_fiber_id {
                fiber::cancel(id);
                fiber::wakeup(id);
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
                let err = Arc::new(error::Error::from(err));
                // Notify all subscribers on closing
                let subscriptions: HashMap<_, _> = $client.awaiting_response.drain().collect();
                for (_, subscription) in subscriptions {
                    // We don't care about errors at this point
                    let _ = subscription.send(Err(err.clone()));
                }
                $client.state = State::ClosedWithError(err);
                return;
            }
        }
    };
}

/// Sender work loop. Yields on each iteration and during awaits.
async fn sender(client: Rc<RefCell<ClientInner>>, mut writer: TcpStream) {
    loop {
        if client.borrow().state.is_closed() || fiber::is_cancelled() {
            return;
        }
        // TODO: limit max send size
        let data = client.borrow_mut().protocol.take_outgoing_data();
        if data.is_empty() {
            // Wait for explicit wakeup, it should happen when there is new outgoing data
            fiber::fiber_yield();
        } else {
            let result = writer.write_all(&data).await;
            handle_result!(client.borrow_mut(), result);
        }
    }
}

/// Receiver work loop. Yields on each iteration and during awaits.
// Clippy falsely reports that we're holding a `RefCell` reference across an
// `await`, even though we're explicitly dropping the reference right before
// awaiting. Thank you clippy, very helpful!
#[allow(clippy::await_holding_refcell_ref)]
async fn receiver(client_cell: Rc<RefCell<ClientInner>>, mut reader: TcpStream) {
    let mut buf = vec![0_u8; 4096];
    loop {
        let client = client_cell.borrow();
        if client.state.is_closed() || fiber::is_cancelled() {
            return;
        }

        let size = client.protocol.read_size_hint();
        if buf.len() < size {
            buf.resize(size, 0);
        }
        let buf_slice = &mut buf[0..size];

        // Reference must be dropped before yielding.
        drop(client);

        let res = reader.read_exact(buf_slice).await;

        let mut client = client_cell.borrow_mut();
        handle_result!(client, res);

        let result = client
            .protocol
            .process_incoming(&mut Cursor::new(buf_slice));
        let result = handle_result!(client, result);
        if let Some(sync) = result {
            let subscription = client.awaiting_response.remove(&sync);
            if let Some(subscription) = subscription {
                subscription
                    .send(Ok(()))
                    .expect("cannot be closed at this point");
            } else {
                crate::say_warn!("received unwaited message for {sync:?}");
            }
        }

        // Wake sender to handle the greeting we may have just received
        maybe_wake_sender(&client);
    }
}

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;
    use crate::error::TarantoolErrorCode;
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
                ..Default::default()
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
        assert!(matches!(dbg!(err), ClientError::ConnectionClosed(_)))
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
            .execute(r#"SELECT * FROM "test_s1""#, &())
            .timeout(Duration::from_secs(3))
            .await
            .unwrap();
        assert!(result.len() >= 2);

        let result = client
            .execute(r#"SELECT * FROM "test_s1" WHERE "id" = ?"#, &(6002,))
            .timeout(Duration::from_secs(3))
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(
            result.first().unwrap().decode::<(u64, String)>().unwrap(),
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
            .unwrap_err();

        let err = error::Error::from(err);
        let error::Error::Remote(err) = err else {
            panic!()
        };

        assert_eq!(err.error_code(), TarantoolErrorCode::NoSuchProc as u32);

        #[rustfmt::skip]
        assert_eq!(err.to_string(), "NoSuchProc: Procedure 'unexistent_proc' is not defined");
    }

    #[crate::test(tarantool = "crate")]
    async fn eval() {
        let client = test_client().await;

        // Ok result
        let result = client
            .eval("return ...", &(1, 2))
            .timeout(Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!(result.decode::<(i32, i32)>().unwrap(), (1, 2));

        // Error result
        let err = client
            .eval("box.error(420)", &())
            .timeout(Duration::from_secs(3))
            .await
            .unwrap_err();

        let err = error::Error::from(err);
        let error::Error::Remote(err) = err else {
            panic!()
        };

        assert_eq!(err.error_code(), 420);
    }

    /// A regression test for https://git.picodata.io/picodata/picodata/tarantool-module/-/merge_requests/302
    #[crate::test(tarantool = "crate")]
    async fn client_count_regression() {
        let client = test_client().await;
        // Should close sender and receiver fibers
        client.0.borrow_mut().stream.close().unwrap();
        // Receiver wakes and closes
        fiber::reschedule();

        let fiber_id = client.0.borrow().sender_fiber_id.unwrap();
        let fiber_exists = fiber::wakeup(fiber_id);
        debug_assert!(fiber_exists);

        // Sender wakes and closes
        fiber::reschedule();
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

    #[cfg(feature = "picodata")]
    #[crate::test(tarantool = "crate")]
    async fn ldap_auth_method() {
        use crate::auth::AuthMethod;
        use std::time::Duration;

        let username = "Johnny";
        let password = "B. Goode";

        let _guard = crate::unwrap_ok_or!(
            crate::test::util::setup_ldap_auth(username, password),
            Err(e) => {
                println!("{e}, skipping ldap test");
                return;
            }
        );

        // Successfull connection
        {
            let client = Client::connect_with_config(
                "localhost",
                listen_port(),
                protocol::Config {
                    creds: Some((username.into(), password.into())),
                    auth_method: AuthMethod::Ldap,
                    ..Default::default()
                },
            )
            .timeout(Duration::from_secs(3))
            .await
            .unwrap();

            // network::Client will not try actually connecting until we send the
            // first request
            client
                .eval("print('\\x1b[32mit works!\\x1b[0m')", &())
                .await
                .unwrap();
        }

        // Wrong password
        {
            let client = Client::connect_with_config(
                "localhost",
                listen_port(),
                protocol::Config {
                    creds: Some((username.into(), "wrong password".into())),
                    auth_method: AuthMethod::Ldap,
                    ..Default::default()
                },
            )
            .timeout(Duration::from_secs(3))
            .await
            .unwrap();

            // network::Client will not try actually connecting until we send the
            // first request
            let err = client.eval("return", &()).await.unwrap_err().to_string();
            #[rustfmt::skip]
            assert_eq!(err, "server responded with error: PasswordMismatch: User not found or supplied credentials are invalid");
        }

        // Wrong auth method
        {
            let client = Client::connect_with_config(
                "localhost",
                listen_port(),
                protocol::Config {
                    creds: Some((username.into(), password.into())),
                    auth_method: AuthMethod::ChapSha1,
                    ..Default::default()
                },
            )
            .timeout(Duration::from_secs(3))
            .await
            .unwrap();

            // network::Client will not try actually connecting until we send the
            // first request
            let err = client.eval("return", &()).await.unwrap_err().to_string();
            #[rustfmt::skip]
            assert_eq!(err, "server responded with error: PasswordMismatch: User not found or supplied credentials are invalid");
        }
    }

    #[crate::test(tarantool = "crate")]
    async fn extended_error_info() {
        let client = test_client().await;

        let res = client
            .eval(
                "error1 = box.error.new(box.error.UNSUPPORTED, 'this', 'that')
                error2 = box.error.new('MyCode', 'my message')
                error3 = box.error.new('MyOtherCode', 'my other message')
                error2:set_prev(error3)
                error1:set_prev(error2)
                error1:raise()",
                &(),
            )
            .timeout(Duration::from_secs(3))
            .await;

        let error::Error::Remote(e) = error::Error::from(res.unwrap_err()) else {
            panic!();
        };

        assert_eq!(e.error_code(), TarantoolErrorCode::Unsupported as u32);
        assert_eq!(e.message(), "this does not support that");
        assert_eq!(e.error_type(), "ClientError");
        assert_eq!(e.file(), Some("eval"));
        assert_eq!(e.line(), Some(1));
        assert_eq!(e.fields().len(), 0);

        let e = e.cause().unwrap();

        assert_eq!(e.error_code(), 0);
        assert_eq!(e.message(), "my message");
        assert_eq!(e.error_type(), "CustomError");
        assert_eq!(e.file(), Some("eval"));
        assert_eq!(e.line(), Some(2));
        assert_eq!(e.fields().len(), 1);
        assert_eq!(e.fields()["custom_type"], rmpv::Value::from("MyCode"));

        let e = e.cause().unwrap();

        assert_eq!(e.error_code(), 0);
        assert_eq!(e.message(), "my other message");
        assert_eq!(e.error_type(), "CustomError");
        assert_eq!(e.file(), Some("eval"));
        assert_eq!(e.line(), Some(3));
        assert_eq!(e.fields().len(), 1);
        assert_eq!(e.fields()["custom_type"], rmpv::Value::from("MyOtherCode"));

        assert!(e.cause().is_none());
    }
}
