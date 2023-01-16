//! Tarantool based client.
//! Can be used only from inside tarantool.

mod tcp;

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Cursor, Error as IoError};
use std::net::ToSocketAddrs;
use std::rc::Rc;
use std::time::Duration;

use self::tcp::{Error as TcpError, TcpStream};

use super::protocol::api::{Call, Eval, Execute, Ping, Request};
use super::protocol::options::{ConnOptions, Options};
use super::protocol::{codec, Error as ProtocolError, Protocol, SizeHint, SyncIndex};
use crate::ffi::tarantool::coio_close;
use crate::fiber;
use crate::fiber::r#async::{oneshot, timeout};
use crate::tuple::{Decode, ToTupleBuffer, Tuple};

use futures::io::{ReadHalf, WriteHalf};
use futures::{AsyncReadExt, AsyncWriteExt};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("tcp stream error: {0}")]
    Tcp(#[from] TcpError),
    #[error("io error: {0}")]
    Io(#[from] IoError),
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("closed with error: {0}")]
    ClosedWithErr(String),
    #[error("{0}")]
    Other(String),
}

#[derive(Clone, Debug)]
enum State {
    Alive,
    ClosedManually,
    ClosedWithError(String),
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
}

impl ClientInner {
    pub fn new() -> Self {
        Self {
            protocol: Protocol::new(),
            awaiting_response: HashMap::new(),
            state: State::Alive,
            close_token: None,
            worker_handles: Vec::new(),
        }
    }
}

/// Actual client that can be used to send and receive messages to tarantool instance.
///
/// Can be cloned and moved into different fibers for connection to be reused
// WARNING: Attention should be payed not to borrow inner client across await and yield points.
#[derive(Clone, Debug)]
pub struct Client(Rc<RefCell<ClientInner>>);

impl Client {
    pub async fn connect(url: &str, port: u16) -> Result<Self, Error> {
        let client = Rc::new(RefCell::new(ClientInner::new()));
        let (tx, rx) = oneshot::channel();
        start_fibers(client.clone(), tx, url, port);
        // RecvError may happen if there was some error during connection.
        let _ = rx.await;
        let client = Self(client);
        client.check_state()?;
        Ok(client)
    }

    fn check_state(&self) -> Result<(), Error> {
        match self.0.borrow().state.clone() {
            State::Alive => Ok(()),
            State::ClosedManually => unreachable!("All client handles are dropped at this point"),
            State::ClosedWithError(err) => Err(Error::ClosedWithErr(err)),
        }
    }

    /// Send [`Request`] and wait for response.
    /// This function yields.
    ///
    /// # Errors
    /// In case of `ClosedWithErr` it is suggested to recreate the connection.
    /// Other errors are self-descriptive.
    async fn send<R: Request>(&self, request: &R) -> Result<R::Response, Error> {
        self.check_state()?;
        let sync = self.0.borrow_mut().protocol.send_request(request)?;
        let (tx, rx) = oneshot::channel();
        self.0.borrow_mut().awaiting_response.insert(sync, tx);
        // TODO: Set timeout
        rx.await.expect("Channel should be open")?;
        Ok(self
            .0
            .borrow_mut()
            .protocol
            .take_response(sync, request)
            .expect("Is present at this point")?)
    }

    /// Execute a PING command.
    pub async fn ping(&self) -> Result<(), Error> {
        self.send(&Ping).await
    }

    /// Call a remote stored procedure.
    ///
    /// `conn.call("func", &("1", "2", "3"))` is the remote-call equivalent of `func('1', '2', '3')`.
    /// That is, `conn.call` is a remote stored-procedure call.
    /// The return from `conn.call` is whatever the function returns.
    pub async fn call<T: ToTupleBuffer>(
        &self,
        fn_name: &str,
        args: &T,
    ) -> Result<Option<Tuple>, Error> {
        self.send(&Call { fn_name, args }).await
    }

    /// Evaluates and executes the expression in Lua-string, which may be any statement or series of statements.
    ///
    /// An execute privilege is required; if the user does not have it, an administrator may grant it with
    /// `box.schema.user.grant(username, 'execute', 'universe')`.
    ///
    /// To ensure that the return from `eval` is whatever the Lua expression returns, begin the Lua-string with the
    /// word `return`.
    pub async fn eval<T: ToTupleBuffer>(
        &self,
        expr: &str,
        args: &T,
    ) -> Result<Option<Tuple>, Error> {
        self.send(&Eval { args, expr }).await
    }

    /// Remote execute of sql query.
    pub async fn execute<T: ToTupleBuffer>(
        &self,
        sql: &str,
        bind_params: &T,
        limit: Option<usize>,
    ) -> Result<Vec<Tuple>, Error> {
        self.send(&Execute {
            sql,
            bind_params,
            limit,
        })
        .await
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // 3 means this client and 2 fibers: receiver and sender
        if Rc::strong_count(&self.0) <= 3 {
            let mut client = self.0.borrow_mut();
            // Stop fibers
            client.state = State::ClosedManually;

            let close_token = client.close_token.take();
            let handles: Vec<_> = client.worker_handles.drain(..).collect();

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
        }
    }
}

macro_rules! handle_result {
    ($client:expr, $e:expr) => {
        match $e {
            Ok(value) => value,
            Err(err) => {
                let err: Error = err.into();
                let str_err = err.to_string();
                $client.state = State::ClosedWithError(err.to_string());
                // Notify all subscribers on closing
                let subscriptions: HashMap<_, _> = $client.awaiting_response.drain().collect();
                for (_, subscription) in subscriptions {
                    // We don't care about errors at this point
                    let _ = subscription.send(Err(Error::ClosedWithErr(str_err.clone())));
                }
                return;
            }
        }
    };
}

/// Starts receiver and sender fibers.
fn start_fibers(client: Rc<RefCell<ClientInner>>, tx: oneshot::Sender<()>, url: &str, port: u16) {
    let jh = fiber::start(move || {
        // TODO: Set actual timeout
        let result = TcpStream::connect(url, port, Duration::MAX);
        let stream = handle_result!(client.borrow_mut(), result);
        client.borrow_mut().close_token = Some(stream.close_token());
        let (reader, writer) = stream.split();

        // start receiver in a separate fiber
        let receiver_client = client.clone();
        let mut receiver_handle = fiber::Builder::new()
            .func(move || fiber::block_on(receiver(receiver_client.clone(), reader)))
            .name("network-client-receiver")
            .start()
            .unwrap();

        // start sender here
        let sender_client = client.clone();
        let sender_handle = fiber::Builder::new()
            .func(move || fiber::block_on(sender(sender_client.clone(), writer)))
            .name("network-client-sender")
            .start()
            .unwrap();
        client.borrow_mut().worker_handles = vec![receiver_handle, sender_handle];
        tx.send(()).unwrap();
    });
    std::mem::forget(jh);
}

/// Sender work loop. Yields on each iteration and during awaits.
async fn sender(client: Rc<RefCell<ClientInner>>, mut writer: WriteHalf<TcpStream>) {
    loop {
        if client.borrow().state.is_closed() {
            return;
        }
        // TODO: Set max drain
        let data: Vec<_> = client
            .borrow_mut()
            .protocol
            .drain_outgoing_data(None)
            .collect();
        if data.is_empty() {
            // TODO: use a watch channel to wait till data is not empty instead
            fiber::r#yield();
        } else {
            let result = writer.write_all(&data).await;
            handle_result!(client.borrow_mut(), result);
        }
    }
}

/// Receiver work loop. Yields on each iteration and during awaits.
async fn receiver(client: Rc<RefCell<ClientInner>>, mut reader: ReadHalf<TcpStream>) {
    let mut hint = client.borrow().protocol.read_size_hint();
    loop {
        if client.borrow().state.is_closed() {
            return;
        }
        match hint {
            SizeHint::Hint(size) => {
                let mut buf = vec![0; size];
                handle_result!(client.borrow_mut(), reader.read_exact(&mut buf).await);
                let mut client_ref = client.borrow_mut();
                let result = client_ref.protocol.process_incoming(&mut Cursor::new(buf));
                hint = client_ref.protocol.read_size_hint();
                if let Some(sync) = handle_result!(client_ref, result) {
                    if let Some(subscription) = client_ref.awaiting_response.remove(&sync) {
                        // Dropping client ref as `send` can wake other fibers.
                        drop(client_ref);
                        subscription
                            .send(Ok(()))
                            .expect("Cannot be closed at this point");
                    } else {
                        log::warn!("Received unwaited message for {sync:?}");
                    }
                }
            }
            SizeHint::FirstU32 => {
                // Read 5 bytes, 1st is a marker
                let mut buf = vec![0; 5];
                handle_result!(client.borrow_mut(), reader.read_exact(&mut buf).await);
                let result = rmp::decode::read_u32(&mut Cursor::new(buf));
                let mut client_ref = client.borrow_mut();
                let new_hint = handle_result!(client_ref, result.map_err(ProtocolError::from));
                if new_hint > 0 {
                    hint = SizeHint::Hint(new_hint as usize)
                } else {
                    handle_result!(
                        client_ref,
                        Err(Error::Other("Unexpected zero message length".to_owned()))
                    )
                }
            }
        }
    }
}

#[cfg(feature = "tarantool_test")]
mod tests {
    use super::*;
    use crate::fiber::r#async::timeout::IntoTimeout as _;
    use crate::test::{TestCase, TARANTOOL_LISTEN, TESTS};
    use crate::test_name;

    use linkme::distributed_slice;

    #[distributed_slice(TESTS)]
    static CONNECT: TestCase = TestCase {
        name: test_name!("connect"),
        f: || {
            fiber::block_on(async {
                let client = Client::connect("localhost", TARANTOOL_LISTEN)
                    .await
                    .unwrap();
            });
        },
    };

    #[distributed_slice(TESTS)]
    static CONNECT_FAILURE: TestCase = TestCase {
        name: test_name!("connect_failure"),
        f: || {
            fiber::block_on(async {
                // Can be any other unused port
                let err = Client::connect("localhost", 3300).await.unwrap_err();
                assert!(matches!(dbg!(err), Error::ClosedWithErr(_)))
            });
        },
    };

    #[distributed_slice(TESTS)]
    static PING: TestCase = TestCase {
        name: test_name!("ping"),
        f: || {
            fiber::block_on(async {
                let client = Client::connect("localhost", TARANTOOL_LISTEN)
                    .await
                    .unwrap();

                for _ in 0..5 {
                    client.ping().timeout(Duration::from_secs(3)).await.unwrap();
                }
            });
        },
    };
}
