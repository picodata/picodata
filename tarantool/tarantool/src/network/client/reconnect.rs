use std::{cell::RefCell, rc::Rc, time::Duration};

use super::Error;
use crate::fiber::r#async::timeout;
use crate::fiber::r#async::timeout::IntoTimeout as _;
use crate::fiber::r#async::watch;
use crate::network::protocol;

/// A reconnecting version of [`super::Client`].
///
/// Reconnects during requests either on timeout or when network error happens.
/// Can be cloned to utilize the same connection from multiple fibers.
///
/// See [`super::AsClient`] for the full API.
#[derive(Debug, Clone)]
pub struct Client {
    client: RefCell<super::Client>,
    new_client_rx: RefCell<watch::Receiver<super::Client>>,
    new_client_tx: Rc<RefCell<watch::Sender<super::Client>>>,
    timeout: Duration,
    url: String,
    port: u16,
    protocol_config: protocol::Config,

    // Testing related code
    #[cfg(feature = "internal_test")]
    inject_error: Rc<RefCell<Option<super::Error>>>,
}

impl Client {
    /// Force reconnection.
    ///
    /// If one of the cloned clients (used in other fibers/places) has already reconnected,
    /// this client will use this new connection instead of trying to establish a new one.
    ///
    /// When reconnection happens ongoing requests (processing in other fibers) will
    /// continue on the old connection, but any new request will use the new connection.
    pub async fn reconnect(&self) -> Result<(), Error> {
        let has_changed = self.new_client_rx.borrow().has_changed();
        if has_changed {
            self.new_client_rx.borrow_mut().mark_seen();
            *self.client.borrow_mut() = self.new_client_rx.borrow().get_cloned();
        } else {
            *self.client.borrow_mut() = super::Client::connect_with_config(
                &self.url,
                self.port,
                self.protocol_config.clone(),
            )
            .await?;
            self.new_client_tx
                .borrow_mut()
                .send(self.client.borrow().clone())
                .expect("no references should be held");
            self.new_client_rx.borrow_mut().mark_seen();
        }
        Ok(())
    }

    /// Creates a new client and tries to establish connection
    /// to `url:port`
    ///
    /// Use `timeout` to specify at which timeout to reconnect during request.
    ///
    /// # Errors
    /// Error is returned if an attempt to connect failed.
    /// See [`Error`].
    pub async fn connect(url: String, port: u16, timeout: Duration) -> Result<Self, Error> {
        Self::connect_with_config(url, port, Default::default(), timeout).await
    }

    /// Creates a new client and tries to establish connection
    /// to `url:port`
    ///
    /// Takes explicit `config` in comparison to [`Client::connect`]
    /// where default values are used.
    ///
    /// Use `timeout` to specify at which timeout to reconnect during request.
    ///
    /// # Errors
    /// Error is returned if an attempt to connect failed.
    /// See [`Error`].
    pub async fn connect_with_config(
        url: String,
        port: u16,
        config: protocol::Config,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let client = super::Client::connect_with_config(&url, port, config.clone()).await?;
        let (new_client_tx, new_client_rx) = watch::channel(client.clone());
        Ok(Self {
            client: RefCell::new(client),
            timeout,
            url,
            port,
            protocol_config: config,
            new_client_rx: RefCell::new(new_client_rx),
            new_client_tx: Rc::new(RefCell::new(new_client_tx)),

            #[cfg(feature = "internal_test")]
            inject_error: Default::default(),
        })
    }

    #[cfg(feature = "internal_test")]
    pub fn inject_error(&self, error: super::Error) {
        *self.inject_error.borrow_mut() = Some(error);
    }

    #[cfg(feature = "internal_test")]
    pub fn reconnect_count(&self) -> u64 {
        self.new_client_rx.borrow().value_version()
    }
}

#[async_trait::async_trait(?Send)]
impl super::AsClient for Client {
    // This warning about `self.client.borrow()` can be ingored as this ref cell is not shared between fibers
    #[allow(clippy::await_holding_refcell_ref)]
    async fn send<R: protocol::api::Request>(&self, request: &R) -> Result<R::Response, Error> {
        loop {
            // Send new messages over new connection if it exists
            let has_changed = self.new_client_rx.borrow().has_changed();
            if has_changed {
                self.new_client_rx.borrow_mut().mark_seen();
                *self.client.borrow_mut() = self.new_client_rx.borrow().get_cloned();
            }

            #[cfg(not(feature = "internal_test"))]
            let result = self
                .client
                .borrow()
                .send(request)
                .timeout(self.timeout)
                .await;
            // Allow error injection in tests
            #[cfg(feature = "internal_test")]
            let result = {
                let inject_error = self.inject_error.borrow_mut().take();
                if let Some(error) = inject_error {
                    Err(timeout::Error::Failed(error))
                } else {
                    self.client
                        .borrow()
                        .send(request)
                        .timeout(self.timeout)
                        .await
                }
            };

            match result {
                Ok(response) => return Ok(response),
                // Reconnect on timeout
                Err(timeout::Error::Expired) => (),
                Err(timeout::Error::Failed(error)) => {
                    if let error @ Error::Protocol(_) = error {
                        // Protocol errors can't be solved by reconnect,
                        // reconnect in all other cases
                        return Err(error);
                    }
                }
            }
            // Try to reconnect
            loop {
                if self.reconnect().timeout(self.timeout).await.is_ok() {
                    break;
                }
            }
        }
    }
}

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;
    use crate::fiber;
    use crate::network::AsClient as _;
    use crate::test::util::TARANTOOL_LISTEN;

    async fn test_client() -> Client {
        Client::connect_with_config(
            "localhost".to_string(),
            TARANTOOL_LISTEN,
            protocol::Config {
                creds: Some(("test_user".into(), "password".into())),
            },
            Duration::from_secs(1),
        )
        .timeout(Duration::from_secs(3))
        .await
        .unwrap()
    }

    #[crate::test(tarantool = "crate")]
    fn connect_failure() {
        fiber::block_on(async {
            // Can be any other unused port
            let err = Client::connect("localhost".to_string(), 3300, Duration::from_secs(3))
                .await
                .unwrap_err();
            assert!(matches!(dbg!(err), Error::Tcp(_)))
        });
    }

    #[crate::test(tarantool = "crate")]
    fn ping_after_reconnect() {
        fiber::block_on(async {
            let client = test_client().await;

            for _ in 0..2 {
                client.ping().timeout(Duration::from_secs(3)).await.unwrap();
            }
            client.reconnect().await.unwrap();
            for _ in 0..2 {
                client.ping().timeout(Duration::from_secs(3)).await.unwrap();
            }
        });
    }

    #[crate::test(tarantool = "crate")]
    fn reconnect_on_network_error() {
        fiber::block_on(async {
            let client = test_client().await;

            client.inject_error(Error::Io(Rc::new(
                std::io::ErrorKind::ConnectionAborted.into(),
            )));
            client.ping().timeout(Duration::from_secs(3)).await.unwrap();
            assert_eq!(client.reconnect_count(), 1);

            client.inject_error(Error::Io(Rc::new(
                std::io::ErrorKind::ConnectionAborted.into(),
            )));
            client.ping().timeout(Duration::from_secs(3)).await.unwrap();
            assert_eq!(client.reconnect_count(), 2);
        });
    }

    #[crate::test(tarantool = "crate")]
    fn dont_reconnect() {
        fiber::block_on(async {
            let client = test_client().await;

            // No error
            client.ping().timeout(Duration::from_secs(3)).await.unwrap();
            assert_eq!(client.reconnect_count(), 0);

            // User error
            client.inject_error(Error::Protocol(Rc::new(protocol::Error::Response(
                protocol::ResponseError {
                    message: "server answered with err".to_string(),
                },
            ))));
            client
                .ping()
                .timeout(Duration::from_secs(3))
                .await
                .unwrap_err();
            assert_eq!(client.reconnect_count(), 0);
        });
    }

    #[crate::test(tarantool = "crate")]
    fn old_connection_remains_for_old_request() {
        let lua = crate::global_lua();
        lua.exec(
            "fiber = require('fiber')
            _G.reconnect_test_chan = fiber.channel()",
        )
        .unwrap();
        let client = fiber::block_on(test_client());
        let client_clone = client.clone();
        let jh = fiber::defer_async(async move {
            client_clone.reconnect().await.unwrap();
            assert_eq!(client_clone.reconnect_count(), 1);
            lua.exec("_G.reconnect_test_chan:put(42)").unwrap();
        });
        fiber::block_on(async move {
            // reconnect will happen during this request
            let result = client
                .eval("return _G.reconnect_test_chan:get()", &())
                .await
                .unwrap()
                .unwrap()
                .decode::<(i32,)>()
                .unwrap();
            // value received on an old connection, though there was a reconnect request
            assert_eq!(result, (42,));
            // Globally the client has 1 reconnection
            assert_eq!(client.reconnect_count(), 1);
            // but this clone of a client does not yet use the new connection
            assert!(client.new_client_rx.borrow().has_changed());

            client.ping().timeout(Duration::from_secs(3)).await.unwrap();
            // it does use the new connection for the new request
            assert!(!client.new_client_rx.borrow().has_changed());
        });
        jh.join();
    }
}
