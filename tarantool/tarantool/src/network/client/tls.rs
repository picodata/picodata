//! Contains an implementation of a custom async coio based [`TlsStream`].
//!
//! [`TlsStream`] is an asynchronous wrapper around [`ssl::SslStream<TcpStream>`]
//! for use with Rust's async/await functionality.
//!
//! Unlike [`ssl::SslStream<S>`], which only works with synchronous `S: Read + Write` types,
//! [`TlsStream`] implements async traits. The underlying [`TcpStream`] implements both [`Read`]
//! and [`Write`] to enable [`ssl::SslStream<TcpStream>`] to function. Internally, [`TcpStream`]
//! uses non-blocking sockets.

use super::tcp::TcpStream;
use crate::ffi::tarantool as ffi;
use crate::fiber::r#async::context::ContextExt;
use futures::{AsyncRead, AsyncWrite};
use openssl::{ssl, x509};
use std::cell::RefCell;
use std::future;
use std::io;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

pub struct TlsConfig<'a> {
    pub cert_file: &'a PathBuf,
    pub key_file: &'a PathBuf,
    pub ca_file: Option<&'a PathBuf>,
}

/// Wrapper around [`ssl::SslConnector`] that configures TLS settings.
#[derive(Debug, Clone)]
pub struct TlsConnector(ssl::SslConnector);

impl TlsConnector {
    pub fn new(config: TlsConfig) -> io::Result<Self> {
        let mut builder = ssl::SslConnector::builder(ssl::SslMethod::tls())?;
        builder.set_verify(ssl::SslVerifyMode::PEER);
        builder.set_certificate_file(config.cert_file, ssl::SslFiletype::PEM)?;
        builder.set_private_key_file(config.key_file, ssl::SslFiletype::PEM)?;

        if let Some(ca_file) = config.ca_file {
            let pem = std::fs::read(ca_file)?;
            let certs = x509::X509::stack_from_pem(&pem)?;
            let mut store_builder = x509::store::X509StoreBuilder::new()?;
            certs
                .into_iter()
                .try_for_each(|c| store_builder.add_cert(c))?;
            builder.set_verify_cert_store(store_builder.build())?;
        }

        Ok(Self(builder.build()))
    }

    pub fn connect(
        &self,
        stream: TcpStream,
        domain: &str,
    ) -> Result<ssl::SslStream<TcpStream>, ssl::HandshakeError<TcpStream>> {
        self.0.connect(domain, stream)
    }
}

/// An asynchronous wrapper around [`ssl::SslStream<TcpStream>`]
/// (to use with Rust async/await).
#[derive(Debug, Clone)]
pub struct TlsStream {
    inner: Rc<RefCell<ssl::SslStream<TcpStream>>>,
}

impl TlsStream {
    pub async fn connect(
        connector: &TlsConnector,
        stream: TcpStream,
        domain: &str,
    ) -> io::Result<Self> {
        let fd = stream.fd()?;

        let res = connector.connect(stream, domain);
        let mut mid_handshake_ssl_stream = match res {
            Ok(stream) => {
                return Ok(Self {
                    inner: Rc::new(RefCell::new(stream)),
                });
            }
            Err(ssl::HandshakeError::WouldBlock(m)) => Some(m),
            Err(e) => return Err(io::Error::other(e)),
        };

        let stream = future::poll_fn(|cx| {
            let mid = mid_handshake_ssl_stream
                .take()
                .expect("taken once per poll");
            match mid.handshake() {
                Ok(stream) => Poll::Ready(Ok(stream)),
                Err(ssl::HandshakeError::WouldBlock(next_mid)) => {
                    let event = if next_mid.error().code() == ssl::ErrorCode::WANT_READ {
                        ffi::CoIOFlags::READ
                    } else {
                        ffi::CoIOFlags::WRITE
                    };
                    mid_handshake_ssl_stream = Some(next_mid);
                    // SAFETY: safe as long as this future is executed by `fiber::block_on` async executor.
                    unsafe {
                        ContextExt::set_coio_wait(cx, fd, event);
                    }
                    Poll::Pending
                }
                Err(e) => Poll::Ready(Err(io::Error::other(e))),
            }
        })
        .await?;

        Ok(Self {
            inner: Rc::new(RefCell::new(stream)),
        })
    }

    pub fn shutdown(&self) -> io::Result<()> {
        self.inner
            .borrow_mut()
            .shutdown()
            .map_err(|e| e.into_io_error().unwrap_or_else(io::Error::other))?;
        self.inner.borrow().get_ref().close()
    }
}

impl AsyncWrite for TlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = this.inner.borrow_mut().write(buf);
        match result {
            Ok(num) => Poll::Ready(Ok(num)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                let raw_fd = this.inner.borrow().get_ref().fd()?;
                // SAFETY: safe as long as this future is executed by `fiber::block_on` async executor.
                unsafe {
                    ContextExt::set_coio_wait(cx, raw_fd, ffi::CoIOFlags::WRITE);
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Poll::Ready(this.shutdown())
    }
}

impl AsyncRead for TlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = this.inner.borrow_mut().read(buf);
        match result {
            Ok(num) => Poll::Ready(Ok(num)),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                let raw_fd = this.inner.borrow().get_ref().fd()?;
                // SAFETY: safe as long as this future is executed by `fiber::block_on` async executor.
                unsafe {
                    ContextExt::set_coio_wait(cx, raw_fd, ffi::CoIOFlags::READ);
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}
