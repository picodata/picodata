//! Stream API for plugin connections.
//!
//! This module provides `PicoStream` which can be either a plain TCP stream
//! or a TLS-encrypted stream, both using Tarantool's cooperative I/O.

use openssl::error::ErrorStack;
use openssl::ssl::{self, SslStream};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::time::Duration;
use tarantool::coio::CoIOStream;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////

/// Error that can occur during TLS handshake.
#[derive(Error, Debug)]
pub enum TlsHandshakeError {
    /// Error during TLS setup.
    #[error("setup failure: {0}")]
    SetupFailure(ErrorStack),

    /// Error during handshake.
    #[error("handshake error: {0}")]
    Failure(ssl::Error),
}

/// Error that can occur when accepting a connection.
#[derive(Error, Debug)]
pub enum PicoStreamError {
    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// IO error.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// TLS handshake error.
    #[error("tls error: {0}")]
    Tls(#[from] TlsHandshakeError),
}

////////////////////////////////////////////////////////////////////////////////
// PicoStream
////////////////////////////////////////////////////////////////////////////////

/// A stream that can be either plain or TLS-encrypted.
///
/// Uses Tarantool's cooperative I/O stream for non-blocking operations.
pub struct PicoStream {
    inner: PicoStreamImpl,
}

enum PicoStreamImpl {
    Plain(CoIOStream),
    Tls(SslStream<CoIOStream>),
}

impl PicoStream {
    /// Creates a new plain stream.
    pub fn plain(stream: CoIOStream) -> Self {
        Self {
            inner: PicoStreamImpl::Plain(stream),
        }
    }

    /// Creates a new TLS-encrypted stream.
    pub fn tls(stream: SslStream<CoIOStream>) -> Self {
        Self {
            inner: PicoStreamImpl::Tls(stream),
        }
    }

    /// Returns true if this stream is TLS-encrypted.
    pub fn is_tls(&self) -> bool {
        matches!(self.inner, PicoStreamImpl::Tls(_))
    }

    /// Sets or clears the `TCP_NODELAY` option on the underlying socket.
    ///
    /// When enabled, disables Nagle's algorithm for lower latency at the cost
    /// of potentially higher network overhead for small writes.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        use std::os::fd::BorrowedFd;
        let fd = self.as_inner().as_raw_fd();
        // SAFETY: stream contains a valid descriptor
        let fd = unsafe { BorrowedFd::borrow_raw(fd) };
        socket2::SockRef::from(&fd).set_nodelay(nodelay)
    }

    /// Returns a reference to the underlying `CoIOStream`.
    fn as_inner(&self) -> &CoIOStream {
        match &self.inner {
            PicoStreamImpl::Plain(s) => s,
            PicoStreamImpl::Tls(s) => s.get_ref(),
        }
    }

    /// Reads data from the stream with a timeout.
    ///
    /// This method provides timeout functionality similar to CoIOStream::read_with_timeout.
    /// For plain connections, it delegates to the underlying CoIOStream.
    /// For TLS connections, implements timeout using Tarantool's fiber API.
    ///
    /// # Parameters
    ///
    /// - `buf`: Buffer to read data into
    /// - `timeout`: Optional timeout duration
    ///
    /// # Returns
    ///
    /// - `Ok(n)`: Number of bytes read
    /// - `Err(e)`: IO error (including TimedOut if timeout expires)
    pub fn read_with_timeout(
        &mut self,
        buf: &mut [u8],
        timeout: Option<Duration>,
    ) -> io::Result<usize> {
        match &mut self.inner {
            PicoStreamImpl::Plain(s) => s.read_with_timeout(buf, timeout),
            PicoStreamImpl::Tls(s) => {
                if let Some(timeout_duration) = timeout {
                    read_tls_with_timeout(s, buf, timeout_duration)
                } else {
                    s.read(buf)
                }
            }
        }
    }
}

/// Helper function to read from TLS stream with timeout using fiber API.
fn read_tls_with_timeout(
    ssl_stream: &mut SslStream<CoIOStream>,
    buf: &mut [u8],
    timeout: Duration,
) -> io::Result<usize> {
    use tarantool::ffi::tarantool as ffi;

    match ssl_stream.read(buf) {
        Ok(n) => return Ok(n),
        Err(e) if e.kind() != io::ErrorKind::WouldBlock => return Err(e),
        _ => {}
    }

    let fd = ssl_stream.get_ref().as_raw_fd();
    let timeout_secs = timeout.as_secs_f64();

    match unsafe { ffi::coio_wait(fd, ffi::CoIOFlags::READ.bits(), timeout_secs) } {
        0 => Err(io::Error::new(io::ErrorKind::TimedOut, "read timeout")),
        _ => ssl_stream.read(buf),
    }
}

impl Read for PicoStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.inner {
            PicoStreamImpl::Plain(s) => s.read(buf),
            PicoStreamImpl::Tls(s) => s.read(buf),
        }
    }
}

impl Write for PicoStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &mut self.inner {
            PicoStreamImpl::Plain(s) => s.write(buf),
            PicoStreamImpl::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.inner {
            PicoStreamImpl::Plain(s) => s.flush(),
            PicoStreamImpl::Tls(s) => s.flush(),
        }
    }
}
