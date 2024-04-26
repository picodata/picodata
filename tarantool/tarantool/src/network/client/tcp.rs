//! Contains an implementation of a custom async coio based [`TcpStream`].
//!
//! ## Example
//! ```no_run
//! # async {
//! use futures::AsyncReadExt;
//! use tarantool::network::client::tcp::TcpStream;
//!
//! let mut stream = TcpStream::connect("localhost", 8080)
//!     .unwrap();
//! let mut buf = vec![];
//! let read_size = stream
//!     .read(&mut buf)
//!     .await
//!     .unwrap();
//! # };
//! ```

use std::cell::Cell;
use std::ffi::{CString, NulError};
use std::io;
use std::mem::{self, MaybeUninit};
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

#[cfg(feature = "async-std")]
use async_std::io::{Read as AsyncRead, Write as AsyncWrite};
#[cfg(not(feature = "async-std"))]
use futures::{AsyncRead, AsyncWrite};

use crate::ffi::tarantool as ffi;
use crate::fiber;
use crate::fiber::r#async::context::ContextExt;
use crate::time::Instant;

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("failed to resolve domain name '{0}'")]
    ResolveAddress(String),
    #[error("input parameters contain ffi incompatible strings: {0}")]
    ConstructCString(NulError),
    #[error("failed to connect to address '{address}': {error}")]
    Connect { error: io::Error, address: String },
    #[error("unknown address family: {0}")]
    UnknownAddressFamily(u16),
    #[error("write half of the stream is closed")]
    WriteClosed,
    #[error("connect timeout")]
    Timeout,
}

/// Async TcpStream based on fibers and coio.
///
/// Use [timeout][t] on top of read or write operations on [`TcpStream`]
/// to set the max time to wait for an operation.
///
/// Atention should be payed that [`TcpStream`] is not [`futures::select`] friendly when awaiting multiple streams
/// As there is no coio support to await multiple file descriptors yet.
/// Though it can be used with [`futures::join`] without problems.
///
/// See module level [documentation](super::tcp) for examples.
///
/// [t]: crate::fiber::async::timeout::timeout
#[derive(Debug, Clone)]
pub struct TcpStream {
    /// A raw tcp socket file descriptor. Replaced with `None` when the stream
    /// is closed.
    ///
    /// Note that it's wrapped in a `Rc`, because the outer `TcpStream` needs to
    /// be mutably borrowable (thanks to AsyncWrite & AsyncRead traits) and it
    /// doesn't make sense to wrap it in a Mutex of any sort, because it's
    /// perfectly safe to read & write on a tcp socket even from concurrent threads,
    /// but we only use it from different fibers.
    fd: Rc<Cell<Option<RawFd>>>,
}

impl TcpStream {
    /// Creates a [`TcpStream`] to `url` and `port`.
    ///
    /// - `host` - url, i.e. "localhost"
    /// - `port` - port, i.e. 8080
    ///
    /// This functions makes the fiber **yield**.
    pub fn connect(url: &str, port: u16) -> Result<Self, Error> {
        Self::connect_timeout(url, port, Duration::MAX)
    }

    /// Creates a [`TcpStream`] to `url` and `port` with provided `timeout`.
    ///
    /// - `host` - url, i.e. "localhost"
    /// - `port` - port, i.e. 8080
    /// - `timeout` - timeout
    ///
    /// This functions makes the fiber **yield**.
    pub fn connect_timeout(url: &str, port: u16, timeout: Duration) -> Result<Self, Error> {
        let deadline = fiber::clock().saturating_add(timeout);

        // SAFETY: it is just simple sys call
        let (v4_addrs, v6_addrs) = unsafe { resolve_addr(url, port, timeout.as_secs_f64())? };

        let mut last_error = None;

        for v4_addr in v4_addrs {
            match Self::connect_single(LibcSocketAddr::V4(v4_addr), deadline) {
                Ok(stream) => {
                    return Ok(stream);
                }
                Err(e) => last_error = Some(e),
            }
        }

        for v6_addr in v6_addrs {
            match Self::connect_single(LibcSocketAddr::V6(v6_addr), deadline) {
                Ok(stream) => {
                    return Ok(stream);
                }
                Err(e) => last_error = Some(e),
            }
        }

        if let Some(error) = last_error {
            if let io::ErrorKind::TimedOut = error.kind() {
                Err(Error::Timeout)
            } else {
                Err(Error::Connect {
                    error,
                    address: format!("{url}:{port}"),
                })
            }
        } else {
            Err(Error::ResolveAddress(url.into()))
        }
    }

    fn connect_single(socket_addr: LibcSocketAddr, deadline: Instant) -> io::Result<Self> {
        let (kind, addr, addr_len);
        match &socket_addr {
            LibcSocketAddr::V4(v4) => {
                kind = libc::AF_INET;
                addr = v4 as *const libc::sockaddr_in as *const libc::sockaddr;
                addr_len = mem::size_of::<libc::sockaddr_in>();
            }
            LibcSocketAddr::V6(v6) => {
                kind = libc::AF_INET6;
                addr = v6 as *const libc::sockaddr_in6 as *const libc::sockaddr;
                addr_len = mem::size_of::<libc::sockaddr_in6>();
            }
        }
        let fd = nonblocking_socket(kind)?;

        let res = cvt(unsafe { libc::connect(fd.0, addr, addr_len as _) });
        if let Err(io_error) = res {
            if io_error.raw_os_error() != Some(libc::EINPROGRESS) {
                return Err(io_error);
            } else {
                // Need to block the fiber until the connection result is known
            }

            let timeout = deadline.duration_since(fiber::clock());
            crate::coio::coio_wait(fd.0, ffi::CoIOFlags::WRITE, timeout.as_secs_f64())?;

            // This is safe, because fd is still open.
            unsafe { check_socket_error(fd.0)? };

            // If no error, then connection is established
        };

        // If this allocation panics the fd will still be closed
        let result = Self {
            fd: Rc::new(Cell::new(None)),
        };
        // Now TcpStream owns the fd and takes responsibility of closing it.
        result.fd.set(Some(fd.into_inner()));
        return Ok(result);
    }

    #[inline(always)]
    #[track_caller]
    pub fn close(&mut self) -> io::Result<()> {
        let Some(fd) = self.fd.take() else {
            // Already closed.
            return Ok(());
        };

        // SAFETY: safe because we close the `fd` only once
        let rc = unsafe { ffi::coio_close(fd) };
        if rc != 0 {
            let e = io::Error::last_os_error();
            if e.raw_os_error() == Some(libc::EBADF) {
                crate::say_error!("close({fd}): Bad file descriptor");
                if cfg!(debug_assertions) {
                    panic!("close({}): Bad file descriptor", fd);
                }
            }
            return Err(e);
        }
        Ok(())
    }
}

fn cvt(t: libc::c_int) -> io::Result<libc::c_int> {
    if t == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(t)
    }
}

#[cfg(target_os = "linux")]
#[inline(always)]
fn nonblocking_socket(kind: libc::c_int) -> io::Result<AutoCloseFd> {
    let fd = unsafe {
        cvt(libc::socket(
            kind,
            libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
            0,
        ))?
    };

    let fd = AutoCloseFd(fd);

    Ok(fd)
}

#[cfg(target_os = "macos")]
fn nonblocking_socket(kind: libc::c_int) -> io::Result<AutoCloseFd> {
    // This is safe because `libc::socket` doesn't do undefined behavior
    let fd = unsafe { cvt(libc::socket(kind, libc::SOCK_STREAM, 0))? };
    let fd = AutoCloseFd(fd);

    // This is safe because fd is open.
    unsafe {
        cvt(libc::ioctl(fd.0, libc::FIOCLEX))?;
    }

    // This is safe because fd is open and the opt_value buffer specification is valid.
    unsafe {
        let opt_value: libc::c_int = 1;
        cvt(libc::setsockopt(
            fd.0,
            libc::SOL_SOCKET,
            libc::SO_NOSIGPIPE,
            &opt_value as *const _ as *const libc::c_void,
            mem::size_of_val(&opt_value) as _,
        ))?;
    }

    // This is safe because fd is open.
    unsafe {
        cvt(libc::ioctl(fd.0, libc::FIONBIO, &mut 1))?;
    }

    Ok(fd)
}

/// A wrapper around a raw file descriptor, which automatically closes the
/// descriptor if dropped.
/// Use [`Self::into_inner`] to disable the automatic close on drop.
///
/// TODO: consider using [`std::os::fd::OwnedFd`] instead
struct AutoCloseFd(RawFd);

impl AutoCloseFd {
    #[inline(always)]
    fn into_inner(self) -> RawFd {
        let fd = self.0;
        std::mem::forget(self);
        fd
    }
}

impl Drop for AutoCloseFd {
    fn drop(&mut self) {
        // Safe as long as we only store open file descriptors
        let rc = unsafe { libc::close(self.0) };
        if rc != 0 {
            crate::say_error!(
                "failed closing socket descriptor: {}",
                io::Error::last_os_error()
            );
        }
    }
}

unsafe fn check_socket_error(fd: RawFd) -> io::Result<()> {
    let mut val: libc::c_int = mem::zeroed();
    let mut val_len = mem::size_of::<libc::c_int>() as libc::socklen_t;
    cvt(libc::getsockopt(
        fd,
        libc::SOL_SOCKET,
        libc::SO_ERROR,
        &mut val as *mut libc::c_int as *mut _,
        &mut val_len,
    ))?;
    match val {
        0 => Ok(()),
        v => Err(io::Error::from_raw_os_error(v as i32)),
    }
}

unsafe fn resolve_addr(
    url: &str,
    port: u16,
    timeout: f64,
) -> Result<(Vec<libc::sockaddr_in>, Vec<libc::sockaddr_in6>), Error> {
    let mut hints = MaybeUninit::<libc::addrinfo>::zeroed().assume_init();
    hints.ai_family = libc::AF_UNSPEC;
    hints.ai_socktype = libc::SOCK_STREAM;

    let host = CString::new(url).map_err(Error::ConstructCString)?;
    let addrinfo = match crate::coio::getaddrinfo(&host, None, &hints, timeout) {
        Ok(v) => v,
        Err(e) => {
            match e {
                crate::error::Error::IO(ref ee) => {
                    if let io::ErrorKind::TimedOut = ee.kind() {
                        return Err(Error::Timeout);
                    }
                }
                crate::error::Error::Tarantool(ref ee) => {
                    if let Some(ref kind) = ee.error_type {
                        let kind: &str = kind;
                        if kind == "TimedOut" {
                            return Err(Error::Timeout);
                        }
                    }
                }
                _ => (),
            }
            crate::say_error!("coio_getaddrinfo failed: {e}");
            return Err(Error::ResolveAddress(url.into()));
        }
    };

    let mut ipv4_addresses = Vec::with_capacity(2);
    let mut ipv6_addresses = Vec::with_capacity(2);
    let mut current = addrinfo;

    while !current.is_null() {
        let ai = *current;
        match ai.ai_family {
            libc::AF_INET => {
                let mut sockaddr = *(ai.ai_addr as *mut libc::sockaddr_in);
                sockaddr.sin_port = port.to_be();
                ipv4_addresses.push(sockaddr);
            }
            libc::AF_INET6 => {
                let mut sockaddr = *(ai.ai_addr as *mut libc::sockaddr_in6);
                sockaddr.sin6_port = port.to_be();
                ipv6_addresses.push(sockaddr);
            }
            af => {
                libc::freeaddrinfo(addrinfo);
                return Err(Error::UnknownAddressFamily(af as u16));
            }
        }
        current = ai.ai_next;
    }

    libc::freeaddrinfo(addrinfo);

    Ok((ipv4_addresses, ipv6_addresses))
}

enum LibcSocketAddr {
    V4(libc::sockaddr_in),
    V6(libc::sockaddr_in6),
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let Some(fd) = self.fd.get() else {
            let e = io::Error::new(io::ErrorKind::Other, "socket closed already");
            return Poll::Ready(Err(e));
        };

        let (result, err) = (
            // `self.fd` must be nonblocking for this to work correctly
            unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, buf.len()) },
            io::Error::last_os_error(),
        );

        if result >= 0 {
            return Poll::Ready(Ok(result as usize));
        }
        match err.kind() {
            io::ErrorKind::WouldBlock => {
                // SAFETY: Safe as long as this future is executed by
                // `fiber::block_on` async executor.
                unsafe { ContextExt::set_coio_wait(cx, fd, ffi::CoIOFlags::WRITE) }
                Poll::Pending
            }
            io::ErrorKind::Interrupted => {
                // Return poll pending without setting coio wait
                // so that write can be retried immediately.
                //
                // SAFETY: Safe as long as this future is executed by
                // `fiber::block_on` async executor.
                unsafe { ContextExt::set_deadline(cx, fiber::clock()) }
                Poll::Pending
            }
            _ => Poll::Ready(Err(err)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.fd.get().is_none() {
            let e = io::Error::new(io::ErrorKind::Other, "socket closed already");
            return Poll::Ready(Err(e));
        };

        // [`TcpStream`] similarily to std does not buffer anything,
        // so there is nothing to flush.
        //
        // If buffering is needed use [`futures::io::BufWriter`] on top of this stream.
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.fd.get().is_none() {
            let e = io::Error::new(io::ErrorKind::Other, "socket closed already");
            return Poll::Ready(Err(e));
        };

        let res = self.close();
        Poll::Ready(res)
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let Some(fd) = self.fd.get() else {
            let e = io::Error::new(io::ErrorKind::Other, "socket closed already");
            return Poll::Ready(Err(e));
        };

        let (result, err) = (
            // `self.fd` must be nonblocking for this to work correctly
            unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) },
            io::Error::last_os_error(),
        );

        if result >= 0 {
            return Poll::Ready(Ok(result as usize));
        }
        match err.kind() {
            io::ErrorKind::WouldBlock => {
                // SAFETY: Safe as long as this future is executed by
                // `fiber::block_on` async executor.
                unsafe { ContextExt::set_coio_wait(cx, fd, ffi::CoIOFlags::READ) }
                Poll::Pending
            }
            io::ErrorKind::Interrupted => {
                // Return poll pending without setting coio wait
                // so that read can be retried immediately.
                //
                // SAFETY: Safe as long as this future is executed by
                // `fiber::block_on` async executor.
                unsafe { ContextExt::set_deadline(cx, fiber::clock()) }
                Poll::Pending
            }
            _ => Poll::Ready(Err(err)),
        }
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            crate::say_error!("TcpStream::drop: closing tcp stream failed: {e}");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// UnsafeSendSyncTcpStream
////////////////////////////////////////////////////////////////////////////////

/// A wrapper around [`TcpStream`] which also implements [`Send`] & [`Sync`].
///
/// Note that it's actually *not safe* to use this stream outside the thread in
/// which it was created, because it's implemented on top of the tarantool's
/// fiber runtime. This wrapper only exists because of the cancerous `Send + Sync`
/// trait bounds placed on almost all third-party async code. These bounds aren't
/// necessary when working with our async runtime, which is single threaded.
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct UnsafeSendSyncTcpStream(pub TcpStream);

unsafe impl Send for UnsafeSendSyncTcpStream {}
unsafe impl Sync for UnsafeSendSyncTcpStream {}

impl AsyncRead for UnsafeSendSyncTcpStream {
    #[inline(always)]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        AsyncRead::poll_read(Pin::new(&mut self.0), cx, buf)
    }
}

impl AsyncWrite for UnsafeSendSyncTcpStream {
    #[inline(always)]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        AsyncWrite::poll_write(Pin::new(&mut self.0), cx, buf)
    }

    #[inline(always)]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_flush(Pin::new(&mut self.0), cx)
    }

    #[inline(always)]
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        AsyncWrite::poll_close(Pin::new(&mut self.0), cx)
    }
}

////////////////////////////////////////////////////////////////////////////////
// tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;

    use crate::fiber;
    use crate::fiber::r#async::timeout;
    use crate::fiber::r#async::timeout::IntoTimeout as _;
    use crate::test::util::always_pending;
    use crate::test::util::listen_port;

    use std::collections::HashSet;
    use std::net;
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    use futures::{AsyncReadExt, AsyncWriteExt, FutureExt};
    use pretty_assertions::assert_eq;

    const _10_SEC: Duration = Duration::from_secs(10);
    const _0_SEC: Duration = Duration::from_secs(0);

    #[inline(always)]
    fn to_socket_addr_v4(sockaddr: libc::sockaddr_in) -> net::SocketAddrV4 {
        net::SocketAddrV4::new(
            net::Ipv4Addr::from(u32::from_be(sockaddr.sin_addr.s_addr)),
            u16::from_be(sockaddr.sin_port),
        )
    }

    #[inline(always)]
    fn to_socket_addr_v6(sockaddr: libc::sockaddr_in6) -> net::SocketAddrV6 {
        // Safety: safe because sizes match
        let be_addr = unsafe { std::mem::transmute_copy(&sockaddr.sin6_addr.s6_addr) };
        net::SocketAddrV6::new(
            net::Ipv6Addr::from(u128::from_be(be_addr)),
            u16::from_be(sockaddr.sin6_port),
            sockaddr.sin6_flowinfo,
            sockaddr.sin6_scope_id,
        )
    }

    #[crate::test(tarantool = "crate")]
    async fn get_libc_addrs() {
        let (addrs_v4, addrs_v6) =
            unsafe { resolve_addr("example.org", 80, _10_SEC.as_secs_f64()).unwrap() };

        let mut our_addrs = HashSet::<net::SocketAddr>::new();
        for v4 in addrs_v4 {
            our_addrs.insert(to_socket_addr_v4(v4).into());
        }
        for v6 in addrs_v6 {
            our_addrs.insert(to_socket_addr_v6(v6).into());
        }

        let addrs_from_std: HashSet<_> = net::ToSocketAddrs::to_socket_addrs(&("example.org", 80))
            .unwrap()
            .collect();

        assert_eq!(our_addrs, addrs_from_std);

        //
        // check what happens with "localhost"
        //
        let (addrs_v4, addrs_v6) =
            unsafe { resolve_addr("localhost", 1337, _10_SEC.as_secs_f64()).unwrap() };

        let mut our_addrs = HashSet::<net::SocketAddr>::new();
        for v4 in addrs_v4 {
            our_addrs.insert(to_socket_addr_v4(v4).into());
        }
        for v6 in addrs_v6 {
            our_addrs.insert(to_socket_addr_v6(v6).into());
        }

        let addrs_from_std: HashSet<_> = net::ToSocketAddrs::to_socket_addrs(&("localhost", 1337))
            .unwrap()
            .collect();

        assert_eq!(our_addrs, addrs_from_std);
    }

    #[crate::test(tarantool = "crate")]
    async fn get_libc_addrs_error() {
        let err = unsafe {
            resolve_addr("invalid domain name", 80, _10_SEC.as_secs_f64())
                .unwrap_err()
                .to_string()
        };

        assert_eq!(err, "failed to resolve domain name 'invalid domain name'");
    }

    #[crate::test(tarantool = "crate")]
    fn connect() {
        let _ = TcpStream::connect("localhost", listen_port()).unwrap();
    }

    #[crate::test(tarantool = "crate")]
    fn connect_timeout() {
        let _ = TcpStream::connect_timeout("localhost", listen_port(), _10_SEC).unwrap();
    }

    #[crate::test(tarantool = "crate")]
    fn connect_zero_timeout() {
        assert!(matches!(
            TcpStream::connect_timeout("example.com", 80, _0_SEC)
                .err()
                .unwrap(),
            Error::Timeout
        ));
    }

    #[crate::test(tarantool = "crate")]
    async fn read() {
        let mut stream = TcpStream::connect_timeout("localhost", listen_port(), _10_SEC).unwrap();
        // Read greeting
        let mut buf = vec![0; 128];
        stream.read_exact(&mut buf).timeout(_10_SEC).await.unwrap();
    }

    #[crate::test(tarantool = "crate")]
    async fn read_timeout() {
        let mut stream = TcpStream::connect_timeout("localhost", listen_port(), _10_SEC).unwrap();
        // Read greeting
        let mut buf = vec![0; 128];
        assert_eq!(
            stream
                .read_exact(&mut buf)
                .timeout(_0_SEC)
                .await
                .unwrap_err()
                .to_string(),
            "deadline expired"
        );
    }

    #[crate::test(tarantool = "crate")]
    fn write() {
        let (sender, receiver) = std::sync::mpsc::channel();
        let listener = TcpListener::bind("127.0.0.1:3302").unwrap();
        // Spawn listener
        thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut buf = vec![];
                <std::net::TcpStream as std::io::Read>::read_to_end(&mut stream, &mut buf).unwrap();
                sender.send(buf).unwrap();
            }
        });
        // Send data
        {
            fiber::block_on(async {
                let mut stream = TcpStream::connect_timeout("localhost", 3302, _10_SEC).unwrap();
                timeout::timeout(_10_SEC, stream.write_all(&[1, 2, 3]))
                    .await
                    .unwrap();
                timeout::timeout(_10_SEC, stream.write_all(&[4, 5]))
                    .await
                    .unwrap();
            });
        }
        let buf = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(buf, vec![1, 2, 3, 4, 5])
    }

    #[crate::test(tarantool = "crate")]
    fn split() {
        let (sender, receiver) = std::sync::mpsc::channel();
        let listener = TcpListener::bind("127.0.0.1:3303").unwrap();
        // Spawn listener
        thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut buf = vec![0; 5];
                <std::net::TcpStream as std::io::Read>::read_exact(&mut stream, &mut buf).unwrap();
                <std::net::TcpStream as std::io::Write>::write_all(&mut stream, &buf.clone())
                    .unwrap();
                sender.send(buf).unwrap();
            }
        });
        // Send and read data
        {
            let stream = TcpStream::connect_timeout("localhost", 3303, _10_SEC).unwrap();
            let (mut reader, mut writer) = stream.split();
            let reader_handle = fiber::start_async(async move {
                let mut buf = vec![0; 5];
                timeout::timeout(_10_SEC, reader.read_exact(&mut buf))
                    .await
                    .unwrap();
                assert_eq!(buf, vec![1, 2, 3, 4, 5])
            });
            let writer_handle = fiber::start_async(async move {
                timeout::timeout(_10_SEC, writer.write_all(&[1, 2, 3]))
                    .await
                    .unwrap();
                timeout::timeout(_10_SEC, writer.write_all(&[4, 5]))
                    .await
                    .unwrap();
            });
            writer_handle.join();
            reader_handle.join();
        }
        let buf = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(buf, vec![1, 2, 3, 4, 5])
    }

    #[crate::test(tarantool = "crate")]
    fn join_correct_timeout() {
        {
            fiber::block_on(async {
                let mut stream =
                    TcpStream::connect_timeout("localhost", listen_port(), _10_SEC).unwrap();
                // Read greeting
                let mut buf = vec![0; 128];
                let (is_err, is_ok) = futures::join!(
                    timeout::timeout(_0_SEC, always_pending()),
                    timeout::timeout(_10_SEC, stream.read_exact(&mut buf))
                );
                assert_eq!(is_err.unwrap_err().to_string(), "deadline expired");
                is_ok.unwrap();
            });
        }
        // Testing with different order in join
        {
            fiber::block_on(async {
                let mut stream =
                    TcpStream::connect_timeout("localhost", listen_port(), _10_SEC).unwrap();
                // Read greeting
                let mut buf = vec![0; 128];
                let (is_ok, is_err) = futures::join!(
                    timeout::timeout(_10_SEC, stream.read_exact(&mut buf)),
                    timeout::timeout(_0_SEC, always_pending())
                );
                assert_eq!(is_err.unwrap_err().to_string(), "deadline expired");
                is_ok.unwrap();
            });
        }
    }

    #[crate::test(tarantool = "crate")]
    fn select_correct_timeout() {
        {
            fiber::block_on(async {
                let mut stream =
                    TcpStream::connect_timeout("localhost", listen_port(), _10_SEC).unwrap();
                // Read greeting
                let mut buf = vec![0; 128];
                let f1 = timeout::timeout(_0_SEC, always_pending()).fuse();
                let f2 = timeout::timeout(_10_SEC, stream.read_exact(&mut buf)).fuse();
                futures::pin_mut!(f1);
                futures::pin_mut!(f2);
                let is_err = futures::select!(
                    res = f1 => res.is_err(),
                    res = f2 => res.is_err()
                );
                assert!(is_err);
            });
        }
        // Testing with different future timeouting first
        {
            fiber::block_on(async {
                let mut stream =
                    TcpStream::connect_timeout("localhost", listen_port(), _10_SEC).unwrap();
                // Read greeting
                let mut buf = vec![0; 128];
                let f1 = timeout::timeout(Duration::from_secs(15), always_pending()).fuse();
                let f2 = timeout::timeout(_10_SEC, stream.read_exact(&mut buf)).fuse();
                futures::pin_mut!(f1);
                futures::pin_mut!(f2);
                let is_ok = futures::select!(
                    res = f1 => res.is_ok(),
                    res = f2 => res.is_ok()
                );
                assert!(is_ok);
            });
        }
    }

    #[crate::test(tarantool = "crate")]
    async fn no_socket_double_close() {
        let mut stream = TcpStream::connect_timeout("localhost", listen_port(), _10_SEC).unwrap();

        let fd = stream.fd.get().unwrap();

        // Socket is not closed yet
        assert_ne!(unsafe { dbg!(libc::fcntl(fd, libc::F_GETFD)) }, -1);

        // Close the socket
        stream.close().unwrap();

        // Socket is closed now
        assert_eq!(unsafe { dbg!(libc::fcntl(fd, libc::F_GETFD)) }, -1);

        // Reuse the socket's file descriptor
        assert_ne!(unsafe { libc::dup2(libc::STDOUT_FILENO, fd) }, -1);

        // The file descriptor is open
        assert_ne!(unsafe { dbg!(libc::fcntl(fd, libc::F_GETFD)) }, -1);

        drop(stream);

        // The now unrelated file descriptor mustn't be closed
        assert_ne!(unsafe { dbg!(libc::fcntl(fd, libc::F_GETFD)) }, -1);

        // Cleanup
        unsafe { libc::close(fd) };
    }

    fn get_socket_fds() -> HashSet<u32> {
        use std::os::unix::fs::FileTypeExt;

        let mut res = HashSet::new();
        for entry in std::fs::read_dir("/dev/fd/").unwrap() {
            let Ok(entry) = entry else {
                continue;
            };
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if meta.file_type().is_socket() {
                continue;
            };
            let fd_path = entry.path();

            // Yay rust!
            let fd_str = fd_path.file_name().unwrap();
            let fd: u32 = fd_str.to_str().unwrap().parse().unwrap();
            res.insert(fd);
        }
        res
    }

    #[crate::test(tarantool = "crate")]
    fn no_leaks_when_failing_to_connect() {
        let fds_before = get_socket_fds();

        for _ in 0..10 {
            TcpStream::connect_timeout("localhost", 0, _10_SEC).unwrap_err();
        }

        let fds_after = get_socket_fds();

        // XXX: this is a bit unreliable, because tarantool is spawning a bunch
        // of other threads which may or may not be creating and closing fds,
        // so we may want to remove this test at some point
        let new_fds: Vec<_> = fds_after.difference(&fds_before).copied().collect();
        assert!(dbg!(new_fds.is_empty()));
    }
}
