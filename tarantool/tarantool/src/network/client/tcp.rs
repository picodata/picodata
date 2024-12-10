#![allow(deprecated)]

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
use std::future::{self};
use std::mem::{self, MaybeUninit};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{io, marker, vec};

#[cfg(feature = "async-std")]
use async_std::io::{Read as AsyncRead, Write as AsyncWrite};
#[cfg(not(feature = "async-std"))]
use futures::{AsyncRead, AsyncWrite};

use crate::ffi::tarantool as ffi;
use crate::fiber;
use crate::fiber::r#async::context::ContextExt;
use crate::fiber::r#async::timeout::{self, IntoTimeout};
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

fn cvt(t: libc::c_int) -> io::Result<libc::c_int> {
    if t == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(t)
    }
}

/// A wrapper around a raw file descriptor, which automatically closes the
/// descriptor if dropped.
struct AutoCloseFd(RawFd);

impl AsRawFd for AutoCloseFd {
    #[inline(always)]
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

impl FromRawFd for AutoCloseFd {
    #[inline(always)]
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self(fd)
    }
}

impl IntoRawFd for AutoCloseFd {
    #[inline(always)]
    fn into_raw_fd(self) -> RawFd {
        let fd = self.0;
        std::mem::forget(self);
        fd
    }
}

impl Drop for AutoCloseFd {
    fn drop(&mut self) {
        // SAFETY: Safe as long as we only store open file descriptors
        let rc = unsafe { ffi::coio_close(self.0) };
        if rc != 0 {
            crate::say_error!(
                "failed closing socket descriptor: {}",
                io::Error::last_os_error()
            );
        }
    }
}

/// A store for raw file descriptor so we can allow cloning actual `TcpStream` properly.
#[derive(Debug)]
struct TcpInner {
    /// A raw tcp socket file descriptor. Replaced with `None` when the stream
    /// is closed.
    fd: Cell<Option<RawFd>>,
}

impl TcpInner {
    #[inline(always)]
    #[track_caller]
    fn close(&self) -> io::Result<()> {
        let Some(fd) = self.fd.take() else {
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

    #[inline(always)]
    fn fd(&self) -> io::Result<RawFd> {
        let Some(fd) = self.fd.get() else {
            let e = io::Error::new(io::ErrorKind::Other, "socket closed already");
            return Err(e);
        };
        Ok(fd)
    }
}

impl Drop for TcpInner {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            crate::say_error!("TcpInner::drop: closing tcp stream inner failed: {e}");
        }
    }
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
    /// An actual fd which also stored it's open/close state.
    inner: Rc<TcpInner>,
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
        let mut last_error = None;

        for addr in resolve_addr(url, port, timeout.as_secs_f64())? {
            match Self::connect_single((&addr).into(), deadline) {
                Ok(stream) => {
                    return Ok(stream);
                }
                Err(e) => last_error = Some(e),
            }
        }
        let Some(error) = last_error else {
            return Err(Error::ResolveAddress(url.into()));
        };
        if io::ErrorKind::TimedOut == error.kind() {
            return Err(Error::Timeout);
        }
        Err(Error::Connect {
            error,
            address: format!("{url}:{port}"),
        })
    }

    fn connect_single(addr_info: AddrInfo<'_>, deadline: Instant) -> io::Result<Self> {
        // SAFETY: safe cause addr_info which is passed bound with it's SockAddr lifetime
        let fd = unsafe { connect_socket(&addr_info)? };
        let timeout = deadline.duration_since(fiber::clock());
        crate::coio::coio_wait(fd.as_raw_fd(), ffi::CoIOFlags::WRITE, timeout.as_secs_f64())?;
        check_socket_error(&fd)?;
        Ok(Self::from(fd))
    }

    pub async fn connect_async(url: &str, port: u16) -> Result<Self, Error> {
        Self::connect_timeout_async(url, port, Duration::MAX).await
    }

    pub async fn connect_timeout_async(
        url: &str,
        port: u16,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let deadline = fiber::clock().saturating_add(timeout);
        let mut last_error = None;
        for addr in resolve_addr(url, port, timeout.as_secs_f64())? {
            match Self::connect_single_async((&addr).into())
                .deadline(deadline)
                .await
            {
                Ok(stream) => {
                    return Ok(stream);
                }
                Err(e) => last_error = Some(e),
            }
        }
        let Some(error) = last_error else {
            return Err(Error::ResolveAddress(url.into()));
        };
        Err(match error {
            timeout::Error::Expired => Error::Timeout,
            timeout::Error::Failed(err) => Error::Connect {
                error: err,
                address: format!("{url}:{port}"),
            },
        })
    }

    async fn connect_single_async(addr_info: AddrInfo<'_>) -> io::Result<Self> {
        // SAFETY: safe cause addr_info which is passed bound with it's SockAddr lifetime
        let fd = unsafe { connect_socket(&addr_info)? };
        // Cause we're inside FnMut we can't use AutoCloseFd
        let raw_fd = fd.into_raw_fd();
        let f = future::poll_fn(|cx| {
            if let Err(e) = check_socket_error(&raw_fd) {
                // SAFETY: this fd is still valid and was not closed.
                unsafe { AutoCloseFd::from_raw_fd(raw_fd) };
                return Poll::Ready(Err(e));
            }
            // We use getpeername to check the connection state.
            // If the call is successful (rc = 0) then the connection is established.
            // We don't care about the actual peer name.
            // SAFERY: this value is not used further.
            let mut dummy = std::mem::MaybeUninit::<libc::sockaddr>::uninit();
            let mut dummy_size = std::mem::size_of_val(&dummy) as _;
            // SAFERY: pointers and valid within thi ffi call so it's safe.
            let rc = unsafe { libc::getpeername(raw_fd, dummy.as_mut_ptr(), &mut dummy_size) };
            if rc == 0 {
                return Poll::Ready(Ok(Self::from(raw_fd)));
            }
            // SAFETY: safe as long as this future is executed by `fiber::block_on` async executor.
            unsafe {
                ContextExt::set_coio_wait(cx, raw_fd, ffi::CoIOFlags::WRITE);
            }
            Poll::Pending
        });

        f.await
    }

    #[inline(always)]
    #[track_caller]
    pub fn close(&self) -> io::Result<()> {
        self.inner.close()
    }
}

/// SAFETY: completely unsafe, but we are allowed to do this cause sending/sharing following stream to/from another thread
/// SAFETY: will take no effect due to no runtime within it
unsafe impl Send for TcpStream {}
unsafe impl Sync for TcpStream {}

impl From<RawFd> for TcpStream {
    fn from(value: RawFd) -> Self {
        Self {
            inner: Rc::new(TcpInner {
                fd: Cell::new(Some(value)),
            }),
        }
    }
}

impl From<AutoCloseFd> for TcpStream {
    fn from(value: AutoCloseFd) -> Self {
        Self::from(value.into_raw_fd())
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let fd = self.inner.fd()?;

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
        self.inner.fd()?;
        // [`TcpStream`] similarily to std does not buffer anything,
        // so there is nothing to flush.
        //
        // If buffering is needed use [`futures::io::BufWriter`] on top of this stream.
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.fd()?;
        let res = self.inner.close();
        Poll::Ready(res)
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let fd = self.inner.fd()?;

        let (result, err) = (
            // `self.inner.fd` must be nonblocking for this to work correctly
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

/// Resolves provided url and port to a sequence of sock addrs.
///
/// # Returns
///
/// A vector of resolved addrs where v4 go first.
fn resolve_addr(url: &str, port: u16, timeout: f64) -> Result<Vec<SockAddr>, Error> {
    // SAFETY: value is not used inled hints are set
    let mut hints = unsafe { MaybeUninit::<libc::addrinfo>::zeroed().assume_init() };

    hints.ai_family = libc::AF_UNSPEC;
    hints.ai_socktype = libc::SOCK_STREAM;

    let host = CString::new(url).map_err(Error::ConstructCString)?;

    // SAFETY: safe as long as we are in tarantool runtime
    let addrinfo = match unsafe { crate::coio::getaddrinfo(&host, None, &hints, timeout) } {
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

    let mut result = Vec::with_capacity(4);
    let mut current = addrinfo;

    while !current.is_null() {
        // SAFETY: we are dereferencing pointers which were allocated by libc so it's fine
        let ai = unsafe { *current };
        match ai.ai_family {
            libc::AF_INET => {
                // SAFETY: we are dereferencing pointers which were allocated by libc so it's fine
                let mut sockaddr = unsafe { *(ai.ai_addr as *mut libc::sockaddr_in) };
                sockaddr.sin_port = port.to_be();
                result.push(SockAddr::V4(sockaddr));
            }
            libc::AF_INET6 => {
                // SAFETY: we are dereferencing pointers which were allocated by libc so it's fine
                let mut sockaddr = unsafe { *(ai.ai_addr as *mut libc::sockaddr_in6) };
                sockaddr.sin6_port = port.to_be();
                result.push(SockAddr::V6(sockaddr));
            }
            af => {
                // SAFETY: value was allocated by libc so it's fine
                unsafe { libc::freeaddrinfo(addrinfo) };
                return Err(Error::UnknownAddressFamily(af as u16));
            }
        }
        current = ai.ai_next;
    }

    // SAFETY: value was allocated by libc so it's fine
    unsafe { libc::freeaddrinfo(addrinfo) };

    // Sort resolved addrs to prefer v4
    result.sort();

    Ok(result)
}

/// # Safety
/// addr_info.add should be a valid
unsafe fn connect_socket(addr_info: &AddrInfo<'_>) -> io::Result<AutoCloseFd> {
    let fd = nonblocking_socket(addr_info.kind)?;
    let Err(e) = cvt(libc::connect(
        fd.as_raw_fd(),
        addr_info.addr,
        addr_info.addr_len,
    )) else {
        return Ok(fd);
    };
    if e.raw_os_error() != Some(libc::EINPROGRESS) {
        return Err(e);
    }
    Ok(fd)
}

#[cfg(target_os = "linux")]
#[inline(always)]
fn nonblocking_socket(kind: libc::c_int) -> io::Result<AutoCloseFd> {
    // SAFETY: This is safe because `libc::socket` doesn't do undefined behavior
    unsafe {
        let raw_fd = cvt(libc::socket(
            kind,
            libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
            0,
        ))?;
        let fd = AutoCloseFd::from_raw_fd(raw_fd);

        Ok(fd)
    }
}

#[cfg(target_os = "macos")]
fn nonblocking_socket(kind: libc::c_int) -> io::Result<AutoCloseFd> {
    // SAFETY: This is safe because `libc::socket` doesn't do undefined behavior
    let fd = unsafe { AutoCloseFd::from_raw_fd(cvt(libc::socket(kind, libc::SOCK_STREAM, 0))?) };
    // SAFETY: This is safe because fd is open
    unsafe { cvt(libc::ioctl(fd.as_raw_fd(), libc::FIOCLEX))? };
    let opt_value = 1;
    // SAFETY: This is safe because fd is open and the opt_value buffer specification is valid.
    unsafe {
        cvt(libc::setsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_NOSIGPIPE,
            &opt_value as *const _ as *const libc::c_void,
            mem::size_of_val(&opt_value) as _,
        ))?;
    };
    // SAFETY: This is safe because fd is open
    unsafe {
        cvt(libc::ioctl(fd.as_raw_fd(), libc::FIONBIO, &mut 1))?;
    };
    Ok(fd)
}

fn check_socket_error(fd: &impl AsRawFd) -> io::Result<()> {
    // SAFETY: passed only to ffi call so it's fine
    let mut val: libc::c_int = 0;
    let mut val_len = mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: fd is not closed since it is inside OwnedFd
    cvt(unsafe {
        libc::getsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            &mut val as *mut libc::c_int as *mut _,
            &mut val_len,
        )
    })?;
    match val {
        0 => Ok(()),
        v => Err(io::Error::from_raw_os_error(v as i32)),
    }
}

#[derive(Debug)]
enum SockAddr {
    V4(libc::sockaddr_in),
    V6(libc::sockaddr_in6),
}

impl Ord for SockAddr {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (SockAddr::V4(_), SockAddr::V6(_)) => std::cmp::Ordering::Less,
            (SockAddr::V6(_), SockAddr::V4(_)) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        }
    }
}

impl PartialOrd for SockAddr {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SockAddr {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (SockAddr::V4(_), SockAddr::V4(_)) | (SockAddr::V6(_), SockAddr::V6(_))
        )
    }
}

impl Eq for SockAddr {}

struct AddrInfo<'a> {
    kind: libc::c_int,
    addr: *const libc::sockaddr,
    addr_len: libc::socklen_t,
    marker: marker::PhantomData<&'a ()>,
}

impl<'a> From<&'a SockAddr> for AddrInfo<'a> {
    fn from(value: &'a SockAddr) -> Self {
        let (kind, addr, addr_len) = match value {
            SockAddr::V4(v4) => {
                let kind = libc::AF_INET;
                let addr = v4 as *const libc::sockaddr_in as *const libc::sockaddr;
                let addr_len = mem::size_of::<libc::sockaddr_in>();
                (kind, addr, addr_len)
            }
            SockAddr::V6(v6) => {
                let kind = libc::AF_INET6;
                let addr = v6 as *const libc::sockaddr_in6 as *const libc::sockaddr;
                let addr_len = mem::size_of::<libc::sockaddr_in6>();
                (kind, addr, addr_len)
            }
        };
        Self {
            kind,
            addr,
            addr_len: addr_len as _,
            marker: marker::PhantomData::<&'a ()>,
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
#[deprecated = "Use `TcpStream` instead"]
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
        let addrs = resolve_addr("example.org", 80, _10_SEC.as_secs_f64()).unwrap();

        let mut our_addrs = HashSet::<net::SocketAddr>::new();
        for addr in addrs {
            match addr {
                SockAddr::V4(v4) => our_addrs.insert(to_socket_addr_v4(v4).into()),
                SockAddr::V6(v6) => our_addrs.insert(to_socket_addr_v6(v6).into()),
            };
        }

        let addrs_from_std: HashSet<_> = net::ToSocketAddrs::to_socket_addrs(&("example.org", 80))
            .unwrap()
            .collect();

        assert_eq!(our_addrs, addrs_from_std);

        //
        // check what happens with "localhost"
        //
        let addrs = resolve_addr("localhost", 1337, _10_SEC.as_secs_f64()).unwrap();

        let mut our_addrs = HashSet::<net::SocketAddr>::new();
        for addr in addrs {
            match addr {
                SockAddr::V4(v4) => our_addrs.insert(to_socket_addr_v4(v4).into()),
                SockAddr::V6(v6) => our_addrs.insert(to_socket_addr_v6(v6).into()),
            };
        }

        let addrs_from_std: HashSet<_> = net::ToSocketAddrs::to_socket_addrs(&("localhost", 1337))
            .unwrap()
            .collect();

        assert_eq!(our_addrs, addrs_from_std);
    }

    #[crate::test(tarantool = "crate")]
    async fn get_libc_addrs_error() {
        let err = resolve_addr("invalid domain name", 80, _10_SEC.as_secs_f64())
            .unwrap_err()
            .to_string();

        assert_eq!(err, "failed to resolve domain name 'invalid domain name'");
    }

    #[crate::test(tarantool = "crate")]
    fn connect() {
        let _ = TcpStream::connect("localhost", listen_port()).unwrap();
    }

    #[crate::test(tarantool = "crate")]
    fn connect_async() {
        let _ = fiber::block_on(TcpStream::connect_async("localhost", listen_port())).unwrap();
    }

    #[crate::test(tarantool = "crate")]
    fn connect_timeout() {
        let _ = TcpStream::connect_timeout("localhost", listen_port(), _10_SEC).unwrap();
    }

    #[crate::test(tarantool = "crate")]
    fn connect_timeout_async() {
        let _ = fiber::block_on(TcpStream::connect_timeout_async(
            "localhost",
            listen_port(),
            _10_SEC,
        ))
        .unwrap();
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
    fn connect_zero_timeout_async() {
        assert!(matches!(
            fiber::block_on(TcpStream::connect_timeout_async("example.com", 80, _0_SEC))
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
    async fn read_clone() {
        let mut stream = TcpStream::connect_timeout("localhost", listen_port(), _10_SEC).unwrap();
        let cloned = stream.clone();
        drop(cloned);
        // Read greeting
        let mut buf = vec![0; 128];
        stream.read_exact(&mut buf).timeout(_10_SEC).await.unwrap();
    }

    #[crate::test(tarantool = "crate")]
    async fn read_timeout() {
        let mut stream = TcpStream::connect_timeout("localhost", listen_port(), _10_SEC).unwrap();
        // Read greeting
        let mut buf = vec![0; 4096];
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
    fn write_clone() {
        let (sender, receiver) = std::sync::mpsc::channel();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
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
                let mut stream =
                    TcpStream::connect_timeout("localhost", addr.port(), _10_SEC).unwrap();
                let cloned = stream.clone();
                drop(cloned);
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
    fn select_correct_connect_timeout() {
        {
            fiber::block_on(async {
                let f1 = timeout::timeout(_0_SEC, always_pending()).fuse();
                let f2 =
                    TcpStream::connect_timeout_async("localhost", listen_port(), _10_SEC).fuse();
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
                let f1 = timeout::timeout(Duration::from_secs(15), always_pending()).fuse();
                let f2 =
                    TcpStream::connect_timeout_async("localhost", listen_port(), _10_SEC).fuse();
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

    // #[crate::test(tarantool = "crate")]
    // async fn no_socket_double_close() {
    //     let mut stream = TcpStream::connect_timeout("localhost", listen_port(), _10_SEC).unwrap();

    //     let fd = stream.fd.get().unwrap();

    //     // Socket is not closed yet
    //     assert_ne!(unsafe { dbg!(libc::fcntl(fd, libc::F_GETFD)) }, -1);

    //     // Close the socket
    //     stream.close().unwrap();

    //     // Socket is closed now
    //     assert_eq!(unsafe { dbg!(libc::fcntl(fd, libc::F_GETFD)) }, -1);

    //     // Reuse the socket's file descriptor
    //     assert_ne!(unsafe { libc::dup2(libc::STDOUT_FILENO, fd) }, -1);

    //     // The file descriptor is open
    //     assert_ne!(unsafe { dbg!(libc::fcntl(fd, libc::F_GETFD)) }, -1);

    //     drop(stream);

    //     // The now unrelated file descriptor mustn't be closed
    //     assert_ne!(unsafe { dbg!(libc::fcntl(fd, libc::F_GETFD)) }, -1);

    //     // Cleanup
    //     unsafe { libc::close(fd) };
    // }

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
