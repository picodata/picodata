use std::cell::{Cell, RefCell};
use std::ffi::{CString, NulError};
use std::mem::{self, MaybeUninit};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::unix::io::RawFd;
use std::os::unix::prelude::IntoRawFd;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use std::{io, ptr};

use futures::{AsyncRead, AsyncWrite};

use crate::ffi::tarantool as ffi;
use crate::fiber::r#async::context::ContextExt;
use crate::fiber::r#async::timeout;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to resolve host address by domain name")]
    ResolveAddress,
    #[error("input parameters contain ffi incompatible strings: {0}")]
    ConstructCString(NulError),
    #[error("failed to connect to supplied address: {0}")]
    Connect(io::Error),
    #[error("failed to set socket to nonblocking mode: {0}")]
    SetNonBlock(io::Error),
    #[error("unknown address family: {0}")]
    UnknownAddressFamily(u16),
    #[error("write half of the stream is closed")]
    WriteClosed,
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
/// [t]: crate::fiber::async::timeout::timeout
pub struct TcpStream {
    fd: RawFd,
    write_closed: bool,
}

impl TcpStream {
    /// Creates a [`TcpStream`] to `url`.
    /// `resolve_timeout` - address resolution timeout.
    ///
    /// This functions makes the fiber **yield**.
    pub unsafe fn connect(
        url: &str,
        port: u16,
        resolve_timeout: Duration,
    ) -> Result<TcpStream, Error> {
        let addr_info = get_address_info(url, resolve_timeout)?;
        let addrs = get_addrs_from_info(addr_info, port);
        libc::freeaddrinfo(addr_info);
        let addrs = addrs?;
        let stream = std::net::TcpStream::connect(addrs.as_slice()).map_err(Error::Connect)?;
        stream.set_nonblocking(true).map_err(Error::SetNonBlock)?;
        Ok(Self {
            fd: stream.into_raw_fd(),
            write_closed: false,
        })
    }
}

unsafe fn get_addrs_from_info(
    addrs: *const libc::addrinfo,
    port: u16,
) -> Result<Vec<SocketAddr>, Error> {
    let mut addr = addrs;
    let mut out_addrs = Vec::new();
    while !addr.is_null() {
        out_addrs.push(to_rs_sockaddr((*addr).ai_addr, port)?);
        addr = (*addr).ai_next;
    }
    Ok(out_addrs)
}

unsafe fn get_address_info(url: &str, timeout: Duration) -> Result<*mut libc::addrinfo, Error> {
    let url = CString::new(url).map_err(Error::ConstructCString)?;
    let mut hints = MaybeUninit::<libc::addrinfo>::zeroed().assume_init();
    hints.ai_family = libc::AF_UNSPEC;
    hints.ai_socktype = libc::SOCK_STREAM;
    let mut addrs = ptr::null_mut();
    let res = ffi::coio_getaddrinfo(
        url.as_ptr(),
        ptr::null(),
        &hints as *const _,
        &mut addrs as *mut _,
        timeout.as_secs_f64(),
    );
    if res != 0 {
        return Err(Error::ResolveAddress);
    }
    Ok(addrs)
}

unsafe fn to_rs_sockaddr(addr: *const libc::sockaddr, port: u16) -> Result<SocketAddr, Error> {
    match (*addr).sa_family as libc::c_int {
        libc::AF_INET => {
            let addr: *mut libc::sockaddr_in = mem::transmute(addr);
            (*addr).sin_port = port;
            let octets: [u8; 4] = (*addr).sin_addr.s_addr.to_ne_bytes();
            Ok(SocketAddr::V4(SocketAddrV4::new(octets.into(), port)))
        }
        libc::AF_INET6 => {
            let addr: *mut libc::sockaddr_in6 = mem::transmute(addr);
            (*addr).sin6_port = port;
            let octets = (*addr).sin6_addr.s6_addr;
            let flow_info = (*addr).sin6_flowinfo;
            let scope_id = (*addr).sin6_scope_id;
            Ok(SocketAddr::V6(SocketAddrV6::new(
                octets.into(),
                port,
                flow_info,
                scope_id,
            )))
        }
        af => Err(Error::UnknownAddressFamily(af as u16)),
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.write_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                Error::WriteClosed,
            )));
        }
        let (result, err) = (
            // `self.fd` must be nonblocking for this to work correctly
            unsafe { libc::write(self.fd, buf.as_ptr() as *const libc::c_void, buf.len()) },
            io::Error::last_os_error(),
        );

        if result >= 0 {
            return Poll::Ready(Ok(result as usize));
        }
        match err.kind() {
            io::ErrorKind::WouldBlock => {
                // SAFETY: Safe as long as this future is executed by
                // `fiber::block_on` async executor.
                unsafe { ContextExt::set_coio_wait(cx, self.fd, ffi::CoIOFlags::WRITE) }
                Poll::Pending
            }
            io::ErrorKind::Interrupted => {
                // Return poll pending without setting coio wait
                // so that write can be retried immediately.
                //
                // SAFETY: Safe as long as this future is executed by
                // `fiber::block_on` async executor.
                unsafe { ContextExt::set_deadline(cx, Instant::now()) }
                Poll::Pending
            }
            _ => Poll::Ready(Err(err)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // [`TcpStream`] similarily to std does not buffer anything,
        // so there is nothing to flush.
        //
        // If buffering is needed use [`futures::io::BufWriter`] on top of this stream.
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.write_closed = true;
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let (result, err) = (
            // `self.fd` must be nonblocking for this to work correctly
            unsafe { libc::read(self.fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) },
            io::Error::last_os_error(),
        );

        if result >= 0 {
            return Poll::Ready(Ok(result as usize));
        }
        match err.kind() {
            io::ErrorKind::WouldBlock => {
                // SAFETY: Safe as long as this future is executed by
                // `fiber::block_on` async executor.
                unsafe { ContextExt::set_coio_wait(cx, self.fd, ffi::CoIOFlags::READ) }
                Poll::Pending
            }
            io::ErrorKind::Interrupted => {
                // Return poll pending without setting coio wait
                // so that read can be retried immediately.
                //
                // SAFETY: Safe as long as this future is executed by
                // `fiber::block_on` async executor.
                unsafe { ContextExt::set_deadline(cx, Instant::now()) }
                Poll::Pending
            }
            _ => Poll::Ready(Err(err)),
        }
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        unsafe { ffi::coio_close(self.fd) };
    }
}

#[cfg(feature = "tarantool_test")]
mod tests {
    use super::*;

    use crate::fiber;
    use crate::test::{TestCase, TESTS};
    use crate::test_name;

    use std::net::TcpListener;
    use std::thread;

    use futures::{AsyncReadExt, AsyncWriteExt, FutureExt};
    use linkme::distributed_slice;

    const _10_SEC: Duration = Duration::from_secs(10);
    const _0_SEC: Duration = Duration::from_secs(0);

    /// The default port where tarantool listens in tests
    const TARANTOOL_LISTEN: u16 = 3301;

    async fn always_pending() {
        loop {
            futures::pending!()
        }
    }

    #[distributed_slice(TESTS)]
    static RESOLVE_ADDRESS: TestCase = TestCase {
        name: test_name!("resolve_address"),
        f: || unsafe {
            let _ = get_address_info("localhost", _10_SEC).unwrap();
        },
    };

    #[distributed_slice(TESTS)]
    static CONNECT: TestCase = TestCase {
        name: test_name!("connect"),
        f: || {
            let _ = unsafe { TcpStream::connect("localhost", TARANTOOL_LISTEN, _10_SEC) }.unwrap();
        },
    };

    #[distributed_slice(TESTS)]
    static READ: TestCase = TestCase {
        name: test_name!("read"),
        f: || {
            let mut stream =
                unsafe { TcpStream::connect("localhost", TARANTOOL_LISTEN, _10_SEC) }.unwrap();
            // Read greeting
            let mut buf = vec![0; 128];
            fiber::block_on(timeout::timeout(_10_SEC, stream.read_exact(&mut buf))).unwrap();
        },
    };

    #[distributed_slice(TESTS)]
    static READ_TIMEOUT: TestCase = TestCase {
        name: test_name!("read_timeout"),
        f: || {
            let mut stream =
                unsafe { TcpStream::connect("localhost", TARANTOOL_LISTEN, _10_SEC) }.unwrap();
            // Read greeting
            let mut buf = vec![0; 128];
            assert_eq!(
                fiber::block_on(timeout::timeout(_0_SEC, stream.read_exact(&mut buf))).unwrap_err(),
                timeout::Expired
            );
        },
    };

    #[distributed_slice(TESTS)]
    static WRITE: TestCase = TestCase {
        name: test_name!("write"),
        f: || {
            let (sender, receiver) = std::sync::mpsc::channel();
            let listener = TcpListener::bind("127.0.0.1:3302").unwrap();
            // Spawn listener
            thread::spawn(move || {
                for stream in listener.incoming() {
                    let mut stream = stream.unwrap();
                    let mut buf = vec![];
                    <std::net::TcpStream as std::io::Read>::read_to_end(&mut stream, &mut buf);
                    sender.send(buf);
                }
            });
            // Send data
            {
                let mut stream = unsafe { TcpStream::connect("localhost", 3302, _10_SEC) }.unwrap();
                fiber::block_on(async move {
                    timeout::timeout(_10_SEC, stream.write_all(&[1, 2, 3]))
                        .await
                        .unwrap();
                    timeout::timeout(_10_SEC, stream.write_all(&[4, 5]))
                        .await
                        .unwrap()
                });
            }
            let buf = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
            assert_eq!(buf, vec![1, 2, 3, 4, 5])
        },
    };

    #[distributed_slice(TESTS)]
    static JOIN_CORRECT_TIMEOUT: TestCase = TestCase {
        name: test_name!("join_correct_timeout"),
        f: || {
            {
                let mut stream =
                    unsafe { TcpStream::connect("localhost", TARANTOOL_LISTEN, _10_SEC) }.unwrap();
                // Read greeting
                let mut buf = vec![0; 128];
                fiber::block_on(async {
                    let (is_err, is_ok) = futures::join!(
                        timeout::timeout(_0_SEC, always_pending()),
                        timeout::timeout(_10_SEC, stream.read_exact(&mut buf))
                    );
                    assert_eq!(is_err.unwrap_err(), timeout::Expired);
                    is_ok.unwrap();
                });
            }
            // Testing with different order in join
            {
                let mut stream =
                    unsafe { TcpStream::connect("localhost", TARANTOOL_LISTEN, _10_SEC) }.unwrap();
                // Read greeting
                let mut buf = vec![0; 128];
                fiber::block_on(async {
                    let (is_ok, is_err) = futures::join!(
                        timeout::timeout(_10_SEC, stream.read_exact(&mut buf)),
                        timeout::timeout(_0_SEC, always_pending())
                    );
                    assert_eq!(is_err.unwrap_err(), timeout::Expired);
                    is_ok.unwrap();
                });
            }
        },
    };

    #[distributed_slice(TESTS)]
    static SELECT_CORRECT_TIMEOUT: TestCase = TestCase {
        name: test_name!("select_correct_timeout"),
        f: || {
            {
                let mut stream =
                    unsafe { TcpStream::connect("localhost", TARANTOOL_LISTEN, _10_SEC) }.unwrap();
                // Read greeting
                let mut buf = vec![0; 128];
                fiber::block_on(async {
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
                let mut stream =
                    unsafe { TcpStream::connect("localhost", TARANTOOL_LISTEN, _10_SEC) }.unwrap();
                // Read greeting
                let mut buf = vec![0; 128];
                fiber::block_on(async {
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
        },
    };
}
