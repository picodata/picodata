use std::cell::{Cell, RefCell};
use std::ffi::{CString, NulError};
use std::future::Future;
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
use crate::fiber::r#async::{self, timeout};

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
#[derive(Debug)]
pub struct TcpStream {
    fd: RawFd,
}

impl TcpStream {
    /// Creates a [`TcpStream`] to `url`.
    /// `resolve_timeout` - address resolution timeout.
    ///
    /// This functions makes the fiber **yield**.
    pub async fn connect(url: &str, port: u16) -> Result<TcpStream, Error> {
        let addrs = unsafe {
            let addr_info = get_address_info(url).await?;
            let addrs = get_addrs_from_info(addr_info, port);
            libc::freeaddrinfo(addr_info);
            addrs
        };
        let addrs = addrs?;
        let stream = std::net::TcpStream::connect(addrs.as_slice()).map_err(Error::Connect)?;
        stream.set_nonblocking(true).map_err(Error::SetNonBlock)?;
        Ok(Self {
            fd: stream.into_raw_fd(),
        })
    }

    /// Close token for [`TcpStream`] to be able to close it from other fibers.
    pub fn close_token(&self) -> CloseToken {
        CloseToken(self.fd)
    }
}

/// Close token for [`TcpStream`] to be able to close it from other fibers.
#[derive(Debug)]
pub struct CloseToken(RawFd);

impl CloseToken {
    pub fn close(&self) -> io::Result<()> {
        let (res, err) = (
            unsafe { ffi::coio_close(self.0) },
            io::Error::last_os_error(),
        );
        if res != 0 {
            Err(err)
        } else {
            Ok(())
        }
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

async unsafe fn get_address_info(url: &str) -> Result<*mut libc::addrinfo, Error> {
    struct GetAddrInfo(r#async::coio::GetAddrInfo);

    impl Future for GetAddrInfo {
        type Output = Result<*mut libc::addrinfo, Error>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            unsafe {
                if self.0.err.get() {
                    return Poll::Ready(Err(Error::ResolveAddress));
                }
                if self.0.res.get().is_null() {
                    ContextExt::set_coio_getaddrinfo(cx, self.0.clone());
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(self.0.res.get()))
                }
            }
        }
    }

    let host = CString::new(url).map_err(Error::ConstructCString)?;
    let mut hints = MaybeUninit::<libc::addrinfo>::zeroed().assume_init();
    hints.ai_family = libc::AF_UNSPEC;
    hints.ai_socktype = libc::SOCK_STREAM;
    GetAddrInfo(r#async::coio::GetAddrInfo {
        host,
        hints,
        res: Rc::new(Cell::new(ptr::null_mut())),
        err: Rc::new(Cell::new(false)),
    })
    .await
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
        Poll::Ready(self.close_token().close())
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
        let _ = self.close_token().close();
    }
}

#[cfg(feature = "tarantool_test")]
mod tests {
    use super::*;

    use crate::fiber;
    use crate::fiber::r#async::timeout::IntoTimeout;
    use crate::test::TARANTOOL_LISTEN;

    use std::net::TcpListener;
    use std::thread;

    use futures::{AsyncReadExt, AsyncWriteExt, FutureExt};

    const _10_SEC: Duration = Duration::from_secs(10);
    const _0_SEC: Duration = Duration::from_secs(0);

    async fn always_pending() {
        loop {
            futures::pending!()
        }
    }

    #[tarantool::test]
    fn resolve_address() {
        unsafe {
            let _ = fiber::block_on(get_address_info("localhost").timeout(_10_SEC))
                .unwrap()
                .unwrap();
        }
    }

    #[tarantool::test]
    fn connect() {
        let _ =
            fiber::block_on(TcpStream::connect("localhost", TARANTOOL_LISTEN).timeout(_10_SEC))
                .unwrap()
                .unwrap();
    }

    #[tarantool::test]
    fn read() {
        fiber::block_on(async {
            let mut stream = TcpStream::connect("localhost", TARANTOOL_LISTEN)
                .timeout(_10_SEC)
                .await
                .unwrap()
                .unwrap();
            // Read greeting
            let mut buf = vec![0; 128];
            stream.read_exact(&mut buf).timeout(_10_SEC).await.unwrap();
        });
    }

    #[tarantool::test]
    fn read_timeout() {
        fiber::block_on(async {
            let mut stream = TcpStream::connect("localhost", TARANTOOL_LISTEN)
                .timeout(_10_SEC)
                .await
                .unwrap()
                .unwrap();
            // Read greeting
            let mut buf = vec![0; 128];
            assert_eq!(
                stream
                    .read_exact(&mut buf)
                    .timeout(_0_SEC)
                    .await
                    .unwrap_err(),
                timeout::Expired
            );
        });
    }

    fn write() {
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
            fiber::block_on(async {
                let mut stream = TcpStream::connect("localhost", 3302)
                    .timeout(_10_SEC)
                    .await
                    .unwrap()
                    .unwrap();
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
    }

    #[tarantool::test]
    fn split() {
        let (sender, receiver) = std::sync::mpsc::channel();
        let listener = TcpListener::bind("127.0.0.1:3303").unwrap();
        // Spawn listener
        thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut buf = vec![0; 5];
                <std::net::TcpStream as std::io::Read>::read_exact(&mut stream, &mut buf);
                <std::net::TcpStream as std::io::Write>::write_all(&mut stream, &buf.clone());
                sender.send(buf);
            }
        });
        // Send and read data
        {
            let mut stream =
                fiber::block_on(TcpStream::connect("localhost", 3303).timeout(_10_SEC))
                    .unwrap()
                    .unwrap();
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

    #[tarantool::test]
    fn join_correct_timeout() {
        {
            fiber::block_on(async {
                let mut stream = TcpStream::connect("localhost", TARANTOOL_LISTEN)
                    .timeout(_10_SEC)
                    .await
                    .unwrap()
                    .unwrap();
                // Read greeting
                let mut buf = vec![0; 128];
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
            fiber::block_on(async {
                let mut stream = TcpStream::connect("localhost", TARANTOOL_LISTEN)
                    .timeout(_10_SEC)
                    .await
                    .unwrap()
                    .unwrap();
                // Read greeting
                let mut buf = vec![0; 128];
                let (is_ok, is_err) = futures::join!(
                    timeout::timeout(_10_SEC, stream.read_exact(&mut buf)),
                    timeout::timeout(_0_SEC, always_pending())
                );
                assert_eq!(is_err.unwrap_err(), timeout::Expired);
                is_ok.unwrap();
            });
        }
    }

    #[tarantool::test]
    fn select_correct_timeout() {
        {
            fiber::block_on(async {
                let mut stream = TcpStream::connect("localhost", TARANTOOL_LISTEN)
                    .timeout(_10_SEC)
                    .await
                    .unwrap()
                    .unwrap();
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
                let mut stream = TcpStream::connect("localhost", TARANTOOL_LISTEN)
                    .timeout(_10_SEC)
                    .await
                    .unwrap()
                    .unwrap();
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
}
