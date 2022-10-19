//! Cooperative input/output
//!
//! See also:
//! - [C API reference: Module coio](https://www.tarantool.io/en/doc/latest/dev_guide/reference_capi/coio/)
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::ffi::c_void;
use std::io::{self, Read, Write};
use std::mem::forget;
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::os::raw::c_char;
use std::os::unix::io::{AsRawFd, IntoRawFd, RawFd};
use std::rc::Rc;
use std::time::Duration;

use core::ptr::null_mut;
use num_traits::Zero;

use crate::error::{Error, TarantoolError};
use crate::ffi::tarantool as ffi;
use crate::fiber::{unpack_callback, Cond};

const TIMEOUT_INFINITY: f64 = 365.0 * 86400.0 * 100.0;

/// Uses CoIO main loop to poll read/write events from wrapped socket
pub struct CoIOStream {
    fd: RawFd,
}

impl CoIOStream {
    /// Convert fd-like object to CoIO stream
    pub fn new<T>(inner: T) -> Result<CoIOStream, io::Error>
    where
        T: IntoRawFd,
    {
        let fd = inner.into_raw_fd();
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }

        if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 } {
            Err(io::Error::last_os_error())
        } else {
            Ok(CoIOStream { fd })
        }
    }

    /// Connect to remote TCP socket
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<CoIOStream, io::Error> {
        let inner_stream = TcpStream::connect(addr)?;
        inner_stream.set_nonblocking(true)?;
        Ok(CoIOStream {
            fd: inner_stream.into_raw_fd(),
        })
    }

    /// Opens a TCP connection to a remote host with a timeout.
    pub fn connect_timeout(addr: &SocketAddr, timeout: Duration) -> Result<CoIOStream, io::Error> {
        let inner_stream = TcpStream::connect_timeout(addr, timeout)?;
        inner_stream.set_nonblocking(true)?;
        Ok(CoIOStream {
            fd: inner_stream.into_raw_fd(),
        })
    }

    /// Pull some bytes from this source into the specified buffer. Returns how many bytes were read or 0 on timeout.
    pub fn read_with_timeout(
        &mut self,
        buf: &mut [u8],
        timeout: Option<Duration>,
    ) -> Result<usize, io::Error> {
        read(self.fd, buf, timeout)
    }

    /// Write a buffer into this writer. Returns how many bytes were written or 0 on timeout.
    pub fn write_with_timeout(
        &mut self,
        buf: &[u8],
        timeout: Option<Duration>,
    ) -> Result<usize, io::Error> {
        write(self.fd, buf, timeout)
    }
}

impl IntoRawFd for CoIOStream {
    fn into_raw_fd(self) -> RawFd {
        let fd = self.fd;
        forget(self);
        fd
    }
}

impl AsRawFd for CoIOStream {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl Read for CoIOStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.read_with_timeout(buf, None)
    }
}

impl Write for CoIOStream {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.write_with_timeout(buf, None)
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        Ok(())
    }
}

impl Drop for CoIOStream {
    fn drop(&mut self) {
        unsafe { ffi::coio_close(self.fd) };
    }
}

/// Uses CoIO main loop to poll incoming connections from wrapped socket listener
pub struct CoIOListener {
    inner: TcpListener,
}

impl CoIOListener {
    /// Accept a new incoming connection from this listener.
    pub fn accept(&self) -> Result<CoIOStream, io::Error> {
        loop {
            let res = self.inner.accept();
            return match res {
                Ok((stream, _)) => CoIOStream::new(stream),

                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        coio_wait(
                            self.inner.as_raw_fd(),
                            ffi::CoIOFlags::READ,
                            TIMEOUT_INFINITY,
                        )?;
                        continue;
                    }
                    Err(e)
                }
            };
        }
    }

    pub fn inner_listener(&mut self) -> &mut TcpListener {
        &mut self.inner
    }
}

impl TryFrom<TcpListener> for CoIOListener {
    type Error = io::Error;

    fn try_from(value: TcpListener) -> Result<Self, Self::Error> {
        value.set_nonblocking(true)?;
        Ok(Self { inner: value })
    }
}

/// Wait until `READ` or `WRITE` event on socket (`fd`). Yields.
///
/// - `fd` - non-blocking socket file description
/// - `events` - requested events to wait. Combination of [CoIOFlags::READ | CoIOFlags::WRITE](struct.CoIOFlags.html) bit flags.
/// - `timeoout` - timeout in seconds.
pub fn coio_wait(fd: RawFd, flags: ffi::CoIOFlags, timeout: f64) -> Result<(), io::Error> {
    match unsafe { ffi::coio_wait(fd, flags.bits(), timeout) } {
        0 => Err(io::ErrorKind::TimedOut.into()),
        _ => Ok(()),
    }
}

/// Create new eio task with specified function and
/// arguments. Yield and wait until the task is complete
/// or a timeout occurs.
///
/// This function doesn't throw exceptions to avoid double error
/// checking: in most cases it's also necessary to check the return
/// value of the called function and perform necessary actions. If
/// func sets errno, the errno is preserved across the call.
///
/// Returns:
/// - `-1` and `errno = ENOMEM` if failed to create a task
/// - the function return (errno is preserved).
///
/// ```no_run
/// use tarantool::coio::coio_call;
///
/// let mut f = |a: Box<i32>| *a + 1;
/// if coio_call(&mut f, 1) == -1 {
///     // handle errors.
/// }
/// ```
pub fn coio_call<F, T>(callback: &mut F, arg: T) -> isize
where
    F: FnMut(Box<T>) -> i32,
{
    let (callback_ptr, trampoline) = unsafe { unpack_callback(callback) };
    unsafe { ffi::coio_call(trampoline, callback_ptr, Box::into_raw(Box::<T>::new(arg))) }
}

/// Fiber-friendly version of `getaddrinfo(3)`.
///
/// - `host` - host name, i.e. "tarantool.org"
/// - `port` - service name, i.e. "80" or "http"
/// - `hints` - hints, see `getaddrinfo(3)`
/// - `timeout` - timeout
pub fn getaddrinfo(
    host: &str,
    port: &str,
    hints: &libc::addrinfo,
    timeout: f64,
) -> Result<libc::addrinfo, Error> {
    let mut result: *mut libc::addrinfo = null_mut();
    if unsafe {
        ffi::coio_getaddrinfo(
            host.as_ptr() as *const c_char,
            port.as_ptr() as *const c_char,
            hints,
            &mut result,
            timeout,
        )
    } < 0
    {
        Err(TarantoolError::last().into())
    } else {
        Ok(unsafe { result.read() })
    }
}

#[inline(always)]
pub(crate) fn read(
    fd: RawFd,
    buf: &mut [u8],
    timeout: Option<Duration>,
) -> Result<usize, io::Error> {
    let buf_len = buf.len();
    let result = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut c_void, buf_len) };
    if result >= 0 {
        return Ok(result as usize);
    }

    let err = io::Error::last_os_error();
    if err.kind() != io::ErrorKind::WouldBlock {
        return Err(err);
    }

    let timeout = match timeout {
        None => TIMEOUT_INFINITY,
        Some(timeout) => timeout.as_secs_f64(),
    };

    coio_wait(fd, ffi::CoIOFlags::READ, timeout)?;
    let result = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut c_void, buf_len) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as usize)
    }
}

#[inline(always)]
pub(crate) fn write(fd: RawFd, buf: &[u8], timeout: Option<Duration>) -> Result<usize, io::Error> {
    let result = unsafe { libc::write(fd, buf.as_ptr() as *mut c_void, buf.len()) };
    if result >= 0 {
        return Ok(result as usize);
    }

    let err = io::Error::last_os_error();
    if err.kind() != io::ErrorKind::WouldBlock {
        return Err(err);
    }

    let timeout = match timeout {
        None => TIMEOUT_INFINITY,
        Some(timeout) => timeout.as_secs_f64(),
    };

    coio_wait(fd, ffi::CoIOFlags::WRITE, timeout)?;
    let result = unsafe { libc::write(fd, buf.as_ptr() as *mut c_void, buf.len()) };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result as usize)
    }
}

/// Creates a new asynchronous channel, returning the sender/receiver halves.
///
/// All data sent on the Sender will become available on the [Receiver] in the same order as it was sent,
/// and no `send` will block the calling fiber, `recv` will block until a message is available.
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let chan = Rc::new(Chan {
        buffer: RefCell::new(VecDeque::with_capacity(capacity)),
        cond: Cond::new(),
        tx_count: Cell::new(1),
        rx_is_active: Cell::new(true),
    });

    (Sender(chan.clone()), Receiver(chan))
}

/// The sending half of channel.
///
/// Messages can be sent through this channel with `send`. Can be cloned.
pub struct Sender<T>(Rc<Chan<T>>);

impl<T> Sender<T> {
    /// Attempts to send a value on this channel, returning it back if it could not be sent.
    /// This method will never block.
    pub fn send(&self, value: T) -> Result<(), io::Error> {
        if !self.0.rx_is_active.get() {
            return Err(io::ErrorKind::NotConnected.into());
        }

        let was_empty = {
            let mut buffer = self.0.buffer.borrow_mut();
            let was_empty = buffer.len() == 0;
            buffer.push_back(value);
            was_empty
        };

        if was_empty {
            self.0.cond.signal();
        }

        Ok(())
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.0.tx_count.set(self.0.tx_count.get() + 1);
        Sender(self.0.clone())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.0.tx_count.set(self.0.tx_count.get() - 1);
        self.0.cond.signal();
    }
}

/// The receiving half of channel.
pub struct Receiver<T>(Rc<Chan<T>>);

impl<T> Receiver<T> {
    /// Attempts to wait for a value on this receiver, returning `None` if the corresponding channel has hung up.
    pub fn recv(&self) -> Option<T> {
        if self.0.buffer.borrow().len() == 0 {
            if self.0.tx_count.get().is_zero() {
                return None;
            }

            self.0.cond.wait();
        }

        self.0.buffer.borrow_mut().pop_front()
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.0.rx_is_active.set(false);
    }
}

struct Chan<T> {
    buffer: RefCell<VecDeque<T>>,
    cond: Cond,
    tx_count: Cell<usize>,
    rx_is_active: Cell<bool>,
}
