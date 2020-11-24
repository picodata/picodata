//! Cooperative input/output
//!
//! See also:
//! - [C API reference: Module coio](https://www.tarantool.io/en/doc/latest/dev_guide/reference_capi/coio/)
use std::convert::TryFrom;
use std::ffi::c_void;
use std::io;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::os::raw::{c_char, c_int};
use std::os::unix::io::{AsRawFd, IntoRawFd, RawFd};

use failure::_core::ptr::null_mut;

use crate::error::{Error, TarantoolError};
use crate::fiber::unpack_callback;

const TIMEOUT_INFINITY: f64 = 365.0 * 86400.0 * 100.0;

/// Uses CoIO main loop to poll read/write events from wrapped socket
pub struct CoIOStream {
    fd: RawFd,
}

bitflags! {
    /// Event type(s) to wait. Can be `READ` or/and `WRITE`
    pub struct CoIOFlags: c_int {
        const READ = 1;
        const WRITE = 2;
    }
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

    /// Pull some bytes from this source into the specified buffer. Returns how many bytes were read or 0 on timeout.
    fn read_with_timeout(&mut self, buf: &mut [u8], timeout: f64) -> Result<usize, io::Error> {
        let buf_len = buf.len();
        let result = unsafe { libc::read(self.fd, buf.as_mut_ptr() as *mut c_void, buf_len) };
        if result >= 0 {
            return Ok(result as usize);
        }

        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::WouldBlock {
            return Err(err);
        }

        coio_wait(self.fd, CoIOFlags::READ, timeout)?;
        let result = unsafe { libc::read(self.fd, buf.as_mut_ptr() as *mut c_void, buf_len) };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result as usize)
        }
    }

    /// Write a buffer into this writer. Returning how many bytes were written or 0 on timeout.
    fn write_with_timeout(&mut self, buf: &[u8], timeout: f64) -> Result<usize, io::Error> {
        let result = unsafe { libc::write(self.fd, buf.as_ptr() as *mut c_void, buf.len()) };
        if result >= 0 {
            return Ok(result as usize);
        }

        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::WouldBlock {
            return Err(err);
        }

        coio_wait(self.fd, CoIOFlags::WRITE, timeout)?;
        let result = unsafe { libc::write(self.fd, buf.as_ptr() as *mut c_void, buf.len()) };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(result as usize)
        }
    }
}

impl IntoRawFd for CoIOStream {
    fn into_raw_fd(self) -> RawFd {
        self.fd
    }
}

impl Read for CoIOStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.read_with_timeout(buf, TIMEOUT_INFINITY)
    }
}

impl Write for CoIOStream {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.write_with_timeout(buf, TIMEOUT_INFINITY)
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
                        coio_wait(self.inner.as_raw_fd(), CoIOFlags::READ, TIMEOUT_INFINITY)?;
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
pub fn coio_wait(fd: RawFd, flags: CoIOFlags, timeout: f64) -> Result<(), io::Error> {
    match unsafe { ffi::coio_wait(fd, flags.bits, timeout) } {
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
/// ```
/// struct FuncArgs {}
///
/// fn func(args: FuncArgs) -> i32 {}
///
/// if coio_call(func, FuncArgs{}) == -1 {
///		// handle errors.
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
            &*hints,
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

mod ffi {
    use std::os::raw::{c_char, c_int};

    use va_list::VaList;

    extern "C" {
        pub fn coio_wait(fd: c_int, event: c_int, timeout: f64) -> c_int;
        pub fn coio_close(fd: c_int) -> c_int;
        pub fn coio_getaddrinfo(
            host: *const c_char,
            port: *const c_char,
            hints: *const libc::addrinfo,
            res: *mut *mut libc::addrinfo,
            timeout: f64,
        ) -> c_int;
        pub fn coio_call(func: Option<unsafe extern "C" fn(VaList) -> c_int>, ...) -> isize;
    }
}
