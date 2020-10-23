use std::convert::TryFrom;
use std::io;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::raw::{c_char, c_int};
use std::os::unix::io::AsRawFd;

use failure::_core::ptr::null_mut;

use crate::error::TarantoolError;
use crate::Error;

pub const TIMEOUT_INFINITY: f64 = 365.0 * 86400.0 * 100.0;

/// Uses CoIO main loop to poll read/write events from wrapped socket
pub struct CoIOStream<T> {
    inner: T,
}

impl<T> CoIOStream<T>
where
    T: Read + Write + AsRawFd,
{
    pub fn new(inner: TcpStream) -> Result<CoIOStream<TcpStream>, io::Error> {
        inner.set_nonblocking(true)?;
        Ok(CoIOStream { inner })
    }

    pub fn inner_stream(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T> Read for CoIOStream<T>
where
    T: Read + AsRawFd,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let res = self.inner.read(buf);
        match res {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                wait(&self.inner, ffi::CoioFlags::Read, TIMEOUT_INFINITY)?;
                self.inner.read(buf)
            }
            res => res,
        }
    }
}

impl<T> Write for CoIOStream<T>
where
    T: Write + AsRawFd,
{
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        let res = self.inner.write(buf);
        match res {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                wait(&self.inner, ffi::CoioFlags::Write, TIMEOUT_INFINITY)?;
                self.inner.write(buf)
            }
            res => res,
        }
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        self.inner.flush()
    }
}

/// Uses CoIO main loop to poll incoming connections from wrapped socket listener
pub struct CoIOListener {
    inner: TcpListener,
}

impl CoIOListener {
    pub fn accept(&self) -> Result<CoIOStream<TcpStream>, io::Error> {
        loop {
            let res = self.inner.accept();
            return match res {
                Ok((stream, _)) => {
                    stream.set_nonblocking(true)?;
                    Ok(CoIOStream { inner: stream })
                }

                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        wait(&self.inner, ffi::CoioFlags::Read, TIMEOUT_INFINITY)?;
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

pub fn wait<T>(fp: &T, flags: ffi::CoioFlags, timeout: f64) -> Result<(), io::Error>
where
    T: AsRawFd,
{
    match unsafe { ffi::coio_wait(fp.as_raw_fd(), flags as c_int, timeout) } {
        0 => Err(io::ErrorKind::TimedOut.into()),
        _ => Ok(()),
    }
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

    #[repr(u32)]
    #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
    pub enum CoioFlags {
        Read = 1,
        Write = 2,
    }

    extern "C" {
        pub fn coio_wait(fd: c_int, event: c_int, timeout: f64) -> c_int;

        #[allow(dead_code)]
        pub fn coio_close(fd: c_int) -> c_int;

        pub fn coio_getaddrinfo(
            host: *const c_char,
            port: *const c_char,
            hints: *const libc::addrinfo,
            res: *mut *mut libc::addrinfo,
            timeout: f64,
        ) -> c_int;
    }
}
