use std::convert::TryFrom;
use std::io;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::raw::c_int;
use std::os::unix::io::AsRawFd;

use crate::c_api;

pub const TIMEOUT_INFINITY: f64 = 365.0 * 86400.0 * 100.0;


pub struct CoIOStream<T> {
    inner: T
}

impl<T> CoIOStream<T> where T: Read + Write + AsRawFd{
    pub fn new(inner: TcpStream) -> Result<CoIOStream<TcpStream>, io::Error> {
        inner.set_nonblocking(true)?;
        Ok(CoIOStream{inner})
    }

    pub fn inner_stream(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T> Read for CoIOStream<T> where T: Read + AsRawFd {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let res = self.inner.read(buf);
        match res {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                wait(&self.inner, c_api::CoioFlags::Read, TIMEOUT_INFINITY)?;
                self.inner.read(buf)
            }
            res => res,
        }
    }
}

impl<T> Write for CoIOStream<T> where T: Write + AsRawFd {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        let res = self.inner.write(buf);
        match res {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                wait(&self.inner, c_api::CoioFlags::Write, TIMEOUT_INFINITY)?;
                self.inner.write(buf)
            }
            res => res,
        }
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        self.inner.flush()
    }
}

pub struct CoIOListener {
    inner: TcpListener
}

impl CoIOListener {
    pub fn accept(&self) -> Result<CoIOStream<TcpStream>, io::Error> {
        loop {
            let res = self.inner.accept();
            return match res {
                Ok((stream, _)) => {
                    stream.set_nonblocking(true)?;
                    Ok(CoIOStream {
                        inner: stream
                    })
                },

                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        wait(&self.inner, c_api::CoioFlags::Read, TIMEOUT_INFINITY)?;
                        continue;
                    }
                    Err(e)
                },
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
        Ok(Self {
            inner: value
        })
    }
}

fn wait<T>(fp: &T, flags: c_api::CoioFlags, timeout: f64) -> Result<(), io::Error> where T: AsRawFd{
    match unsafe { c_api::coio_wait(fp.as_raw_fd(), flags as c_int, timeout) } {
        0 => Err(io::ErrorKind::TimedOut.into()),
        _ => Ok(())
    }
}
