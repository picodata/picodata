use std::cell::Cell;
use std::io::{self, Read, Write};
use std::os::unix::io::{IntoRawFd, RawFd};
use std::rc::Rc;

use crate::coio::{read, write, CoIOStream};
use crate::error::Error;
use crate::ffi::tarantool as ffi;
use crate::fiber::Cond;

pub struct ConnStream {
    fd: RawFd,
    reader_guard: Rc<ConnStreamGuard>,
    writer_guard: Rc<ConnStreamGuard>,
}

impl ConnStream {
    pub fn new(stream: CoIOStream) -> Result<Self, Error> {
        Ok(ConnStream {
            fd: stream.into_raw_fd(),
            reader_guard: Rc::new(ConnStreamGuard {
                is_acquired: Cell::new(false),
                drop_cond: Cond::new(),
            }),
            writer_guard: Rc::new(ConnStreamGuard {
                is_acquired: Cell::new(false),
                drop_cond: Cond::new(),
            }),
        })
    }

    pub fn acquire_reader(&self) -> ConnStreamReader {
        self.reader_guard.wait();
        self.reader_guard.is_acquired.set(true);
        ConnStreamReader {
            fd: self.fd,
            reader_guard: self.reader_guard.clone(),
        }
    }

    pub fn acquire_writer(&self) -> ConnStreamWriter {
        self.writer_guard.wait();
        self.writer_guard.is_acquired.set(true);
        ConnStreamWriter {
            fd: self.fd,
            writer_guard: self.writer_guard.clone(),
        }
    }
}

struct ConnStreamGuard {
    is_acquired: Cell<bool>,
    drop_cond: Cond,
}

impl ConnStreamGuard {
    fn wait(&self) {
        if self.is_acquired.get() {
            self.drop_cond.wait();
        }
    }
}

impl Drop for ConnStream {
    fn drop(&mut self) {
        self.reader_guard.wait();
        self.writer_guard.wait();
        unsafe { ffi::coio_close(self.fd) };
    }
}

pub struct ConnStreamReader {
    fd: RawFd,
    reader_guard: Rc<ConnStreamGuard>,
}

impl Read for ConnStreamReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        read(self.fd, buf, None)
    }
}

impl Drop for ConnStreamReader {
    fn drop(&mut self) {
        self.reader_guard.is_acquired.set(false);
        self.reader_guard.drop_cond.signal();
    }
}

pub struct ConnStreamWriter {
    fd: RawFd,
    writer_guard: Rc<ConnStreamGuard>,
}

impl Write for ConnStreamWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        write(self.fd, buf, None)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for ConnStreamWriter {
    fn drop(&mut self) {
        self.writer_guard.is_acquired.set(false);
        self.writer_guard.drop_cond.signal();
    }
}
