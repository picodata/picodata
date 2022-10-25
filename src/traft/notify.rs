use ::tarantool::fiber;

use std::time::Duration;

use super::error::Error;

use crate::util::{downcast, AnyWithTypeName};

#[derive(Clone)]
pub struct Notify {
    ch: fiber::Channel<Result<Box<dyn AnyWithTypeName>, Error>>,
}

impl Notify {
    #[inline]
    pub fn new() -> Self {
        Self {
            ch: fiber::Channel::new(1),
        }
    }

    #[inline]
    pub fn notify_ok_any(&self, res: Box<dyn AnyWithTypeName>) {
        self.ch.try_send(Ok(res)).ok();
    }

    #[inline]
    pub fn notify_ok<T: AnyWithTypeName>(&self, res: T) {
        self.notify_ok_any(Box::new(res));
    }

    #[inline]
    pub fn notify_err<E: Into<Error>>(&self, err: E) {
        self.ch.try_send(Err(err.into())).ok();
    }

    #[inline]
    pub fn recv_any(self) -> Result<Box<dyn AnyWithTypeName>, Error> {
        match self.ch.recv() {
            Some(v) => v,
            None => {
                self.ch.close();
                Err(Error::Timeout)
            }
        }
    }

    #[inline]
    pub fn recv_timeout_any(self, timeout: Duration) -> Result<Box<dyn AnyWithTypeName>, Error> {
        match self.ch.recv_timeout(timeout) {
            Ok(v) => v,
            Err(_) => {
                self.ch.close();
                Err(Error::Timeout)
            }
        }
    }

    #[inline]
    pub fn recv_timeout<T: 'static>(self, timeout: Duration) -> Result<T, Error> {
        downcast(self.recv_timeout_any(timeout)?)
    }

    #[inline]
    #[allow(unused)]
    pub fn recv<T: 'static>(self) -> Result<T, Error> {
        downcast(self.recv_any()?)
    }

    pub fn is_closed(&self) -> bool {
        self.ch.is_closed()
    }
}

impl std::fmt::Debug for Notify {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Notify").finish_non_exhaustive()
    }
}
