use ::tarantool::fiber;

use std::any::Any;
use std::time::Duration;

use super::error::Error;

#[derive(Clone)]
pub struct Notify {
    ch: fiber::Channel<Result<Box<dyn Any>, Error>>,
}

impl Notify {
    pub fn new() -> Self {
        Self {
            ch: fiber::Channel::new(1),
        }
    }

    pub fn notify_ok_any(&self, res: Box<dyn Any>) {
        self.ch.try_send(Ok(res)).ok();
    }

    pub fn notify_ok<T: Any>(&self, res: T) {
        self.notify_ok_any(Box::new(res));
    }

    pub fn notify_err<E: Into<Error>>(&self, err: E) {
        self.ch.try_send(Err(err.into())).ok();
    }

    pub fn recv_any(self) -> Result<Box<dyn Any>, Error> {
        match self.ch.recv() {
            Some(v) => v,
            None => {
                self.ch.close();
                Err(Error::Timeout)
            }
        }
    }

    pub fn recv_timeout_any(self, timeout: Duration) -> Result<Box<dyn Any>, Error> {
        match self.ch.recv_timeout(timeout) {
            Ok(v) => v,
            Err(_) => {
                self.ch.close();
                Err(Error::Timeout)
            }
        }
    }

    pub fn recv_timeout<T: 'static>(self, timeout: Duration) -> Result<T, Error> {
        let any: Box<dyn Any> = self.recv_timeout_any(timeout)?;
        let boxed: Box<T> = any.downcast().map_err(|_| Error::DowncastError)?;
        Ok(*boxed)
    }

    #[allow(unused)]
    pub fn recv<T: 'static>(self) -> Result<T, Error> {
        let any: Box<dyn Any> = self.recv_any()?;
        let boxed: Box<T> = any.downcast().map_err(|_| Error::DowncastError)?;
        Ok(*boxed)
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
