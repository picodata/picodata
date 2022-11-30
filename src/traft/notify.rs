use ::tarantool::fiber::r#async::oneshot;
use ::tarantool::fiber::r#async::timeout::IntoTimeout as _;

use std::time::Duration;

use crate::traft::error::Error;
use crate::traft::Result;
use crate::util::{downcast, AnyWithTypeName};

pub struct Notifier(oneshot::Sender<Result<Box<dyn AnyWithTypeName>>>);
pub struct Notify(oneshot::Receiver<Result<Box<dyn AnyWithTypeName>>>);

#[inline]
pub fn notification() -> (Notifier, Notify) {
    let (tx, rx) = oneshot::channel();
    (Notifier(tx), Notify(rx))
}

impl Notifier {
    #[inline]
    pub fn notify_ok_any(self, res: Box<dyn AnyWithTypeName>) {
        self.0.send(Ok(res)).ok();
    }

    #[inline]
    pub fn notify_ok<T: AnyWithTypeName>(self, res: T) {
        self.notify_ok_any(Box::new(res));
    }

    #[inline]
    #[allow(dead_code)]
    pub fn notify_err<E: Into<Error>>(self, err: E) {
        self.0.send(Err(err.into())).ok();
    }

    #[inline]
    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

impl Notify {
    #[inline]
    pub async fn recv_any(self) -> Result<Box<dyn AnyWithTypeName>> {
        match self.0.await.ok() {
            Some(v) => v,
            None => Err(Error::Timeout),
        }
    }

    #[inline]
    pub async fn recv_timeout_any(self, timeout: Duration) -> Result<Box<dyn AnyWithTypeName>> {
        match self.0.timeout(timeout).await {
            Ok(Ok(v)) => v,
            Ok(Err(_)) | Err(_) => Err(Error::Timeout),
        }
    }

    #[inline]
    pub async fn recv_timeout<T: 'static>(self, timeout: Duration) -> Result<T> {
        downcast(self.recv_timeout_any(timeout).await?)
    }

    #[inline]
    #[allow(unused)]
    pub async fn recv<T: 'static>(self) -> Result<T> {
        downcast(self.recv_any().await?)
    }
}

impl std::fmt::Debug for Notify {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Notify").finish_non_exhaustive()
    }
}
