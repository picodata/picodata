//! Protocol description without actual network layer

use std::fmt::{Display, Formatter};

use self::options::ConnOptions;
pub mod codec;
pub mod conn;
pub mod options;
pub mod send_queue;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SyncIndex(u64);

impl SyncIndex {
    pub fn next(&mut self) -> Self {
        let sync = self.0;
        self.0 += 1;
        Self(sync)
    }
}

#[derive(Debug)]
pub struct ResponseError {
    message: String,
}

impl Display for ResponseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<ResponseError> for crate::error::Error {
    fn from(error: ResponseError) -> Self {
        crate::error::Error::Remote(crate::net_box::ResponseError {
            message: error.message,
        })
    }
}
