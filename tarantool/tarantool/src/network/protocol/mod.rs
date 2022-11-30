//! Protocol description without actual network layer

use std::fmt::{Display, Formatter};
pub mod codec;
pub mod options;
pub mod send_queue;

#[derive(Debug, Copy, Clone)]
pub enum ConnState {
    Init,
    Connecting,
    Auth,
    Active,
    Error,
    ErrorReconnect,
    Closed,
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
