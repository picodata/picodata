/*
 * Original work Copyright (c) 2025 Neon, Inc
 * Modified work Copyright (c) 2025 Picodata
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Simple Authentication and Security Layer.
//!
//! RFC: <https://datatracker.ietf.org/doc/html/rfc4422>.
//!
//! Reference implementation:
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/backend/libpq/auth-sasl.c>
//! * <https://github.com/postgres/postgres/blob/94226d4506e66d6e7cbf4b391f1e7393c1962841/src/interfaces/libpq/fe-auth.c>

mod channel_binding;

use std::io;
use thiserror::Error;

pub use channel_binding::ChannelBinding;

/// Fine-grained auth errors help in writing tests.
#[derive(Error, Debug)]
pub enum Error {
    #[error("Unsupported authentication method: {0}")]
    BadAuthMethod(Box<str>),

    #[error("Channel binding failed: {0}")]
    ChannelBindingFailed(&'static str),

    #[error("Unsupported channel binding method: {0}")]
    ChannelBindingBadMethod(Box<str>),

    #[error("Bad client message: {0}")]
    BadClientMessage(&'static str),

    #[error("Internal error: missing digest")]
    MissingBinding,

    #[error("could not decode salt: {0}")]
    Base64(#[from] base64::DecodeError),

    #[error(transparent)]
    Io(#[from] io::Error),
}

/// A convenient result type for SASL exchange.
pub type Result<T> = std::result::Result<T, Error>;

/// A result of one SASL exchange.
#[must_use]
pub enum Step<T, R> {
    /// We should continue exchanging messages.
    Continue(T, String),

    /// The client has been authenticated successfully.
    Success(R, String),

    /// Authentication failed.
    Failure,
}

/// Every SASL mechanism (e.g. [SCRAM](crate::scram)) is expected to implement this trait.
pub trait Mechanism: Sized {
    /// What's produced as a result of successful authentication.
    type Output;

    /// Produce a server challenge to be sent to the client.
    /// This is how this method is called in PostgreSQL (`libpq/sasl.h`).
    fn exchange(self, input: &str) -> Result<Step<Self, Self::Output>>;
}
