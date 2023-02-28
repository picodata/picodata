//! Tarantool Binary Protocol implementation without actual transport layer.
//!
//! According to Sans-I/O pattern this implementation processes requests and responses
//! by outputing the corresponding bytes into incoming and outgoing buffers.
//! And it provides an API for the upper client layer to get data from these buffers.
//!
//! See [`super::client`] if you need a fully functional Tarantool client.

pub mod api;
pub mod codec;

use std::cmp;
use std::collections::HashMap;
use std::io::{Cursor, Read, Seek};
use std::str::Utf8Error;
use std::vec::Drain;

use api::Request;

/// Error returned by [`Protocol`].
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("utf8 error: {0}")]
    Utf8(#[from] Utf8Error),
    #[error("failed to encode: {0}")]
    Encode(#[from] rmp::encode::ValueWriteError),
    #[error("failed to decode: {0}")]
    Decode(#[from] rmp::decode::ValueReadError),
    #[error("failed to decode: {0}")]
    DecodeNum(#[from] rmp::decode::NumValueReadError),
    #[error("service responded with error: {0}")]
    Response(#[from] ResponseError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    // TODO: Remove when `Encode` trait will return rmp errors
    #[error("{0}")]
    Other(#[from] Box<crate::error::Error>),
}

impl From<crate::error::Error> for Error {
    fn from(value: crate::error::Error) -> Self {
        Self::Other(Box::new(value))
    }
}

/// Unique identifier of the sent message on this connection.
/// It is used to retrieve response for the corresponding request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SyncIndex(u64);

impl SyncIndex {
    pub fn next_index(&mut self) -> Self {
        let sync = self.0;
        self.0 += 1;
        Self(sync)
    }
}

/// Error returned from the Tarantool server.
///
/// It represents an error with which Tarantool server
/// answers to the client in case of faulty request or an error
/// during request execution on the server side.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct ResponseError {
    pub(crate) message: String,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum State {
    /// Awaits greeting
    Init,
    /// Awaits auth
    Auth,
    /// Ready to accept new messages
    Ready,
}

/// A hint to a client implementation on the expected size of the next
/// message that should be read by receiver.
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum SizeHint {
    /// Receiver should read exactly `n` bytes in `Hint(n)`.
    Hint(usize),
    // TODO: Eliminate this option? Process first u32 here and always give exact hint
    /// The first `u32` that will be read by receiver identifies the size of the message.
    FirstU32,
}

/// Configuration of [`Protocol`].
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct Config {
    /// (user, password)
    pub creds: Option<(String, String)>,
    // TODO: add buffer limits here
}

/// A sans-io connection handler.
///
/// Buffers incoming and outgoing bytes and provides an API for
/// a client implementation to:
/// - Input requests
/// - Get processed responses
/// - Retrieve outgoing bytes
/// - Input incoming bytes
#[derive(Debug)]
pub struct Protocol {
    state: State,
    outgoing: Vec<u8>,
    pending_outgoing: Vec<u8>,
    sync: SyncIndex,
    // TODO: limit incoming size
    incoming: HashMap<SyncIndex, Result<Vec<u8>, ResponseError>>,
    /// (user, password)
    creds: Option<(String, String)>,
}

impl Default for Protocol {
    fn default() -> Self {
        Self::new()
    }
}

impl Protocol {
    /// Construct [`Protocol`] with default values for [`Config`].
    pub fn new() -> Self {
        Self {
            state: State::Init,
            sync: SyncIndex(0),
            pending_outgoing: Vec::new(),
            creds: None,
            outgoing: Vec::new(),
            incoming: HashMap::new(),
        }
    }

    /// Construct [`Protocol`] with custom values for [`Config`].
    pub fn with_config(config: Config) -> Self {
        let mut protocol = Self::new();
        protocol.creds = config.creds;
        protocol
    }

    /// Returns `true` if the [`Protocol`] has passed initialization and authorization
    /// stages.
    ///
    /// Data can be sent independently of whether the protocol [`Self::is_ready`].
    /// If the protocol is not ready data will be queued and eventually processed
    /// after auth is done.
    pub fn is_ready(&self) -> bool {
        matches!(self.state, State::Ready)
    }

    /// Processes incoming request and buffers generated outgoing bytes.
    /// Outgoing bytes can be retrieved with [`Protocol::drain_outgoing_data`]
    ///
    /// Data can be sent independently of whether the protocol [`Self::is_ready`].
    /// If the protocol is not ready data will be queued and eventually processed
    /// after auth is done.
    pub fn send_request(&mut self, request: &impl Request) -> Result<SyncIndex, Error> {
        let end = self.pending_outgoing.len();
        let mut buf = Cursor::new(&mut self.pending_outgoing);
        buf.set_position(end as u64);
        // TODO: limit the pending vec size
        write_to_buffer(&mut buf, self.sync, request)?;
        self.process_pending_data();
        Ok(self.sync.next_index())
    }

    /// Take existing response by [`SyncIndex`].
    pub fn take_response<R: Request>(
        &mut self,
        sync: SyncIndex,
        request: &R,
    ) -> Option<Result<R::Response, Error>> {
        let response = match self.incoming.remove(&sync)? {
            Ok(response) => response,
            Err(err) => return Some(Err(err.into())),
        };
        Some(request.decode_body(&mut Cursor::new(response)))
    }

    /// Drop response by [`SyncIndex`] if it exists. If not - does nothing.
    pub fn drop_response(&mut self, sync: SyncIndex) {
        self.incoming.remove(&sync);
    }

    /// Get [`SizeHint`]. See [`SizeHint`] struct description for more information.
    pub fn read_size_hint(&self) -> SizeHint {
        if let State::Init = self.state {
            // Greeting message is exactly 128 bytes
            SizeHint::Hint(128)
        } else {
            SizeHint::FirstU32
        }
    }

    /// Processes incoming bytes received over transport layer.
    ///
    /// Returns a [`SyncIndex`] if non-technical message was received.
    /// This message can be retreived by this index with [`Protocol::take_response`].
    pub fn process_incoming<R: Read + Seek>(
        &mut self,
        chunk: &mut R,
    ) -> Result<Option<SyncIndex>, Error> {
        let sync = match self.state {
            State::Init => {
                let salt = codec::decode_greeting(chunk)?;
                if let Some((user, pass)) = self.creds.as_ref() {
                    // Auth
                    self.state = State::Auth;
                    // Write straight to outgoing, it should be empty
                    debug_assert!(self.outgoing.is_empty());
                    let mut buf = Cursor::new(&mut self.outgoing);
                    let sync = self.sync.next_index();
                    write_to_buffer(
                        &mut buf,
                        sync,
                        &api::Auth {
                            user,
                            pass,
                            salt: &salt,
                        },
                    )?;
                } else {
                    // No auth
                    self.state = State::Ready;
                }
                None
            }
            State::Auth => {
                let header = codec::decode_header(chunk)?;
                if header.status_code != 0 {
                    return Err(codec::decode_error(chunk)?.into());
                }
                self.state = State::Ready;
                None
            }
            State::Ready => {
                let header = codec::decode_header(chunk)?;
                let response = if header.status_code != 0 {
                    Err(codec::decode_error(chunk)?)
                } else {
                    let mut buf = Vec::new();
                    chunk.read_to_end(&mut buf)?;
                    Ok(buf)
                };
                self.incoming.insert(header.sync, response);
                Some(header.sync)
            }
        };
        self.process_pending_data();
        Ok(sync)
    }

    /// Returns a number of outgoing data bytes.
    pub fn ready_outgoing_len(&self) -> usize {
        self.outgoing.len()
    }

    /// Drains and returns buffered outgoing data.
    ///
    /// The returned bytes can then be sent through a
    /// transport layer to a Tarantool server.
    pub fn drain_outgoing_data(&mut self, max: Option<usize>) -> Drain<u8> {
        let bound = if let Some(max) = max {
            cmp::min(self.ready_outgoing_len(), max)
        } else {
            self.ready_outgoing_len()
        };
        self.outgoing.drain(..bound)
    }

    fn process_pending_data(&mut self) {
        if self.is_ready() {
            let pending_data = self.pending_outgoing.drain(..);
            // TODO: limit the ready vec size
            self.outgoing.extend(pending_data);
        }
    }
}

fn write_to_buffer(
    buffer: &mut Cursor<&mut Vec<u8>>,
    sync: SyncIndex,
    request: &impl Request,
) -> Result<(), Error> {
    // write MSG_SIZE placeholder
    let msg_start_offset = buffer.position();
    rmp::encode::write_u32(buffer, 0)?;

    // write message payload
    let payload_start_offset = buffer.position();
    request.encode(buffer, sync)?;
    let payload_end_offset = buffer.position();

    // calculate and write MSG_SIZE
    buffer.set_position(msg_start_offset);
    rmp::encode::write_u32(buffer, (payload_end_offset - payload_start_offset) as u32)?;
    buffer.set_position(payload_end_offset);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// See [tarantool docs](https://www.tarantool.io/en/doc/latest/dev_guide/internals/iproto/authentication/#greeting-message).
    fn fake_greeting() -> Vec<u8> {
        let mut greeting = Vec::new();
        greeting.extend([0; 63].iter());
        greeting.push(b'\n');
        greeting.extend(b"QK2HoFZGXTXBq2vFj7soCsHqTo6PGTF575ssUBAJLAI=".iter());
        while greeting.len() < 127 {
            greeting.push(0);
        }
        greeting.push(b'\n');
        greeting
    }

    #[test]
    fn connection_established() {
        let mut conn = Protocol::new();
        assert!(!conn.is_ready());
        conn.process_incoming(&mut Cursor::new(fake_greeting()))
            .unwrap();
        assert!(conn.is_ready())
    }

    #[test]
    fn send_bytes_generated() {
        let mut conn = Protocol::new();
        conn.process_incoming(&mut Cursor::new(fake_greeting()))
            .unwrap();
        conn.send_request(&api::Ping).unwrap();
        assert!(conn.ready_outgoing_len() > 0);
    }
}
