use std::{
    cmp::{self, min},
    io::{BufWriter, Cursor, Read, Seek, Write},
    vec::Drain,
};

use crate::error::Error;

use super::{
    api::{self, Request},
    codec::{self, Header},
    options::ConnOptions,
    SyncIndex,
};

pub type Response = Vec<u8>;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum State {
    /// Awaits greeting
    Init,
    /// Awaits auth
    Auth,
    /// Ready to accept new messages
    Ready,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum SizeHint {
    Hint(usize),
    FirstU32,
}

/// A sans-io connection handler.
///
/// Uses events and actions to communicate with the specific
/// client implementation.
pub struct Conn {
    state: State,
    ready_data: Vec<u8>,
    pending_data: Vec<u8>,
    sync: SyncIndex,
    // TODO: remove everything besides name and password from options
    options: ConnOptions,
}

impl Conn {
    pub fn with_options(options: ConnOptions) -> Self {
        Self {
            state: State::Init,
            sync: SyncIndex(0),
            pending_data: Vec::new(),
            options,
            ready_data: Vec::new(),
        }
    }

    pub fn is_ready(&self) -> bool {
        matches!(self.state, State::Ready)
    }

    pub fn send_request(&mut self, request: &impl Request) -> Result<SyncIndex, Error> {
        let end = self.pending_data.len();
        let mut buf = Cursor::new(&mut self.pending_data);
        buf.set_position(end as u64);
        // TODO: limit the pending vec size
        write_to_buffer(&mut buf, self.sync, request)?;
        self.process_pending_data();
        Ok(self.sync.next())
    }

    pub fn read_size_hint(&self) -> SizeHint {
        if let State::Init = self.state {
            // Greeting message is exactly 128 bytes
            SizeHint::Hint(128)
        } else {
            SizeHint::FirstU32
        }
    }

    // TODO: handle multiple chunks in incoming data
    fn process_data<R: Read + Seek>(
        &mut self,
        chunk: &mut R,
    ) -> Result<Option<(Header, Response)>, Error> {
        let response = match self.state {
            State::Init => {
                let salt = codec::decode_greeting(chunk)?;
                if self.options.user.is_empty() {
                    // No auth
                    self.state = State::Ready;
                } else {
                    // Auth
                    self.state = State::Auth;
                    let end = self.pending_data.len();
                    let user = self.options.user.as_ref();
                    let pass = self.options.password.as_ref();
                    let mut buf = Cursor::new(&mut self.ready_data);
                    buf.set_position(end as u64);
                    let sync = self.sync.next();
                    write_to_buffer(
                        &mut buf,
                        sync,
                        &api::Auth {
                            user,
                            pass,
                            salt: &salt,
                        },
                    );
                }
                None
            }
            State::Auth => {
                // TODO: Will the client get the length of both header and error?
                let header = codec::decode_header(chunk)?;
                if header.status_code != 0 {
                    return Err(codec::decode_error(chunk)?.into());
                }
                self.state = State::Ready;
                None
            }
            State::Ready => {
                let header = codec::decode_header(chunk)?;
                if header.status_code != 0 {
                    return Err(codec::decode_error(chunk)?.into());
                }
                let mut buf = Vec::new();
                chunk.read_to_end(&mut buf);
                Some((header, buf))
            }
        };
        self.process_pending_data();
        Ok(response)
    }

    pub fn ready_data_len(&self) -> usize {
        self.ready_data.len()
    }

    pub fn drain_ready_data(&mut self, max: Option<usize>) -> Drain<u8> {
        let bound = if let Some(max) = max {
            cmp::min(self.ready_data_len(), max)
        } else {
            self.ready_data_len()
        };
        self.ready_data.drain(..bound)
    }

    fn process_pending_data(&mut self) {
        if self.is_ready() {
            let pending_data = self.pending_data.drain(..);
            // TODO: limit the ready vec size
            self.ready_data.extend(pending_data);
        }
    }
}

pub fn write_to_buffer(
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
    use std::convert::TryInto;
    use std::io::Write;

    use super::*;

    /// See [tarantool docs](https://www.tarantool.io/en/doc/latest/dev_guide/internals/iproto/authentication/#greeting-message).
    fn fake_greeting() -> Vec<u8> {
        let mut greeting = Vec::new();
        greeting.extend([0; 63].iter());
        greeting.push(b'\n');
        greeting.extend(b"QK2HoFZGXTXBq2vFj7soCsHqTo6PGTF575ssUBAJLAI=".into_iter());
        while greeting.len() < 127 {
            greeting.push(0);
        }
        greeting.push(b'\n');
        greeting
    }

    #[test]
    fn connection_established() {
        let mut conn = Conn::with_options(Default::default());
        assert!(!conn.is_ready());
        conn.process_data(&mut Cursor::new(fake_greeting()));
        assert!(conn.is_ready())
    }

    #[test]
    fn send_bytes_generated() {
        let mut conn = Conn::with_options(Default::default());
        conn.process_data(&mut Cursor::new(fake_greeting()));
        conn.send_request(&api::Ping).unwrap();
        assert!(conn.ready_data_len() > 0);
    }
}
