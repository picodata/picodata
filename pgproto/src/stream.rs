use crate::error::PgResult;
use bytes::{BufMut, BytesMut};
use std::io::{self, ErrorKind::UnexpectedEof};

// Public re-exports.
pub use pgwire::messages::{
    self, PgWireBackendMessage as BeMessage, PgWireFrontendMessage as FeMessage,
};

fn read_into_buf(reader: &mut impl io::Read, buf: &mut impl BufMut) -> io::Result<usize> {
    // TODO: check if it's empty (+ resize).
    let slice = buf.chunk_mut();

    // SAFETY: coio's Read impl won't read uninitialized bytes.
    let cnt = unsafe {
        let uninit = slice.as_uninit_slice_mut();
        reader.read(std::mem::transmute(uninit))
    }?;

    unsafe { buf.advance_mut(cnt) }
    Ok(cnt)
}

/// Postgres connection wrapper for raw streams.
pub struct PgStream<S> {
    /// Raw byte stream (duplex).
    raw: S,
    /// Receive buffer.
    ibuf: BytesMut,
    /// Send buffer.
    obuf: BytesMut,
    /// True if we've already received StartupPacket.
    startup_processed: bool,
}

impl<S> PgStream<S> {
    pub fn new(raw: S) -> PgStream<S> {
        const INITIAL_BUF_SIZE: usize = 8192;
        PgStream {
            raw,
            ibuf: BytesMut::with_capacity(INITIAL_BUF_SIZE),
            obuf: BytesMut::with_capacity(INITIAL_BUF_SIZE),
            startup_processed: false,
        }
    }
}

/// Read part of the stream.
impl<S: io::Read> PgStream<S> {
    /// Try decoding an incoming message considering the connection's state.
    /// Return `None` if the packet is not complete for parsing.
    fn try_decode_message(&mut self) -> PgResult<Option<FeMessage>> {
        use messages::{startup::Startup, Message};

        if self.startup_processed {
            return FeMessage::decode(&mut self.ibuf).map_err(|e| e.into());
        }

        // This is done once at connection startup.
        let startup = Startup::decode(&mut self.ibuf)?.map(|x| {
            log::debug!("received StartupPacket from client");
            self.startup_processed = true;
            FeMessage::Startup(x)
        });

        Ok(startup)
    }

    /// Receive a new message from client.
    pub fn read_message(&mut self) -> PgResult<FeMessage> {
        loop {
            if !self.ibuf.is_empty() {
                if let Some(message) = self.try_decode_message()? {
                    return Ok(message);
                }
            }

            let cnt = read_into_buf(&mut self.raw, &mut self.ibuf)?;
            log::debug!("received {cnt} bytes from client");
            if cnt == 0 {
                return Err(io::Error::from(UnexpectedEof).into());
            }
        }
    }
}

/// Write part of the stream.
impl<S: io::Write> PgStream<S> {
    /// Flush all buffered messages to an underlying byte stream.
    pub fn flush(&mut self) -> PgResult<&mut Self> {
        self.raw.write_all(&self.obuf)?;
        self.obuf.clear();
        Ok(self)
    }

    /// Put the message into the output buffer, but don't flush just yet.
    pub fn write_message_noflush(&mut self, message: BeMessage) -> PgResult<&mut Self> {
        message.encode(&mut self.obuf)?;
        Ok(self)
    }

    /// Put the message into the output buffer and immediately flush everything.
    pub fn write_message(&mut self, message: BeMessage) -> PgResult<&mut Self> {
        self.write_message_noflush(message)?.flush()
    }
}
