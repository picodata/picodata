use super::tls::{TlsAcceptor, TlsStream};
use crate::tlog;
use bytes::{BufMut, BytesMut};
use pgwire::messages::startup::SslRequest;
use std::io::{self, ErrorKind::UnexpectedEof, Write};

// Public re-exports.
pub use pgwire::messages::{
    self, PgWireBackendMessage as BeMessage, PgWireFrontendMessage as FeMessage,
};

fn read_into_buf(reader: &mut impl io::Read, buf: &mut impl BufMut) -> io::Result<usize> {
    // TODO: check if it's empty (+ resize).
    let slice = buf.chunk_mut();

    // SAFETY: coio's Read impl won't read uninitialized bytes.
    let uninit = unsafe { slice.as_uninit_slice_mut() };
    // SAFETY: safe, because MaybeUninit has repr(transparent)
    let buffer =
        unsafe { std::mem::transmute::<&mut [std::mem::MaybeUninit<u8>], &mut [u8]>(uninit) };
    let cnt = reader.read(buffer)?;

    // SAFETY: safe, because reader has just inserted this data
    unsafe { buf.advance_mut(cnt) }
    Ok(cnt)
}

enum PgSocket<S> {
    Plain(S),
    Secure(TlsStream<S>),
}

// TlsStream used for secure sockets requires Read and Write for reading.
impl<S: io::Read + io::Write> io::Read for PgSocket<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            PgSocket::Plain(sock) => sock.read(buf),
            PgSocket::Secure(sock) => sock.read(buf),
        }
    }
}

// TlsStream used for secure sockets requires Read and Write for writing.
impl<S: io::Read + io::Write> io::Write for PgSocket<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            PgSocket::Plain(sock) => sock.write(buf),
            PgSocket::Secure(sock) => sock.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            PgSocket::Plain(sock) => sock.flush(),
            PgSocket::Secure(sock) => sock.flush(),
        }
    }
}

/// Postgres connection wrapper for raw streams.
pub struct PgStream<S> {
    /// Raw byte stream (duplex).
    socket: PgSocket<S>,
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
            socket: PgSocket::Plain(raw),
            ibuf: BytesMut::with_capacity(INITIAL_BUF_SIZE),
            obuf: BytesMut::with_capacity(INITIAL_BUF_SIZE),
            startup_processed: false,
        }
    }
}

/// Read part of the stream.
impl<S: io::Read + io::Write> PgStream<S> {
    /// Try decoding an incoming message considering the connection's state.
    /// Return `None` if the packet is not complete for parsing.
    fn try_decode_message(&mut self) -> io::Result<Option<FeMessage>> {
        use messages::{startup::Startup, Message};

        if self.startup_processed {
            return FeMessage::decode(&mut self.ibuf).map_err(io::Error::other);
        }

        // Try to decode SslRequest first, as it fits the Startup format with an invalid version.
        let res = SslRequest::decode(&mut self.ibuf).map_err(io::Error::other)?;
        if let Some(ssl_request) = res {
            return Ok(Some(FeMessage::SslRequest(ssl_request)));
        }

        // This is done once at connection startup.
        let res = Startup::decode(&mut self.ibuf).map_err(io::Error::other)?;
        let startup = res.map(|x| {
            tlog!(Debug, "received StartupPacket from client");
            self.startup_processed = true;
            FeMessage::Startup(x)
        });

        Ok(startup)
    }

    /// Receive a new message from client.
    pub fn read_message(&mut self) -> io::Result<FeMessage> {
        loop {
            if !self.ibuf.is_empty() {
                if let Some(message) = self.try_decode_message()? {
                    return Ok(message);
                }
            }

            let cnt = read_into_buf(&mut self.socket, &mut self.ibuf)?;
            tlog!(Debug, "received {cnt} bytes from client");

            if cnt == 0 {
                return Err(io::Error::from(UnexpectedEof));
            }
        }
    }
}

/// Write part of the stream.
impl<S: io::Read + io::Write> PgStream<S> {
    /// Flush all buffered messages to an underlying byte stream.
    pub fn flush(&mut self) -> io::Result<&mut Self> {
        self.socket.write_all(&self.obuf)?;
        self.obuf.clear();
        Ok(self)
    }

    /// Put the message into the output buffer, but don't flush just yet.
    pub fn write_message_noflush(&mut self, message: BeMessage) -> io::Result<&mut Self> {
        message.encode(&mut self.obuf).map_err(io::Error::other)?;
        Ok(self)
    }

    /// Put the message into the output buffer and immediately flush everything.
    pub fn write_message(&mut self, message: BeMessage) -> io::Result<&mut Self> {
        self.write_message_noflush(message)?.flush()
    }
}

impl<S: io::Read + io::Write> PgStream<S> {
    pub fn into_secure(self, acceptor: &TlsAcceptor) -> io::Result<PgStream<S>> {
        let PgSocket::Plain(socket) = self.socket else {
            panic!("BUG: cannot upgrade TLS stream");
        };

        let secure_socket = acceptor.accept(socket).map_err(io::Error::other)?;
        let stream = PgStream {
            socket: PgSocket::Secure(secure_socket),
            ..self
        };

        Ok(stream)
    }
}
