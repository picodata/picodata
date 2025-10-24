use super::tcp::TcpStream;
use super::tls::TlsStream;
use futures::{AsyncRead, AsyncWrite};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// An asynchronous network stream that supports both plain and encrypted connections.
///
/// This enum abstracts away the differences between plain TCP streams and
/// TLS-encrypted streams, allowing unified handling of both connection types.
#[derive(Debug, Clone)]
pub enum Stream {
    Plain(TcpStream),
    Secure(TlsStream),
}

impl Stream {
    pub fn shutdown(&self) -> io::Result<()> {
        match self {
            Self::Plain(tcp) => tcp.close(),
            Self::Secure(tls) => tls.shutdown(),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(tcp) => Pin::new(tcp).poll_write(cx, buf),
            Self::Secure(tls) => Pin::new(tls).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(tcp) => Pin::new(tcp).poll_flush(cx),
            Self::Secure(tls) => Pin::new(tls).poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(tcp) => Pin::new(tcp).poll_close(cx),
            Self::Secure(tls) => Pin::new(tls).poll_close(cx),
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(tcp) => Pin::new(tcp).poll_read(cx, buf),
            Self::Secure(tls) => Pin::new(tls).poll_read(cx, buf),
        }
    }
}
