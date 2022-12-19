use std::ffi::{CString, NulError};
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::os::unix::io::RawFd;
use std::ptr;
use std::time::Duration;

use libc::{addrinfo, close, connect, freeaddrinfo, socket, AF_UNSPEC, SOCK_STREAM};

use crate::coio::{read, write, CoIOStream};
use crate::ffi::tarantool as ffi;
use crate::fiber::Cond;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to resolve host address by domain name")]
    ResolveAddress,
    #[error("Input parameters contain ffi incompatible strings: {0}")]
    ConstructCString(NulError),
    #[error("Failed to connect to supplied address")]
    Connect,
}

pub struct TcpStream {
    fd: RawFd,
}

impl TcpStream {
    pub unsafe fn new(
        url: &str,
        port: Option<&str>,
        timeout: Duration,
    ) -> Result<TcpStream, Error> {
        let addrs = Self::get_address_info(url, port, timeout)?;
        let res = Self::connect(addrs);
        freeaddrinfo(addrs);
        res
    }

    unsafe fn connect(addrs: *mut addrinfo) -> Result<TcpStream, Error> {
        unsafe fn connect_single(addr: addrinfo) -> Option<TcpStream> {
            let sfd = socket(addr.ai_family, addr.ai_socktype, addr.ai_protocol);
            if sfd == -1 {
                close(sfd);
                return None;
            }
            if connect(sfd, addr.ai_addr, addr.ai_addrlen) == -1 {
                close(sfd);
                return None;
            }
            Some(TcpStream { fd: sfd })
        }

        let mut addr = *addrs;
        loop {
            if let Some(stream) = connect_single(addr) {
                return Ok(stream);
            }
            if !addr.ai_next.is_null() {
                addr = *addr.ai_next
            } else {
                break;
            }
        }
        Err(Error::Connect)
    }

    unsafe fn get_address_info(
        url: &str,
        port: Option<&str>,
        timeout: Duration,
    ) -> Result<*mut addrinfo, Error> {
        let url = CString::new(url).map_err(Error::ConstructCString)?;
        let port = port
            .map(|port| CString::new(port).map_err(Error::ConstructCString))
            .transpose()?;
        let mut hints = MaybeUninit::<addrinfo>::zeroed().assume_init();
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        let mut addrs = ptr::null_mut();
        let res = ffi::coio_getaddrinfo(
            url.as_ptr(),
            match port {
                Some(port) => port.as_ptr(),
                None => ptr::null(),
            },
            &hints as *const _,
            &mut addrs as *mut _,
            timeout.as_secs_f64(),
        );
        if res != 0 {
            return Err(Error::ResolveAddress);
        }
        Ok(addrs)
    }

    // TODO fn write -> Future
    // TODO fn read -> Future
    // TODO fn read_exact -> Future
}

#[cfg(feature = "tarantool_test")]
mod tests {
    use super::*;

    use crate::test::{TestCase, TESTS};
    use crate::test_name;

    use linkme::distributed_slice;

    #[distributed_slice(TESTS)]
    static RESOLVE_ADDRESS: TestCase = TestCase {
        name: test_name!("resolve_address"),
        f: || unsafe {
            let _ =
                TcpStream::get_address_info("localhost", None, Duration::from_secs(10)).unwrap();
        },
    };
}
