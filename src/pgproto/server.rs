use crate::tlog;
use std::io;
use tarantool::coio::CoIOListener;

pub fn new_listener(addr: (&str, u16)) -> tarantool::Result<CoIOListener> {
    let mut socket = None;
    let mut f = |_| {
        let wrapped = std::net::TcpListener::bind(addr);
        tlog!(Debug, "PG socket bind result: {wrapped:?}");
        socket.replace(wrapped);
        0
    };

    if tarantool::coio::coio_call(&mut f, ()) != 0 {
        return Err(io::Error::last_os_error().into());
    }

    let socket = socket.expect("uninitialized socket")?;
    let listener = tarantool::coio::CoIOListener::try_from(socket)?;
    Ok(listener)
}
