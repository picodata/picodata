use std::io;
use tarantool::coio::CoIOListener;

pub fn new_listener(addr: (&str, u16)) -> io::Result<CoIOListener> {
    let mut socket = None;
    let mut f = |_| {
        let wrapped = std::net::TcpListener::bind(addr);
        log::debug!("PG socket bind result: {wrapped:?}");
        socket.replace(wrapped);
        0
    };

    if tarantool::coio::coio_call(&mut f, ()) != 0 {
        return Err(io::Error::last_os_error());
    }

    let socket = socket.expect("uninitialized socket")?;
    tarantool::coio::CoIOListener::try_from(socket)
}
