use std::io::{self, ErrorKind, Read, Write};
use std::os::unix::net::UnixStream;
use std::str::from_utf8;
use std::time::Duration;

use crate::tarantool_main;
use crate::util::unwrap_or_terminate;

use super::args;
use super::console::{Console, ReplError};

/// Wrapper around unix socket with console-like interface
/// for communicating with tarantool console.
pub struct UnixClient {
    socket: UnixStream,
    buffer: Vec<u8>,
}

#[derive(thiserror::Error, Debug)]
pub enum UnixClientError {
    #[error("error during IO: {0}")]
    Io(#[from] io::Error),

    #[error("malformed output: {0}")]
    DeserializeMessageError(String),
}

pub type Result<T> = std::result::Result<T, UnixClientError>;

impl UnixClient {
    const SERVER_DELIM: &str = "$EOF$\n";
    const CLIENT_DELIM: &[u8] = b"\n...\n";
    const WAIT_TIMEOUT: u64 = 10;
    const INITIAL_BUFFER_SIZE: usize = 1024;

    fn from_stream(socket: UnixStream) -> Result<Self> {
        socket.set_read_timeout(Some(Duration::from_secs(Self::WAIT_TIMEOUT)))?;
        Ok(UnixClient {
            socket,
            buffer: vec![0; Self::INITIAL_BUFFER_SIZE],
        })
    }

    /// Creates struct object using `path` for raw unix socket.
    ///
    /// Setup delimiter and ignore tarantool prompt.
    fn new(path: &str) -> Result<Self> {
        let socket = UnixStream::connect(path)?;
        let mut client = Self::from_stream(socket)?;

        // set delimiter
        let prelude: &str = "require(\"console\").delimiter(\"$EOF$\")\n";
        client.write_raw(prelude)?;

        // Ignore tarantool prompt.
        // Prompt looks like:
        // "Tarantool $version (Lua console)
        //  type 'help' for interactive help"
        let prompt = client.read()?;
        debug_assert!(prompt.contains("Tarantool"));
        debug_assert!(prompt.contains("Lua console"));

        Ok(client)
    }

    /// Writes message appended with delimiter to tarantool console
    fn write(&mut self, line: &str) -> Result<()> {
        self.write_raw(&(line.to_owned() + Self::SERVER_DELIM))
    }

    fn write_raw(&mut self, line: &str) -> Result<()> {
        self.socket
            .write_all(line.as_bytes())
            .map_err(UnixClientError::Io)
    }

    /// Reads response from tarantool console.
    /// Blocks until delimiter sequence or timeout is reached.
    ///
    /// # Errors
    /// Returns error in the following cases:
    /// 1. Read timeout
    /// 2. Deserialization failure
    fn read(&mut self) -> Result<String> {
        let mut pos = 0;
        loop {
            let read = match self.socket.read(&mut self.buffer[pos..]) {
                Ok(n) => n,
                Err(ref e) if e.kind() == ErrorKind::Interrupted => {
                    continue;
                }
                Err(err) => return Err(err.into()),
            };

            pos += read;

            // tarantool console appends delimiter to each response.
            // Delimiter can be changed, but since we not do it manually, it's ok
            if self.buffer[..pos].ends_with(Self::CLIENT_DELIM) {
                break;
            }

            if pos == self.buffer.len() {
                self.buffer.resize(pos * 2, 0);
            }
        }

        let deserialized = from_utf8(&self.buffer[..pos])
            .map_err(|err| UnixClientError::DeserializeMessageError(err.to_string()))?
            .to_string();

        return Ok(deserialized);
    }
}

fn admin_repl(args: args::Admin) -> core::result::Result<(), ReplError> {
    let mut client = UnixClient::new(&args.socket_path).map_err(|err| {
        ReplError::Other(format!(
            "connection via unix socket by path '{}' is not established, reason: {}",
            args.socket_path, err
        ))
    })?;

    let mut console = Console::new("picoadmin :) ")?;

    while let Some(line) = console.read()? {
        client.write(&line)?;
        let response = client.read()?;
        console.write(&response);
    }

    Ok(())
}

pub fn main(args: args::Admin) -> ! {
    let rc = tarantool_main!(
        args.tt_args().unwrap(),
        callback_data: args,
        callback_data_type: args::Admin,
        callback_body: {
            unwrap_or_terminate(admin_repl(args));
            std::process::exit(0)
        }
    );

    std::process::exit(rc);
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixStream;

    use rmp::encode::RmpWrite;

    use super::{UnixClient, UnixClientError};

    fn setup_client_server() -> (UnixClient, UnixStream) {
        let (client, server) = UnixStream::pair().unwrap();
        let unix_client = UnixClient::from_stream(client).unwrap();
        (unix_client, server)
    }

    #[test]
    fn delimiter_timeout() {
        let (mut client, mut server) = setup_client_server();
        server.write_bytes(b"output without delim").unwrap();
        let output = client.read();
        assert!(output.is_err());
    }

    #[test]
    fn non_utf8_output() {
        let (mut client, mut server) = setup_client_server();
        let non_utf = b"\x00\x9f\x92\x96\n...\n";
        server.write_bytes(non_utf).unwrap();
        let output = client.read();
        match output {
            Err(UnixClientError::DeserializeMessageError(_)) => (),
            _ => panic!(),
        }
    }

    #[test]
    fn output_with_delimiter_is_accepted() {
        let (mut client, mut server) = setup_client_server();
        server.write_bytes(b"output with delimiter\n...\n").unwrap();
        let output = client.read();
        assert!(output.is_ok());
        assert_eq!(output.unwrap(), "output with delimiter\n...\n");
    }

    #[test]
    fn resize_logic() {
        let (mut client, mut server) = setup_client_server();
        let initial_buf_size = client.buffer.len(); // 1024
        let delimiter = b"\n...\n";
        let mut big_output = vec![0u8; 1024];
        big_output.extend(delimiter);
        server.write_bytes(&big_output.as_slice()).unwrap();
        let output = client.read();
        assert!(output.is_ok());
        assert!(client.buffer.len() > initial_buf_size);
        assert!(client.buffer.len() == initial_buf_size * 2);
    }
}
