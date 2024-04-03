use std::cell::RefCell;
use std::io::{self, ErrorKind, Read, Write};
use std::os::unix::net::UnixStream;
use std::rc::Rc;
use std::str::from_utf8;
use std::time::Duration;

use rustyline::completion::{extract_word, Completer};
use rustyline::error::ReadlineError;
use rustyline::Context;
use rustyline_derive::{Completer, Helper, Highlighter, Hinter, Validator};

use crate::tarantool_main;

use super::args;
use super::connect::ResultSet;
use super::console::{Command, Console, ReplError, SpecialCommand};

pub struct LuaCompleter {
    client: Rc<RefCell<UnixClient>>,
}

impl Completer for LuaCompleter {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> std::result::Result<(usize, Vec<Self::Candidate>), ReadlineError> {
        let is_break_char = |ch: char| ch == ' ' || ch == '(';
        let (start, to_complete) = extract_word(line, pos, None, is_break_char);

        let res = self
            .client
            .borrow_mut()
            .complete_input(to_complete, start, pos);

        let completions = match res {
            Ok(completions) => completions,
            Err(e) => {
                crate::tlog!(Warning, "getting completions failed: {e}");
                Vec::new()
            }
        };

        Ok((start, completions))
    }
}

#[derive(Completer, Helper, Validator, Hinter, Highlighter)]
pub struct LuaHelper {
    #[rustyline(Completer)]
    completer: LuaCompleter,
}

#[derive(PartialEq)]
enum ConsoleLanguage {
    Lua,
    Sql,
}

/// Wrapper around unix socket with console-like interface
/// for communicating with tarantool console.
pub struct UnixClient {
    socket: UnixStream,
    current_language: ConsoleLanguage,
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
    const SERVER_DELIM: &'static str = "$EOF$\n";
    const CLIENT_DELIM: &'static [u8] = b"\n...\n";
    const WAIT_TIMEOUT: u64 = 10;
    const INITIAL_BUFFER_SIZE: usize = 1024;

    fn from_stream(socket: UnixStream) -> Result<Self> {
        socket.set_read_timeout(Some(Duration::from_secs(Self::WAIT_TIMEOUT)))?;
        Ok(UnixClient {
            socket,
            current_language: ConsoleLanguage::Lua,
            buffer: vec![0; Self::INITIAL_BUFFER_SIZE],
        })
    }

    /// Creates struct object using `path` for raw unix socket.
    ///
    /// Setup delimiter, default language and ignore tarantool prompt.
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

        // set default language SQL
        client.write("\\set language sql")?;
        let response = client.read()?;
        debug_assert!(response.contains("true"));
        client.current_language = ConsoleLanguage::Sql;

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
                Ok(0) => {
                    return Err(UnixClientError::Io(io::Error::other(
                        "server probably is closed, try to reconnect",
                    )))
                }
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

    fn complete_input(&mut self, line: &str, left: usize, right: usize) -> Result<Vec<String>> {
        // Completions are available only for Lua
        if self.current_language != ConsoleLanguage::Lua {
            return Ok(Vec::new());
        }

        self.write(&format!(
            "return require(\"console\").completion_handler(\"{}\", {}, {})",
            line, left, right
        ))?;

        let response = self.read()?;

        // Completions are returned in the following yaml format:
        // ---
        // - null              <-- case when no completion was proposed
        // ...
        //
        // ---
        // - - $current_line
        //   - completion_1
        //   - completion_2    <-- case when at least one completion was proposed
        //   - completion_3
        // ...
        let completions: Option<Vec<Vec<String>>> = serde_yaml::from_str(&response)
            .map_err(|msg| UnixClientError::DeserializeMessageError(msg.to_string()))?;

        let res = completions
            .unwrap_or_default()
            .first()
            .map(|v| v[1..].into())
            .unwrap_or_default();

        Ok(res)
    }
}

fn admin_repl(args: args::Admin) -> core::result::Result<(), ReplError> {
    let client = UnixClient::new(&args.socket_path).map_err(|err| {
        ReplError::Other(format!(
            "connection via unix socket by path '{}' is not established, reason: {}",
            args.socket_path, err
        ))
    })?;

    // SAFETY: client mutably borrowed in the following "functions":
    // `console.read()`, REPL (bellow in this function)
    // It is impossible situation, when REPL "called" from console.read() and vice versa
    let client = Rc::new(RefCell::new(client));

    let helper = LuaHelper {
        completer: LuaCompleter {
            client: client.clone(),
        },
    };

    let mut console = Console::with_completer(helper)?;

    console.greet(&format!(
        "Connected to admin console by socket path \"{}\"",
        args.socket_path
    ));

    const HELP_MESSAGE: &'static str = "
    Available backslash commands:
        \\e            Open the editor specified by the EDITOR environment variable
        \\help         Show this screen
        \\sql          Switch console language to SQL (default)
        \\lua          Switch console language to Lua (deprecated)

    Available hotkeys:
        Enter         Submit the request
        Alt  + Enter  Insert a newline character
        Ctrl + C      Discard current input
        Ctrl + D      Quit interactive console";

    while let Some(command) = console.read()? {
        let mut temp_client = client.borrow_mut();
        match command {
            Command::Control(command) => match command {
                SpecialCommand::SwitchLanguageToLua => {
                    temp_client.write("\\set language lua")?;
                    temp_client.read()?;
                    temp_client.current_language = ConsoleLanguage::Lua;
                    console.write("Language switched to Lua");
                }

                SpecialCommand::SwitchLanguageToSql => {
                    temp_client.write("\\set language sql")?;
                    temp_client.read()?;
                    temp_client.current_language = ConsoleLanguage::Sql;
                    console.write("Language switched to SQL");
                }

                SpecialCommand::PrintHelp => {
                    console.write(HELP_MESSAGE);
                }
            },
            Command::Expression(line) => {
                temp_client.write(&line)?;
                let raw_response = temp_client.read()?;
                let formatted = match temp_client.current_language {
                    ConsoleLanguage::Lua => raw_response,
                    ConsoleLanguage::Sql => serde_yaml::from_str::<ResultSet>(&raw_response)
                        .map_err(|err| {
                            ReplError::Other(format!(
                                "error occured while processing output: {}",
                                err
                            ))
                        })?
                        .to_string(),
                };

                console.write(&formatted);
            }
        };
    }

    Ok(())
}

pub fn main(args: args::Admin) -> ! {
    let rc = tarantool_main!(
        args.tt_args().unwrap(),
        callback_data: args,
        callback_data_type: args::Admin,
        callback_body: {
            if let Err(e) = admin_repl(args) {
                crate::tlog!(Critical, "{e}");
                std::process::exit(1);
            }
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
            _ => assert!(false),
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
        server.write_bytes(big_output.as_slice()).unwrap();
        let output = client.read();
        assert!(output.is_ok());
        assert!(client.buffer.len() > initial_buf_size);
        assert!(client.buffer.len() == initial_buf_size * 2);
    }

    #[test]
    fn server_die() {
        let (mut client, server) = setup_client_server();
        drop(server);
        let output = client.read();
        match output {
            Err(UnixClientError::Io(err)) => assert_eq!(
                err.to_string(),
                "server probably is closed, try to reconnect"
            ),
            _ => assert!(false),
        }
    }
}
