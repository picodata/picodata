use nix::unistd::isatty;
use std::collections::VecDeque;
use std::env;
use std::fs::read_to_string;
use std::io;
use std::ops::ControlFlow;
use std::path::Path;
use std::path::PathBuf;
use std::process;

use rustyline::config::Configurer;
use rustyline::Helper;
use rustyline::{error::ReadlineError, history::FileHistory, Editor};
use tarantool::network::ClientError;

use super::admin::LuaHelper;
use super::admin::UnixClientError;
use std::error::Error;

#[derive(thiserror::Error, Debug)]
pub enum ReplError {
    #[error("{0}")]
    Client(#[from] ClientError),

    #[error("{0}")]
    UnixClient(UnixClientError),

    #[error("{0}")]
    Io(#[from] io::Error),

    #[error("{0}")]
    EditorError(#[from] ReadlineError),

    #[error("lost connection to the server: {0}")]
    LostConnectionToServer(Box<dyn Error>),

    #[error("{0}")]
    Other(String),
}

impl ReplError {
    pub fn other<E>(error: E) -> Self
    where
        E: Into<Box<dyn Error>>,
    {
        Self::Other(error.into().to_string())
    }
}

impl From<UnixClientError> for ReplError {
    fn from(error: UnixClientError) -> Self {
        if let UnixClientError::Io(e) = error {
            return ReplError::LostConnectionToServer(e.into());
        }
        ReplError::UnixClient(error)
    }
}

pub type Result<T> = std::result::Result<T, ReplError>;
const DELIMITER: &str = ";";

#[derive(Clone, Copy, PartialEq)]
pub enum ConsoleLanguage {
    Lua,
    Sql,
}

impl std::fmt::Display for ConsoleLanguage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConsoleLanguage::Lua => write!(f, "lua"),
            ConsoleLanguage::Sql => write!(f, "sql"),
        }
    }
}

pub enum SpecialCommand {
    SwitchLanguage(ConsoleLanguage),
    PrintHelp,
}

pub enum Command {
    // A builtin REPL command
    Control(SpecialCommand),
    // Either lua or sql expression
    Expression(String),
}

enum ConsoleCommand {
    SetLanguage(ConsoleLanguage),
    // None represent default delimiter (pressing enter in console and eof in case of pipe)
    SetDelimiter(Option<String>),
    Invalid,
}

#[derive(PartialEq)]
pub enum Mode {
    Admin,
    Connection,
}

/// Input/output handler
pub struct Console<H: Helper> {
    editor: Editor<H, FileHistory>,
    history_file_path: PathBuf,
    delimiter: Option<String>,
    current_language: ConsoleLanguage,
    pub mode: Mode,
    // Queue of separated by delimiter statements
    separated_statements: VecDeque<String>,
    uncompleted_statement: String,
    eof_received: bool,
}

impl<T: Helper> Console<T> {
    const HISTORY_FILE_NAME: &'static str = ".picodata_history";
    const INNER_PROMPT_FOR_CONNECT: &'static str = "   > ";
    const INNER_PROMPT_FOR_ADMIN: &'static str = "           > ";
    const SPECIAL_COMMAND_PREFIX: &'static str = "\\";
    const LUA_PROMPT: &'static str = "lua> ";
    const SQL_PROMPT: &'static str = "sql> ";
    const ADMIN_MODE: &'static str = "(admin) ";

    fn handle_special_command(&mut self, command: &str) -> Result<ControlFlow<Command>> {
        use ConsoleLanguage::*;
        use SpecialCommand::*;

        let parsed_command = match command {
            "\\lua" | "\\lua;" => Some(SwitchLanguage(Lua)),
            "\\sql" | "\\sql;" => Some(SwitchLanguage(Sql)),
            "\\help" | "\\h" | "\\help;" | "\\h;" => Some(PrintHelp),
            "\\e" | "\\e;" => return self.open_external_editor(),
            _ => match self.parse_special_command(command) {
                ConsoleCommand::SetLanguage(language) => Some(SwitchLanguage(language)),
                ConsoleCommand::SetDelimiter(delimiter) => {
                    self.update_delimiter(delimiter);
                    None
                }
                ConsoleCommand::Invalid => {
                    self.write("Unknown special sequence");
                    None
                }
            },
        };

        match parsed_command {
            Some(command) => match command {
                SwitchLanguage(console_language) => {
                    if self.mode != Mode::Admin {
                        self.write("Language cannot be changed in this console");
                        Ok(ControlFlow::Continue(()))
                    } else {
                        self.current_language = console_language;
                        Ok(ControlFlow::Break(Command::Control(command)))
                    }
                }
                PrintHelp => Ok(ControlFlow::Break(Command::Control(command))),
            },
            None => Ok(ControlFlow::Continue(())),
        }
    }

    fn open_external_editor(&mut self) -> Result<ControlFlow<Command>> {
        let editor = match env::var_os("EDITOR") {
            Some(e) => e,
            None => {
                self.write("EDITOR environment variable is not set");
                return Ok(ControlFlow::Continue(()));
            }
        };

        let temp = tempfile::Builder::new().suffix(".sql").tempfile()?;
        let status = process::Command::new(&editor).arg(temp.path()).status()?;

        if !status.success() {
            self.write(&format!(
                "{editor:?} returned non-zero exit status: {status}",
            ));
            return Ok(ControlFlow::Continue(()));
        }

        let line = read_to_string(temp.path()).map_err(ReplError::Io)?;
        Ok(ControlFlow::Break(Command::Expression(line)))
    }

    fn update_delimiter(&mut self, delimiter: Option<String>) {
        match delimiter {
            Some(custom) => {
                self.write(&format!("Delimiter changed to '{custom}'"));
                self.delimiter = Some(custom);
            }
            None => {
                self.write("Delimiter changed to 'enter'");
                self.delimiter = None;
            }
        }
    }

    fn parse_special_command(&self, command: &str) -> ConsoleCommand {
        let parts: Vec<&str> = command.split_whitespace().collect();

        if parts.len() < 3 || !["\\s", "\\set"].contains(&parts[0]) {
            return ConsoleCommand::Invalid;
        }

        match parts[1] {
            "language" | "l" | "lang" => match parts.get(2) {
                Some(&"lua") => ConsoleCommand::SetLanguage(ConsoleLanguage::Lua),
                Some(&"sql") => ConsoleCommand::SetLanguage(ConsoleLanguage::Sql),
                _ => ConsoleCommand::Invalid,
            },
            "delimiter" | "d" | "delim" => match parts.get(2).copied() {
                Some("default") => ConsoleCommand::SetDelimiter(Some(DELIMITER.to_string())),
                Some("enter") => ConsoleCommand::SetDelimiter(None),
                Some(custom) => ConsoleCommand::SetDelimiter(Some(custom.to_string())),
                None => ConsoleCommand::Invalid,
            },
            _ => ConsoleCommand::Invalid,
        }
    }

    fn update_history(&mut self, command: Command) -> Command {
        // do not save special commands
        if let Command::Expression(expression) = &command {
            let history_entry = match self.current_language {
                ConsoleLanguage::Lua => expression.clone(),
                ConsoleLanguage::Sql => {
                    expression.clone() + &self.delimiter.clone().unwrap_or_default()
                }
            };

            if let Err(e) = self.editor.add_history_entry(history_entry) {
                println!("error while updating history: {e}");
            }
            if let Err(e) = self.editor.save_history(&self.history_file_path) {
                println!("error while saving history: {e}");
            }
        }

        command
    }

    fn process_command(&mut self) {
        match self.current_language {
            ConsoleLanguage::Lua => {
                if !self.uncompleted_statement.trim().is_empty() {
                    self.separated_statements
                        .push_back(std::mem::take(&mut self.uncompleted_statement));
                }
            }
            ConsoleLanguage::Sql => {
                if let Some(ref delimiter) = self.delimiter {
                    while let Some((separated_part, tail)) =
                        self.uncompleted_statement.split_once(delimiter)
                    {
                        self.separated_statements.push_back(separated_part.into());
                        self.uncompleted_statement = tail.into();
                    }
                } else {
                    // if delimiter is None (enter), treat statement as single command
                    if !self.uncompleted_statement.trim().is_empty() {
                        self.separated_statements
                            .push_back(std::mem::take(&mut self.uncompleted_statement));
                    }
                }
            }
        }
    }

    pub fn read(&mut self) -> Result<Option<Command>> {
        loop {
            if self.eof_received {
                self.write("Bye");
                return Ok(None);
            }

            while let Some(separated_input) = self.separated_statements.pop_front() {
                let processed = {
                    if separated_input.starts_with(Self::SPECIAL_COMMAND_PREFIX) {
                        self.handle_special_command(&separated_input)?
                    } else {
                        ControlFlow::Break(Command::Expression(separated_input))
                    }
                };

                match processed {
                    ControlFlow::Continue(_) => continue,
                    ControlFlow::Break(command) => return Ok(Some(self.update_history(command))),
                }
            }

            let prompt = if !self.uncompleted_statement.is_empty() {
                self.uncompleted_statement.push(' ');
                match self.mode {
                    Mode::Admin => Self::INNER_PROMPT_FOR_ADMIN,
                    Mode::Connection => Self::INNER_PROMPT_FOR_CONNECT,
                }
                .to_string()
            } else {
                let admin = match self.mode {
                    Mode::Admin => Self::ADMIN_MODE,
                    Mode::Connection => "",
                };
                let language = match self.current_language {
                    ConsoleLanguage::Lua => Self::LUA_PROMPT,
                    ConsoleLanguage::Sql => Self::SQL_PROMPT,
                };
                format!("{admin}{language}")
            };

            let readline = self.editor.readline(&prompt);

            match readline {
                Ok(line) => {
                    // process special command with no need for delimiter
                    if line.starts_with(Self::SPECIAL_COMMAND_PREFIX) {
                        let processed = self.handle_special_command(&line)?;

                        match processed {
                            ControlFlow::Continue(_) => continue,
                            ControlFlow::Break(command) => {
                                return Ok(Some(self.update_history(command)))
                            }
                        }
                    } else {
                        self.uncompleted_statement += &line;
                        self.process_command();
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    self.write("CTRL+C");
                }
                Err(ReadlineError::Eof) => {
                    let is_terminal = isatty(0).unwrap_or(false);

                    if !is_terminal && !self.uncompleted_statement.is_empty() {
                        self.eof_received = true;
                        return Ok(Some(Command::Expression(std::mem::take(
                            &mut self.uncompleted_statement,
                        ))));
                    }

                    self.write("Bye");
                    return Ok(None);
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    pub fn write(&self, line: &str) {
        println!("{line}")
    }

    fn editor_with_history() -> Result<(Editor<T, FileHistory>, PathBuf)> {
        let mut editor = Editor::new()?;

        // newline by ALT + ENTER
        editor.bind_sequence(
            rustyline::KeyEvent(rustyline::KeyCode::Enter, rustyline::Modifiers::ALT),
            rustyline::EventHandler::Simple(rustyline::Cmd::Newline),
        );

        // It is deprecated because of unexpected behavior on windows.
        // We're ok with that.
        #[allow(deprecated)]
        let history_file_path = env::home_dir()
            .unwrap_or_default()
            .join(Path::new(Self::HISTORY_FILE_NAME));

        // We're ok with history load failures. E g this is the case
        // for first launch when history file doesnt exist yet
        let _ = editor.load_history(&history_file_path);

        Ok((editor, history_file_path))
    }

    /// Prints information about connection and help hint
    pub fn greet(&self, connection_info: &str) {
        self.write(connection_info);
        self.write("type '\\help' for interactive help");
    }
}

impl Console<LuaHelper> {
    pub fn with_completer(helper: LuaHelper) -> Result<Self> {
        let (mut editor, history_file_path) = Self::editor_with_history()?;

        editor.set_helper(Some(helper));

        editor.set_completion_type(rustyline::CompletionType::List);

        Ok(Console {
            editor,
            history_file_path,
            delimiter: Some(DELIMITER.to_string()),
            separated_statements: VecDeque::new(),
            uncompleted_statement: String::new(),
            eof_received: false,
            current_language: ConsoleLanguage::Sql,
            mode: Mode::Admin,
        })
    }
}

impl Console<()> {
    pub fn new() -> Result<Self> {
        let (editor, history_file_path) = Self::editor_with_history()?;

        Ok(Console {
            editor,
            history_file_path,
            delimiter: Some(DELIMITER.to_string()),
            separated_statements: VecDeque::new(),
            uncompleted_statement: String::new(),
            eof_received: false,
            current_language: ConsoleLanguage::Sql,
            mode: Mode::Connection,
        })
    }
}
