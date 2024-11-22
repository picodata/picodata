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

#[derive(thiserror::Error, Debug)]
pub enum ReplError {
    #[error("{0}")]
    Client(#[from] ClientError),

    #[error("{0}")]
    UnixClient(#[from] UnixClientError),

    #[error("{0}")]
    Io(#[from] io::Error),

    #[error("{0}")]
    EditorError(#[from] ReadlineError),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, ReplError>;
const DELIMITER: &str = ";";

pub enum SpecialCommand {
    SwitchLanguageToLua,
    SwitchLanguageToSql,
    PrintHelp,
}

pub enum Command {
    // A builtin REPL command
    Control(SpecialCommand),
    // Either lua or sql expression
    Expression(String),
}

enum ConsoleLanguage {
    Lua,
    Sql,
}

enum ConsoleCommand {
    SetLanguage(ConsoleLanguage),
    // None represent default delimiter (pressing enter in console and eof in case of pipe)
    SetDelimiter(Option<String>),
    Invalid,
}

/// Input/output handler
pub struct Console<H: Helper> {
    editor: Editor<H, FileHistory>,
    history_file_path: PathBuf,
    delimiter: Option<String>,
    // Queue of separated by delimiter statements
    separated_statements: VecDeque<String>,
    uncompleted_statement: String,
    eof_received: bool,
}

impl<T: Helper> Console<T> {
    const HISTORY_FILE_NAME: &'static str = ".picodata_history";
    const PROMPT: &'static str = "picodata> ";
    const INNER_PROMPT: &'static str = "        > ";
    const SPECIAL_COMMAND_PREFIX: &'static str = "\\";

    fn handle_special_command(&mut self, command: &str) -> Result<ControlFlow<Command>> {
        match command {
            "\\e" | "\\e;" => self.open_external_editor(),
            "\\help" | "\\h" | "\\help;" | "\\h;" => Ok(ControlFlow::Break(Command::Control(
                SpecialCommand::PrintHelp,
            ))),
            "\\lua" | "\\lua;" => Ok(ControlFlow::Break(Command::Control(
                SpecialCommand::SwitchLanguageToLua,
            ))),
            "\\sql" | "\\sql;" => Ok(ControlFlow::Break(Command::Control(
                SpecialCommand::SwitchLanguageToSql,
            ))),
            _ => self.handle_parsed_command(command),
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
                "{:?} returned non-zero exit status: {}",
                editor, status
            ));
            return Ok(ControlFlow::Continue(()));
        }

        let line = read_to_string(temp.path()).map_err(ReplError::Io)?;
        Ok(ControlFlow::Break(Command::Expression(line)))
    }

    fn handle_parsed_command(&mut self, command: &str) -> Result<ControlFlow<Command>> {
        match self.parse_special_command(command) {
            ConsoleCommand::SetLanguage(ConsoleLanguage::Lua) => Ok(ControlFlow::Break(
                Command::Control(SpecialCommand::SwitchLanguageToLua),
            )),
            ConsoleCommand::SetLanguage(ConsoleLanguage::Sql) => Ok(ControlFlow::Break(
                Command::Control(SpecialCommand::SwitchLanguageToSql),
            )),
            ConsoleCommand::SetDelimiter(delimiter) => {
                self.update_delimiter(delimiter);
                Ok(ControlFlow::Continue(()))
            }
            ConsoleCommand::Invalid => {
                self.write("Unknown special sequence");
                Ok(ControlFlow::Continue(()))
            }
        }
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

    fn update_history(&mut self, command: Command) -> Result<Option<Command>> {
        // do not save special commands
        if let Command::Expression(expression) = &command {
            if let Err(e) = self.editor.add_history_entry(expression) {
                println!("error while updating history: {e}");
            }
            if let Err(e) = self.editor.save_history(&self.history_file_path) {
                println!("error while saving history: {e}");
            }
        }

        Ok(Some(command))
    }

    fn process_command(&mut self) {
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
                    ControlFlow::Break(command) => return self.update_history(command),
                }
            }

            let prompt = if self.uncompleted_statement.is_empty() {
                Self::PROMPT
            } else {
                self.uncompleted_statement.push(' ');
                Self::INNER_PROMPT
            };
            let readline = self.editor.readline(prompt);

            match readline {
                Ok(line) => {
                    // process special command with no need for delimiter
                    if line.starts_with(Self::SPECIAL_COMMAND_PREFIX) {
                        let processed = self.handle_special_command(&line)?;

                        match processed {
                            ControlFlow::Continue(_) => continue,
                            ControlFlow::Break(command) => return self.update_history(command),
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
        println!("{}", line)
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
        })
    }
}
