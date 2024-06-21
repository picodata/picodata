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

/// Input/output handler
pub struct Console<H: Helper> {
    editor: Editor<H, FileHistory>,
    history_file_path: PathBuf,
    delimiter: Option<String>,
    // Queue for handling lines with several delimiters.
    buffer: VecDeque<String>,
}

impl<T: Helper> Console<T> {
    const HISTORY_FILE_NAME: &'static str = ".picodata_history";
    const PROMPT: &'static str = "picodata> ";
    const SPECIAL_COMMAND_PREFIX: &'static str = "\\";

    fn handle_special_command(&mut self, command: &str) -> Result<ControlFlow<Command>> {
        if command == "\\e" {
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
                    "{:?} returned non zero exit status: {}",
                    editor, status
                ));
                return Ok(ControlFlow::Continue(()));
            }

            // we don't check content intentionally
            let line = read_to_string(temp.path()).map_err(ReplError::Io)?;

            return Ok(ControlFlow::Break(Command::Expression(line)));
        } else if ["\\help", "\\h"].contains(&command) {
            return Ok(ControlFlow::Break(Command::Control(
                SpecialCommand::PrintHelp,
            )));
        } else if command == "\\lua" {
            return Ok(ControlFlow::Break(Command::Control(
                SpecialCommand::SwitchLanguageToLua,
            )));
        } else if command == "\\sql" {
            return Ok(ControlFlow::Break(Command::Control(
                SpecialCommand::SwitchLanguageToSql,
            )));
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

        fn parse_special_command(command: &str) -> ConsoleCommand {
            let splitted = command.split_whitespace().collect::<Vec<_>>();

            if splitted.len() < 3 {
                return ConsoleCommand::Invalid;
            }

            if splitted[0] != "\\s" && splitted[0] != "\\set" {
                return ConsoleCommand::Invalid;
            }

            if splitted[1] == "language" || splitted[1] == "l" || splitted[1] == "lang" {
                if splitted[2] == "lua" {
                    return ConsoleCommand::SetLanguage(ConsoleLanguage::Lua);
                }

                if splitted[2] == "sql" {
                    return ConsoleCommand::SetLanguage(ConsoleLanguage::Sql);
                }
            }

            if splitted[1] == "delimiter" || splitted[1] == "d" || splitted[1] == "delim" {
                let delimiter = splitted[2];
                if delimiter == "default" {
                    return ConsoleCommand::SetDelimiter(None);
                }

                return ConsoleCommand::SetDelimiter(Some(delimiter.into()));
            }

            ConsoleCommand::Invalid
        }

        match parse_special_command(command) {
            ConsoleCommand::SetLanguage(language) => match language {
                ConsoleLanguage::Lua => Ok(ControlFlow::Break(Command::Control(
                    SpecialCommand::SwitchLanguageToLua,
                ))),
                ConsoleLanguage::Sql => Ok(ControlFlow::Break(Command::Control(
                    SpecialCommand::SwitchLanguageToSql,
                ))),
            },
            ConsoleCommand::SetDelimiter(delimiter) => {
                match delimiter {
                    Some(custom) => {
                        self.write(&format!("Delimiter changed to '{custom}'"));
                        self.delimiter = Some(custom);
                    }
                    None => {
                        self.write("Delimiter changed to default");
                        self.delimiter = None;
                    }
                }
                Ok(ControlFlow::Continue(()))
            }
            ConsoleCommand::Invalid => {
                self.write("Unknown special sequence");
                Ok(ControlFlow::Continue(()))
            }
        }
    }

    fn process_input(&mut self, input: &str) -> Result<ControlFlow<Command>> {
        if input.starts_with(Self::SPECIAL_COMMAND_PREFIX) {
            return self.handle_special_command(input);
        }

        Ok(ControlFlow::Break(Command::Expression(input.into())))
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

    pub fn read(&mut self) -> Result<Option<Command>> {
        loop {
            // firstly try to consume buffer filled for example from one line with several delimiters,
            // then read from stdin
            while let Some(input) = self.buffer.pop_front() {
                match self.process_input(&input)? {
                    ControlFlow::Continue(_) => continue,
                    ControlFlow::Break(command) => return self.update_history(command),
                }
            }

            let readline = self.editor.readline(Self::PROMPT);

            match readline {
                Ok(input) => {
                    let statement = {
                        if let Some(ref delimiter) = self.delimiter {
                            let splitted = input.split(delimiter).collect::<Vec<_>>();
                            for command in &splitted[1..] {
                                self.buffer.push_back(command.to_string())
                            }

                            // We are sure that splitting any string results in a non empty vector
                            splitted[0].to_string()
                        } else {
                            input.to_string()
                        }
                    };

                    match self.process_input(&statement)? {
                        ControlFlow::Continue(_) => continue,
                        ControlFlow::Break(command) => return self.update_history(command),
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    self.write("CTRL+C");
                    continue;
                }
                Err(ReadlineError::Eof) => {
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
            delimiter: None,
            buffer: VecDeque::new(),
        })
    }
}

impl Console<()> {
    pub fn new() -> Result<Self> {
        let (editor, history_file_path) = Self::editor_with_history()?;

        Ok(Console {
            editor,
            history_file_path,
            delimiter: None,
            buffer: VecDeque::new(),
        })
    }
}
