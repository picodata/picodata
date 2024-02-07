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
use tarantool::network::client;

use super::admin::LuaHelper;
use super::admin::UnixClientError;

#[derive(thiserror::Error, Debug)]
pub enum ReplError {
    #[error("{0}")]
    Client(#[from] client::Error),

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

/// Shows user of console
pub enum ConsoleType {
    Admin,
    User,
}

/// Input/output handler
pub struct Console<H: Helper> {
    editor: Editor<H, FileHistory>,
    history_file_path: PathBuf,
    console_type: ConsoleType,
}

impl<T: Helper> Console<T> {
    const HISTORY_FILE_NAME: &str = ".picodata_history";
    const PROMPT: &str = "picodata> ";

    // Ideally we should have an enum for all commands. For now we have only two options, usual line
    // and only one special command. To not overengineer things at this point just handle this as ifs.
    // When the set of commands grows it makes total sense to transform this to clear parse/execute pipeline
    // and separate enum variants for each command variant.
    fn process_line(&self, line: String) -> Result<ControlFlow<String>> {
        if line.is_empty() {
            return Ok(ControlFlow::Continue(()));
        }

        if !line.starts_with('\\') {
            return Ok(ControlFlow::Break(line));
        }

        if line == "\\e" {
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

            return Ok(ControlFlow::Break(line));
        } else if line == "\\help" {
            self.print_help();
            return Ok(ControlFlow::Continue(()));
        }

        // all language switching is availiable only for admin console
        match self.console_type {
            ConsoleType::User => (),
            ConsoleType::Admin => {
                if line == "\\lua" {
                    return Ok(ControlFlow::Break("\\set language lua".into()));
                } else if line == "\\sql" {
                    return Ok(ControlFlow::Break("\\set language sql".into()));
                }

                let splitted = line.split_whitespace().collect::<Vec<_>>();
                if splitted.len() > 2
                    && (splitted[0] == "\\s" || splitted[0] == "\\set")
                    && (splitted[1] == "language" || splitted[1] == "l" || splitted[1] == "lang")
                    && (splitted[2] == "lua" || splitted[2] == "sql")
                {
                    return Ok(ControlFlow::Break(line));
                }
            }
        }

        self.write("Unknown special sequence");
        Ok(ControlFlow::Continue(()))
    }

    fn update_history(&mut self, line: &str) -> Result<()> {
        self.editor.add_history_entry(line)?;
        Ok(self.editor.save_history(&self.history_file_path)?)
    }

    /// Reads from stdin. Takes into account treating special symbols.
    pub fn read(&mut self) -> Result<Option<String>> {
        loop {
            let readline = self.editor.readline(Self::PROMPT);
            match readline {
                Ok(line) => {
                    let line = match self.process_line(line)? {
                        ControlFlow::Continue(_) => continue,
                        ControlFlow::Break(line) => line,
                    };

                    if let Err(e) = self.update_history(&line) {
                        println!("{}: {}", self.history_file_path.display(), e);
                    }

                    return Ok(Some(line));
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

    fn print_help(&self) {
        let switch_language_info: &'static str = "
        \\sql          Switch console language to SQL (default)
        \\lua          Switch console language to Lua (deprecated)";

        let lang_info = match self.console_type {
            ConsoleType::Admin => switch_language_info,
            ConsoleType::User => "",
        };

        let help = format!(
            "
    Available backslash commands:
        \\e            Open the editor specified by the EDITOR environment variable
        \\help         Show this screen{lang_info}

    Available hotkeys:
        Enter         Submit the request
        Alt  + Enter  Insert a newline character
        Ctrl + C      Discard current input
        Ctrl + D      Quit interactive console
        "
        );

        self.write(&help);
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
            console_type: ConsoleType::Admin,
        })
    }
}

impl Console<()> {
    pub fn new() -> Result<Self> {
        let (editor, history_file_path) = Self::editor_with_history()?;

        Ok(Console {
            editor,
            history_file_path,
            console_type: ConsoleType::User,
        })
    }
}
