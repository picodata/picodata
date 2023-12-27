use std::env;
use std::fs::read_to_string;
use std::io;
use std::ops::ControlFlow;
use std::path::Path;
use std::path::PathBuf;
use std::process;

use rustyline::{error::ReadlineError, history::FileHistory, DefaultEditor, Editor};
use tarantool::network::client;

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

/// Input/output handler
pub struct Console {
    editor: Editor<(), FileHistory>,
    history_file_path: PathBuf,
    prompt: String,
}

impl Console {
    const HISTORY_FILE_NAME: &str = ".picodata_history";

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

            let line = read_to_string(temp.path()).map_err(ReplError::Io)?;

            return Ok(ControlFlow::Break(line));
        } else if line == "\\lua" {
            return Ok(ControlFlow::Break("\\set language lua".to_owned()));
        } else if line == "\\sql" {
            return Ok(ControlFlow::Break("\\set language sql".to_owned()));
        }

        self.write("Unknown special sequence");
        Ok(ControlFlow::Continue(()))
    }

    pub fn new(prompt: &str) -> Result<Self> {
        let mut editor = DefaultEditor::new()?;

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

        editor.load_history(&history_file_path)?;

        Ok(Console {
            editor,
            history_file_path,
            prompt: prompt.to_string(),
        })
    }

    /// Reads from stdin. Takes into account treating special symbols.
    pub fn read(&mut self) -> Result<Option<String>> {
        loop {
            let readline = self.editor.readline(&self.prompt);
            match readline {
                Ok(line) => {
                    let line = match self.process_line(line)? {
                        ControlFlow::Continue(_) => continue,
                        ControlFlow::Break(line) => line,
                    };

                    self.editor.add_history_entry(line.as_str())?;
                    self.editor.save_history(&self.history_file_path)?;

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
}
