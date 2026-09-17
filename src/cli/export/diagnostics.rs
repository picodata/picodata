/// All the information that the program provides to the user during the export process,
/// as opposed to the error that may cause the process to terminate.
/// Only when the `verbose` option is used are they displayed as they occur;
/// warnings are logged in any case so that they can be counted at the end.
/// None of this is recorded in the dump itself.
#[derive(Debug)]
pub(super) struct Diagnostics {
    messages: Vec<String>,
    verbose: bool,
}

impl Diagnostics {
    pub(super) fn new(verbose: bool) -> Self {
        Self {
            messages: Vec::new(),
            verbose,
        }
    }

    pub(super) fn add(&mut self, message: impl Into<String>) {
        let message = message.into();
        if self.verbose {
            crate::eprintln_buffered!("WARNING: {message}");
        }
        self.messages.push(message);
    }

    pub(super) fn messages(&self) -> &[String] {
        &self.messages
    }

    pub(super) fn report(&self, message: impl std::fmt::Display) {
        if self.verbose {
            crate::eprintln_buffered!("{message}");
        }
    }
}
