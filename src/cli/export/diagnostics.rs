/// All the information that the program provides to the user during the export process,
/// as opposed to the error that may cause the process to terminate.
/// The progress is displayed only when the `verbose` option is used,
/// the warnings are always displayed.
#[derive(Debug, Clone, Copy)]
pub(super) struct Diagnostics {
    verbose: bool,
}

impl Diagnostics {
    pub(super) fn new(verbose: bool) -> Self {
        Self { verbose }
    }

    pub(super) fn report(&self, message: impl std::fmt::Display) {
        if self.verbose {
            crate::eprintln_buffered!("{message}");
        }
    }

    pub(super) fn warn(&self, message: impl std::fmt::Display) {
        crate::eprintln_buffered!("WARNING: {message}");
    }
}
