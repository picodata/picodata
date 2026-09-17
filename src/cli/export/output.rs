use std::io::{BufWriter, StdoutLock, Write};
use std::path::Path;

use nix::unistd::{access, AccessFlags};
use tempfile::NamedTempFile;

use crate::cli::export::{ExportError, ExportErrorKind};

pub(super) enum DumpOutput<'path> {
    Stdout(BufWriter<StdoutLock<'static>>),
    File {
        // `NamedTempFile` forwards records straight to its file and holds no
        // data itself, so this buffer is all the dump that sits in memory.
        writer: BufWriter<NamedTempFile>,
        path: &'path Path,
    },
}

impl<'path> DumpOutput<'path> {
    /// Checks the destination before the password prompt and the network round trips.
    /// The temporary file is not created here, otherwise interrupted export may leave it dangling.
    pub(super) fn check(output_file: Option<&Path>) -> Result<(), ExportError> {
        let Some(path) = output_file else {
            return Ok(());
        };
        if path.is_dir() {
            let source = std::io::Error::from(std::io::ErrorKind::IsADirectory);
            return Err(failed_at(path.display(), source));
        }
        let parent = get_parent_directory(path);
        access(parent, AccessFlags::W_OK | AccessFlags::X_OK)
            .map_err(|errno| failed_at(path.display(), errno.into()))
    }

    pub(super) fn open(output_file: Option<&'path Path>) -> Result<Self, ExportError> {
        let Some(path) = output_file else {
            return Ok(Self::Stdout(BufWriter::new(std::io::stdout().lock())));
        };

        let temporary = NamedTempFile::new_in(get_parent_directory(path))
            .map_err(|source| failed_at(path.display(), source))?;

        Ok(Self::File {
            writer: BufWriter::new(temporary),
            path,
        })
    }

    pub(super) fn finish(self) -> Result<(), ExportError> {
        match self {
            Self::Stdout(mut writer) => writer.flush().map_err(|source| failed_at(STDOUT, source)),
            Self::File { writer, path } => {
                // On an error the temporary file is dropped.
                let temporary = writer
                    .into_inner()
                    .map_err(|error| failed_at(path.display(), error.into_error()))?;
                temporary
                    .persist(path)
                    .map_err(|error| failed_at(path.display(), error.error))?;
                Ok(())
            }
        }
    }

    /// Specifies the destination of the dump whose writing was interrupted by an error.
    /// The renderer writes into a plain `Write` and cannot know where that leads.
    pub(super) fn name_destination_of(&self, error: ExportError) -> ExportError {
        let ExportError { kind, location } = error;
        let ExportErrorKind::Write(source) = kind else {
            return ExportError { kind, location };
        };
        // The original location is kept.
        let path = match self {
            Self::Stdout(_) => STDOUT.to_owned(),
            Self::File { path, .. } => path.display().to_string(),
        };
        ExportError {
            kind: ExportErrorKind::Output { path, source },
            location,
        }
    }

    fn writer(&mut self) -> &mut dyn Write {
        match self {
            Self::Stdout(writer) => writer,
            Self::File { writer, .. } => writer,
        }
    }
}

impl Write for DumpOutput<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.writer().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer().flush()
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.writer().write_all(buf)
    }
}

const STDOUT: &str = "<stdout>";

#[track_caller]
fn failed_at(destination: impl std::fmt::Display, source: std::io::Error) -> ExportError {
    ExportError::from(ExportErrorKind::Output {
        path: destination.to_string(),
        source,
    })
}

fn get_parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::assert_snapshot;
    use pretty_assertions::assert_eq;

    fn write_to(path: &Path, dump: &str) -> Result<(), ExportError> {
        let mut output = DumpOutput::open(Some(path))?;
        output.write_all(dump.as_bytes()).expect("writes");
        output.finish()
    }

    fn list_directory(directory: &Path) -> Vec<std::ffi::OsString> {
        let mut names = std::fs::read_dir(directory)
            .expect("lists")
            .map(|entry| entry.expect("an entry").file_name())
            .collect::<Vec<_>>();
        names.sort_unstable();
        names
    }

    #[test]
    fn a_dump_written_to_a_file_lands_there_whole() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join("dump.sql");

        write_to(&path, "CREATE TABLE t (a INT);\n").expect("writes");

        assert_eq!(
            std::fs::read_to_string(&path).expect("reads back"),
            "CREATE TABLE t (a INT);\n"
        );
    }

    #[test]
    fn writing_again_replaces_the_previous_dump_entirely() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join("dump.sql");

        write_to(&path, "-- long first dump, many statements\n").expect("writes");
        write_to(&path, "-- short\n").expect("writes again");

        assert_eq!(
            std::fs::read_to_string(&path).expect("reads back"),
            "-- short\n"
        );
    }

    #[test]
    fn no_stray_temporary_file_is_left_behind() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join("dump.sql");

        write_to(&path, "-- dump\n").expect("writes");

        assert_eq!(list_directory(directory.path()), ["dump.sql"]);
    }

    #[test]
    fn checking_the_destination_creates_nothing() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join("dump.sql");

        DumpOutput::check(Some(&path)).expect("the destination is writable");

        assert!(list_directory(directory.path()).is_empty());
    }

    #[test]
    fn a_directory_or_a_missing_parent_fails_the_check() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let missing_parent = directory.path().join("missing").join("dump.sql");

        for path in [directory.path(), &missing_parent] {
            let error = DumpOutput::check(Some(path)).expect_err("cannot be written");
            assert!(
                matches!(error.kind, ExportErrorKind::Output { .. }),
                "{error:?}"
            );
        }
    }

    #[test]
    fn a_dump_dropped_midway_leaves_the_previous_one_intact() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join("dump.sql");
        write_to(&path, "-- previous dump\n").expect("writes");

        let mut output = DumpOutput::open(Some(&path)).expect("opens");
        output
            .write_all(&[b'x'; 64 * 1024])
            .expect("writes a part of the dump");
        drop(output);

        assert_eq!(list_directory(directory.path()), ["dump.sql"]);
        assert_eq!(
            std::fs::read_to_string(&path).expect("reads back"),
            "-- previous dump\n"
        );
    }

    #[test]
    fn a_dump_dropped_midway_creates_no_file() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join("dump.sql");

        let mut output = DumpOutput::open(Some(&path)).expect("opens");
        output.write_all(b"-- a part\n").expect("writes");
        drop(output);

        assert!(list_directory(directory.path()).is_empty());
    }

    #[test]
    fn the_written_part_is_on_disk_not_in_memory_before_the_end() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join("dump.sql");
        let chunk = [b'x'; 64 * 1024];

        let mut output = DumpOutput::open(Some(&path)).expect("opens");
        (0..4).for_each(|_| output.write_all(&chunk).expect("writes a chunk"));

        assert!(!path.exists());
        let DumpOutput::File { writer, .. } = &output else {
            unreachable!("a file output");
        };
        let on_disk = writer
            .get_ref()
            .as_file()
            .metadata()
            .expect("stats the temporary file")
            .len();
        let written = 4 * chunk.len() as u64;
        assert!(
            written - on_disk <= writer.capacity() as u64,
            "{on_disk} of {written} bytes on disk"
        );

        output.finish().expect("finishes");
        assert_eq!(
            std::fs::metadata(&path).expect("stats the dump").len(),
            written
        );
    }

    #[test]
    fn a_write_that_failed_midway_is_reported_with_its_destination() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join("dump.sql");
        let output = DumpOutput::open(Some(&path)).expect("opens");

        let reported = output.name_destination_of(
            ExportErrorKind::Write(std::io::Error::from_raw_os_error(28)).into(),
        );

        let ExportErrorKind::Output { path: named, .. } = &reported.kind else {
            panic!("expected a destination, got {reported:?}");
        };
        assert_eq!(named, &path.display().to_string());
    }

    #[test]
    fn a_write_to_the_standard_output_names_it_too() {
        let output = DumpOutput::open(None).expect("opens");

        let reported = output.name_destination_of(
            ExportErrorKind::Write(std::io::Error::from_raw_os_error(32)).into(),
        );

        assert_snapshot!(
            crate::cli::export::describe_chain(&reported.kind),
            @"failed writing the dump to <stdout>: Broken pipe (os error 32)"
        );
    }

    #[test]
    fn an_error_that_is_not_a_write_error_returns_as_is() {
        let output = DumpOutput::open(None).expect("opens");

        let reported = output.name_destination_of(ExportErrorKind::NoUser.into());

        assert!(
            matches!(reported.kind, ExportErrorKind::NoUser),
            "{reported:?}"
        );
    }

    #[test]
    fn a_bare_file_name_is_written_into_the_working_directory() {
        assert_eq!(get_parent_directory(Path::new("dump.sql")), Path::new("."));
        assert_eq!(
            get_parent_directory(Path::new("out/dump.sql")),
            Path::new("out")
        );
        assert_eq!(
            get_parent_directory(Path::new("/tmp/dump.sql")),
            Path::new("/tmp")
        );
    }

    #[test]
    fn a_directory_that_does_not_exist_is_reported_with_its_path() {
        let path = Path::new("/nonexistent-directory-for-a-test/dump.sql");

        let error = write_to(path, "-- dump\n").expect_err("cannot be written");
        assert!(
            error
                .to_string()
                .contains("/nonexistent-directory-for-a-test/dump.sql"),
            "{error}"
        );
    }
}
