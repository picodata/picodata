use std::path::PathBuf;

use tokio_postgres as pg;

use crate::address::{DEFAULT_LISTEN_HOST, DEFAULT_PGPROTO_PORT};
use crate::cli::args;
use crate::cli::export::output::DumpOutput;
use crate::cli::export::{ExportError, ExportErrorKind};

/// `Debug` doesn't leak the password: `tokio_postgres::Config` prints it as `_`.
#[derive(Debug)]
pub(super) struct ExportOptions {
    pub(super) pg_config: pg::Config,
    /// `None` means the standard output.
    pub(super) output_file: Option<PathBuf>,
}

pub(super) fn build(arguments: args::Export) -> Result<ExportOptions, ExportError> {
    let mut pg_config = match arguments.dsn.as_deref() {
        Some(dsn) => dsn.parse::<pg::Config>().map_err(ExportErrorKind::BadDsn)?,
        None => pg::Config::new(),
    };

    let tls_parameter = match (pg_config.get_ssl_mode(), pg_config.get_ssl_negotiation()) {
        (pg::config::SslMode::Require, _) => Some("sslmode=require"),
        (_, pg::config::SslNegotiation::Direct) => Some("sslnegotiation=direct"),
        _ => None,
    };
    if let Some(parameter) = tls_parameter {
        return Err(ExportError::from(ExportErrorKind::TlsRequested {
            parameter,
        }));
    }

    // Flags only fill in what the DSN left empty: the DSN wins, as in `pg_dump`.
    if pg_config.get_hosts().is_empty() && pg_config.get_hostaddrs().is_empty() {
        let host = arguments.host.as_deref().unwrap_or(DEFAULT_LISTEN_HOST);
        pg_config.host(host);
    }
    if pg_config.get_ports().is_empty() {
        let port = arguments.port.unwrap_or_else(|| {
            DEFAULT_PGPROTO_PORT
                .parse()
                .expect("the default pgproto port is a valid u16")
        });
        pg_config.port(port);
    }
    if pg_config.get_user().is_none() {
        // Unlike libpq, no fallback to the OS user name.
        let username = arguments
            .username
            .as_deref()
            .ok_or(ExportErrorKind::NoUser)?;
        pg_config.user(username);
    }

    // Checked before the password prompt and the network round trips,
    // so a bad `-f` path is reported first.
    DumpOutput::check(arguments.file.as_deref())?;

    // The password comes from the connection string or else it is asked as `psql` and `pg_dump` do.
    if pg_config.get_password().is_none() {
        let prompt = format!(
            "Enter password for {}: ",
            pg_config.get_user().unwrap_or_default()
        );
        let password = crate::cli::util::prompt_password(&prompt).map_err(|source| {
            ExportError::other("failed reading the password from a terminal", source)
        })?;
        pg_config.password(password);
    }

    Ok(ExportOptions {
        pg_config,
        output_file: arguments.file,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser as _;

    const WITH_PASSWORD: &str = "password=secret";

    fn make_arguments() -> args::Export {
        args::Export::parse_from(["export", "--schema-only"])
    }

    fn build_config(arguments: args::Export) -> pg::Config {
        let options = build(arguments).expect("the arguments are valid");
        options.pg_config
    }

    fn collect_hosts(config: &pg::Config) -> Vec<String> {
        config
            .get_hosts()
            .iter()
            .map(|host| match host {
                pg::config::Host::Tcp(name) => name.clone(),
                #[cfg(unix)]
                pg::config::Host::Unix(path) => path.display().to_string(),
            })
            .collect()
    }

    #[test]
    fn flags_fill_in_what_the_dsn_left_empty() {
        let mut arguments = make_arguments();
        arguments.dsn = Some(WITH_PASSWORD.into());
        arguments.host = Some("10.0.0.1".into());
        arguments.port = Some(5555);
        arguments.username = Some("alice".into());

        let config = build_config(arguments);

        assert_eq!(collect_hosts(&config), vec!["10.0.0.1".to_owned()]);
        assert_eq!(config.get_ports(), [5555]);
        assert_eq!(config.get_user(), Some("alice"));
    }

    #[test]
    fn dsn_takes_precedence_over_the_flags() {
        let mut arguments = make_arguments();
        arguments.dsn = Some("host=dsn-host port=1111 user=dsn-user password=secret".into());
        arguments.host = Some("flag-host".into());
        arguments.port = Some(2222);
        arguments.username = Some("flag-user".into());

        let config = build_config(arguments);

        assert_eq!(collect_hosts(&config), vec!["dsn-host".to_owned()]);
        assert_eq!(config.get_ports(), [1111]);
        assert_eq!(config.get_user(), Some("dsn-user"));
    }

    #[test]
    fn a_multi_host_dsn_is_not_overridden_by_a_single_host_flag() {
        let mut arguments = make_arguments();
        arguments.dsn = Some("host=a,b port=1111,2222 user=dsn-user password=secret".into());
        arguments.host = Some("flag-host".into());
        arguments.port = Some(3333);

        let config = build_config(arguments);

        assert_eq!(collect_hosts(&config), vec!["a".to_owned(), "b".to_owned()]);
        assert_eq!(config.get_ports(), [1111, 2222]);
    }

    #[test]
    fn defaults_are_used_when_neither_dsn_nor_flags_say_otherwise() {
        let mut arguments = make_arguments();
        arguments.dsn = Some(WITH_PASSWORD.into());
        arguments.username = Some("admin".into());

        let config = build_config(arguments);

        assert_eq!(collect_hosts(&config), vec![DEFAULT_LISTEN_HOST.to_owned()]);
        assert_eq!(
            config.get_ports(),
            [DEFAULT_PGPROTO_PORT.parse::<u16>().unwrap()]
        );
    }

    #[test]
    fn the_uri_and_the_key_value_forms_agree() {
        let mut from_uri = make_arguments();
        from_uri.dsn = Some("postgres://admin:secret@example.org:4327".into());
        let mut from_pairs = make_arguments();
        from_pairs.dsn = Some("host=example.org port=4327 user=admin password=secret".into());

        assert_eq!(build_config(from_uri), build_config(from_pairs));
    }

    #[test]
    fn a_missing_user_is_an_error_rather_than_the_os_user() {
        let error = build(make_arguments()).expect_err("no user was given");
        assert!(matches!(error.kind, ExportErrorKind::NoUser), "{error:?}");
    }

    #[test]
    fn a_malformed_dsn_is_reported_as_such() {
        let mut arguments = make_arguments();
        arguments.dsn = Some("host=localhost port=not-a-number".into());

        let error = build(arguments).expect_err("the port is not a number");
        assert!(
            matches!(error.kind, ExportErrorKind::BadDsn(_)),
            "{error:?}"
        );
        let reported = crate::cli::export::describe_chain(&error);
        assert!(reported.contains("option `port`"), "{reported}");
    }

    #[test]
    fn an_unsupported_dsn_parameter_is_named_by_the_driver() {
        for dsn in [
            "host=localhost user=admin sslrootcert=/tmp/ca.crt",
            "postgres://admin@localhost/?sslrootcert=/tmp/ca.crt",
        ] {
            let mut arguments = make_arguments();
            arguments.dsn = Some(dsn.into());

            let error = build(arguments).expect_err("sslrootcert is not supported");
            let reported = crate::cli::export::describe_chain(&error);
            assert!(reported.contains("sslrootcert"), "{reported}");
        }
    }

    /// `tokio_postgres::config::SslMode` has only `Disable`, `Prefer` or
    /// `Require`, so its parser rejects the verifying modes of libpq.
    #[test]
    fn a_verifying_sslmode_is_reported_with_its_value() {
        let mut arguments = make_arguments();
        arguments.dsn = Some("host=localhost user=admin sslmode=verify-full".into());

        let error = build(arguments).expect_err("verify-full is not supported");
        let reported = crate::cli::export::describe_chain(&error);
        assert!(reported.contains("sslmode"), "{reported}");
    }

    #[test]
    fn an_sslmode_that_tolerates_plaintext_is_left_alone() {
        for mode in ["disable", "prefer"] {
            let mut arguments = make_arguments();
            arguments.dsn = Some(format!("user=admin password=secret sslmode={mode}"));

            build(arguments).unwrap_or_else(|error| panic!("`{mode}` allows plaintext: {error}"));
        }
    }

    #[test]
    fn asking_for_tls_is_refused_before_connecting() {
        for (dsn, parameter) in [
            (
                "user=admin password=secret sslmode=require",
                "sslmode=require",
            ),
            (
                "user=admin password=secret sslmode=prefer sslnegotiation=direct",
                "sslnegotiation=direct",
            ),
            (
                "user=admin password=secret sslnegotiation=direct",
                "sslnegotiation=direct",
            ),
        ] {
            let mut arguments = make_arguments();
            arguments.dsn = Some(dsn.into());

            let error = build(arguments).expect_err("TLS is not supported yet");
            assert!(
                matches!(error.kind, ExportErrorKind::TlsRequested { parameter: named } if named == parameter),
                "{error:?}"
            );
        }
    }

    #[test]
    fn a_password_in_the_dsn_is_taken_as_it_is() {
        let mut arguments = make_arguments();
        arguments.dsn = Some("user=admin password=from-the-dsn".into());

        let config = build_config(arguments);

        assert_eq!(config.get_password(), Some(&b"from-the-dsn"[..]));
    }
}
