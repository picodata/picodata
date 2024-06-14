// Copyright 2024 Picodata - All Rights Reserved
//
// This source code is protected under international copyright law. All rights
// reserved and protected by the copyright holders.
// This file is confidential and only available to authorized individuals with the
// permission of the copyright holders. If you encounter this file and do not have
// permission, please contact the copyright holders and delete this file.
//

use clap::Parser;
use std::{collections, path, sync};
use tokio::{
    fs,
    io::{self, AsyncBufReadExt},
    runtime, signal,
    sync::oneshot,
    task, time,
};

const TASKS_CAP: usize = 1024;
const USER_NODE: &str = "default";
const X_NODE_ID: &str = "0.0.0";
const MODULE: &str = "picodata";
const METAMODEL_VERSION: &str = "0.1.0";
const EVENT_TYPES: [&str; 34] = [
    "access_denied",
    "auth_fail",
    "auth_ok",
    "change_config",
    "change_current_grade",
    "change_password",
    "change_target_grade",
    "connect_local_db",
    "create_local_db",
    "create_procedure",
    "create_role",
    "create_table",
    "create_user",
    "drop_local_db",
    "drop_procedure",
    "drop_role",
    "drop_table",
    "drop_user",
    "expel_instance",
    "grant_privilege",
    "grant_role",
    "init_audit",
    "integrity_violation",
    "join_instance",
    "local_shutdown",
    "local_startup",
    "recover_local_db",
    "rename_procedure",
    "rename_user",
    "revoke_privilege",
    "revoke_role",
    "shredding_failed",
    "shredding_finished",
    "shredding_started",
];

enum LineReader {
    File(io::Lines<io::BufReader<fs::File>>),
    Pipe(io::Lines<io::BufReader<io::Stdin>>),
}

impl LineReader {
    pub async fn next_line(&mut self) -> io::Result<Option<String>> {
        match self {
            Self::File(f) => f.next_line().await,
            Self::Pipe(p) => p.next_line().await,
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("send log bad status")]
    SendLogStatus,
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("join error: {0}")]
    Join(#[from] task::JoinError),
    #[error("logger error: {0}")]
    Logger(#[from] log::SetLoggerError),
}

#[derive(Clone, Debug, serde::Serialize)]
struct Param {
    #[serde(rename = "name")]
    key: String,
    value: String,
}

fn map_to_params(map: collections::HashMap<String, String>) -> Vec<Param> {
    let mut result = Vec::with_capacity(map.len());
    for (key, value) in map {
        result.push(Param { key, value });
    }
    result
}

#[derive(Clone, Debug, serde::Serialize)]
struct Log {
    tags: [String; 1],
    #[serde(rename = "createdAt")]
    datetime: i64,
    module: String,
    #[serde(rename = "metamodelVersion")]
    metamodel_version: String,
    name: String,
    params: Vec<Param>,
    #[serde(rename = "session")]
    session_id: Option<String>,
    #[serde(rename = "userLogin")]
    user_login: String,
    #[serde(rename = "userName")]
    user_name: Option<String>,
    #[serde(rename = "userNode")]
    user_node: Option<String>,
}

impl TryFrom<collections::HashMap<String, String>> for Log {
    type Error = Option<Error>;

    fn try_from(value: collections::HashMap<String, String>) -> Result<Self, Self::Error> {
        let mut map = value;

        let time = map
            .get("time")
            .expect("getting time should not fail")
            .clone();
        let name = map
            .get("title")
            .expect("getting title should not fail")
            .clone();
        let severity = map
            .get("severity")
            .expect("getting severity should not fail")
            .clone();

        if map.get("initiator").is_none() {
            map.insert(String::from("initiator"), String::from("unset"));
        }

        let user_login = match (map.get("user"), map.get("initiator")) {
            (Some(v1), _) => v1.as_str(),
            (None, Some(v2)) => v2.as_str(),
            (None, None) => "unset",
        }
        .to_string();

        let datetime = time
            .parse::<chrono::DateTime<chrono::FixedOffset>>()
            .expect("invalid time format")
            .timestamp_millis();

        if !EVENT_TYPES.contains(&name.as_str()) {
            log::error!("unknown log type line: {}", &name);
            return Err(None);
        }

        let params = map_to_params(map);

        Ok(Self {
            tags: [severity],
            datetime,
            module: MODULE.to_string(),
            metamodel_version: METAMODEL_VERSION.to_string(),
            name,
            params,
            session_id: None,
            user_login,
            user_name: None,
            user_node: Some(String::from(USER_NODE)),
        })
    }
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum Type {
    Pipe,
    File,
}

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    url: String,
    #[arg(long, default_value = "audit.log")]
    filename: String,
    #[arg(long, value_enum, default_value = "pipe")]
    r#type: Type,
    #[arg(long, default_value = "false")]
    debug: bool,
    #[arg(long, default_value = "")]
    certificate: String,
    #[arg(long, default_value = "")]
    private_key: String,
    #[arg(long, default_value = "")]
    ca_certificate: String,
}

async fn reader(args: sync::Arc<Args>) -> Result<LineReader, Error> {
    Ok(match args.r#type {
        Type::File => {
            let file = fs::File::open(path::Path::new(&args.filename)).await?;
            let reader = io::BufReader::new(file);
            LineReader::File(reader.lines())
        }
        Type::Pipe => {
            let stdin = io::stdin();
            let reader = io::BufReader::new(stdin);
            LineReader::Pipe(reader.lines())
        }
    })
}

async fn send_logs(
    args: sync::Arc<Args>,
    mut lines: LineReader,
    mut receiver: oneshot::Receiver<Result<(), Error>>,
) -> Result<(), Error> {
    let mut builder = reqwest::ClientBuilder::new();
    if !args.certificate.is_empty() && !args.private_key.is_empty() {
        let cert = fs::read(&args.certificate).await?;
        let key = fs::read(&args.private_key).await?;
        let id = reqwest::Identity::from_pkcs8_pem(&cert, &key)?;
        builder = builder.identity(id).use_native_tls();

        #[cfg(test)]
        {
            builder = builder.danger_accept_invalid_certs(true);
        };
    }

    if !args.ca_certificate.is_empty() {
        let ca: Vec<u8> = fs::read(&args.ca_certificate).await?;
        builder = builder
            .add_root_certificate(reqwest::tls::Certificate::from_pem(&ca)?)
            .use_native_tls();
    }

    let client = builder.build()?;
    let mut tasks = Vec::with_capacity(TASKS_CAP);

    loop {
        if tasks.len() == TASKS_CAP {
            while let Some(task) = tasks.pop() {
                task.await??;
            }
        }
        if let Ok(v) = receiver.try_recv() {
            v?;
            break;
        }

        let Some(line) = lines.next_line().await? else {
            break;
        };

        log::debug!("Handle row: {line}.");

        let map = match serde_json::from_str::<collections::HashMap<String, String>>(&line) {
            Ok(v) => v,
            Err(e) => {
                log::debug!("Error parsing line: {e:?}.");
                continue;
            }
        };

        let x_node_id = map
            .get("raft_id")
            .map(String::as_str)
            .unwrap_or(X_NODE_ID)
            .split('.')
            .next()
            .expect("getting x node id should not fail")
            .to_string();

        let entry = match Log::try_from(map) {
            Ok(v) => v,
            Err(e) => match e {
                Some(ee) => return Err(ee),
                None => continue,
            },
        };

        let args = args.clone();
        let client = client.clone();

        tasks.push(tokio::spawn(async move {
            log::debug!("Send: {entry:?}.");
            let mut tries = 1;

            loop {
                match client
                    .post(&args.url)
                    .header("X-Node-ID", &x_node_id)
                    .json(&entry)
                    .send()
                    .await
                {
                    Ok(v) => {
                        let statuses: [reqwest::StatusCode; 4] = [
                            reqwest::StatusCode::OK,
                            reqwest::StatusCode::CREATED,
                            reqwest::StatusCode::ACCEPTED,
                            reqwest::StatusCode::NO_CONTENT,
                        ];
                        let status = v.status();
                        if statuses.contains(&status) {
                            return Ok(v);
                        }
                        let text = v.text().await;
                        log::debug!("Error sending log, bad status: {status:?}, body: {text:?}.");
                        if tries < 10 {
                            tries += 1;
                            time::sleep(time::Duration::from_millis(100)).await;
                            continue;
                        }
                        return Err(Error::SendLogStatus);
                    }
                    Err(e) => {
                        if tries < 10 {
                            tries += 1;
                            time::sleep(time::Duration::from_millis(100)).await;
                            continue;
                        }
                        log::debug!("Error sending log: {e:?}.");
                        return Err(Error::Http(e));
                    }
                }
            }
        }));
    }

    while let Some(task) = tasks.pop() {
        task.await??;
    }

    Ok(())
}

fn main() -> Result<(), Error> {
    let args = sync::Arc::new(Args::parse());
    if args.debug {
        simple_logger::init_with_level(log::Level::Debug)?;
    }

    log::debug!("Run audit log sender.");

    let rt = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to build rt.");

    let res: Result<(), Error> = rt.block_on(async move {
        let lines = reader(args.clone()).await?;
        let (sender, receiver) = oneshot::channel();
        let signal = tokio::spawn(async move {
            match signal::ctrl_c().await {
                Ok(()) => _ = sender.send(Ok(())),
                Err(e) => _ = sender.send(Err(Error::Io(e))),
            };
        });
        send_logs(args.clone(), lines, receiver).await?;
        signal.abort();
        Ok(())
    });

    rt.shutdown_timeout(time::Duration::from_secs(120));

    log::debug!("Stop audit log sender.");
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    use actix_web::web;
    use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
    use std::str::FromStr;
    use std::{net, thread, time};
    use tokio::{runtime, sync::mpsc as tmpsc, sync::oneshot, time as ttime};

    pub struct Server {
        addr: net::SocketAddr,
        shutdown_tx: Option<oneshot::Sender<()>>,
    }

    impl Server {
        pub fn addr(&self) -> net::SocketAddr {
            self.addr
        }
    }

    impl Drop for Server {
        fn drop(&mut self) {
            if let Some(tx) = self.shutdown_tx.take() {
                let _ = tx.send(());
            }
        }
    }

    fn server(
        addr: &str,
        sender: tmpsc::Sender<()>,
        key_file: String,
        cert_file: String,
    ) -> Server {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let thread_name = format!(
            "test({})-support-server",
            thread::current().name().unwrap_or("<unknown>")
        );

        let addr = net::SocketAddr::from_str(addr).expect("Failed to create socket addr.");

        thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let rt = runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create tokio runtime.");

                async fn handler(s: web::Data<tmpsc::Sender<()>>) -> impl actix_web::Responder {
                    s.send(()).await.expect("Failed to send.");
                    ""
                }

                rt.block_on(async move {
                    let builder = actix_web::HttpServer::new(move || {
                        actix_web::App::new()
                            .app_data(web::Data::new(sender.clone()))
                            .route("/log", web::post().to(handler))
                    });
                    let server = if !key_file.is_empty() && !cert_file.is_empty() {
                        let mut ssl_builder =
                            SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
                        ssl_builder
                            .set_private_key_file(key_file, SslFiletype::PEM)
                            .unwrap();
                        ssl_builder.set_certificate_chain_file(cert_file).unwrap();
                        builder
                            .bind_openssl(addr, ssl_builder)
                            .expect("Failed to bind.")
                            .workers(1)
                            .run()
                    } else {
                        builder
                            .bind(addr)
                            .expect("Failed to bind.")
                            .workers(1)
                            .run()
                    };

                    let handle = server.handle();
                    let task = tokio::spawn(async move { server.await.expect("Failed to run") });
                    let _ = shutdown_rx.await;
                    handle.stop(true).await;
                    let _ = task.await;
                })
            })
            .expect("Failed to spawn thread.");

        std::thread::sleep(std::time::Duration::from_secs(5));
        Server {
            addr,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    #[tokio::test]
    async fn logs_sent_success() {
        _ = simple_logger::init_with_level(log::Level::Debug);
        let (s, mut r) = tmpsc::channel(256);

        let server = server("127.0.0.1:4005", s, String::new(), String::new());

        let args = sync::Arc::new(Args {
            url: format!("http://{}/log", server.addr()),
            filename: String::from("audit.log"),
            r#type: Type::File,
            debug: true,
            certificate: String::from(""),
            private_key: String::from(""),
            ca_certificate: String::from(""),
        });

        let lines = reader(args.clone()).await.expect("Failed to get reader.");
        let (sender, receiver) = oneshot::channel();

        let task = {
            let args = args.clone();
            tokio::spawn(async move { send_logs(args, lines, receiver).await })
        };

        const EXPECTED_LINES: usize = 13;

        ttime::sleep(time::Duration::from_secs(3)).await;

        let _ = sender.send(Ok(()));

        task.await
            .expect("Failed to join task outer.")
            .expect("Failed to join task inner.");

        drop(server);

        let mut counter = 0;

        loop {
            match r.try_recv() {
                Ok(_) => counter += 1,
                Err(e) => match e {
                    tmpsc::error::TryRecvError::Disconnected => break,
                    tmpsc::error::TryRecvError::Empty => continue,
                },
            };
        }

        assert_eq!(counter, EXPECTED_LINES);
    }

    #[tokio::test]
    async fn logs_sent_success_ssl() {
        _ = simple_logger::init_with_level(log::Level::Debug);
        let (s, mut r) = tmpsc::channel(256);

        let server = server(
            "127.0.0.1:4006",
            s,
            String::from("key.pem"),
            String::from("cert.pem"),
        );

        let args = sync::Arc::new(Args {
            url: format!("https://{}/log", server.addr()),
            filename: String::from("audit.log"),
            r#type: Type::File,
            debug: true,
            certificate: String::from("client.cert.pem"),
            private_key: String::from("client.key.pem"),
            ca_certificate: String::from("cert.pem"),
        });

        let lines = reader(args.clone()).await.expect("Failed to get reader.");
        let (sender, receiver) = oneshot::channel();

        let task = {
            let args = args.clone();
            tokio::spawn(async move { send_logs(args, lines, receiver).await })
        };

        const EXPECTED_LINES: usize = 13;

        ttime::sleep(time::Duration::from_secs(3)).await;

        let _ = sender.send(Ok(()));

        task.await
            .expect("Failed to join task outer.")
            .expect("Failed to join task inner.");

        drop(server);

        let mut counter = 0;

        loop {
            match r.try_recv() {
                Ok(_) => counter += 1,
                Err(e) => match e {
                    tmpsc::error::TryRecvError::Disconnected => break,
                    tmpsc::error::TryRecvError::Empty => continue,
                },
            };
        }

        assert_eq!(counter, EXPECTED_LINES);
    }

    #[tokio::test]
    async fn logs_server_unavailable() {
        _ = simple_logger::init_with_level(log::Level::Debug);

        let args = sync::Arc::new(Args {
            url: String::from("http://localhost:0192/log"),
            filename: String::from("audit.log"),
            r#type: Type::File,
            debug: true,
            certificate: String::from(""),
            private_key: String::from(""),
            ca_certificate: String::from(""),
        });

        let lines = reader(args.clone()).await.expect("Failed to get reader.");
        let (_, receiver) = oneshot::channel();
        let task = {
            let args = args.clone();
            tokio::spawn(send_logs(args, lines, receiver))
        };

        ttime::sleep(time::Duration::from_secs(3)).await;

        let res = task.await;

        log::debug!("{:?}", res);

        assert!(res.expect("Failed to join task outer.").is_err());
    }
}
