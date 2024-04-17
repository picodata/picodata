use clap::Parser;
use std::{collections, path, sync};
use tokio::{
    fs,
    io::{self, AsyncBufReadExt},
    process, runtime, signal,
    sync::oneshot,
    task, time,
};

const TASKS_CAP: usize = 1024;
const TAGS_NAMES: [&str; 4] = ["severity", "initiator", "instance_id", "raft_id"];

static mut PICODATA_NAME: String = String::new();
static mut PICODATA_VERSION: String = String::new();

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
    #[error("gather info error: {0}")]
    InfoCollect(String),
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

#[derive(Clone, Debug, serde::Serialize)]
struct Log {
    tags: Vec<String>,
    #[serde(rename = "Datetime")]
    datetime: i64,
    #[serde(rename = "serviceName")]
    service_name: String,
    #[serde(rename = "serviceVersion")]
    service_version: String,
    name: String,
    params: Vec<Param>,
    #[serde(rename = "sessionID")]
    session_id: Option<String>,
    #[serde(rename = "userLogin")]
    user_login: String,
    #[serde(rename = "userName")]
    user_name: Option<String>,
    #[serde(rename = "userNode")]
    user_node: Option<String>,
}

impl TryFrom<String> for Log {
    type Error = Option<Error>;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let mut map = match serde_json::from_str::<collections::HashMap<String, String>>(&value) {
            Ok(v) => v,
            Err(e) => {
                log::debug!("Error parsing line: {e:?}.");
                return Err(None);
            }
        };

        let Some(time) = map.remove("time") else {
            return Err(None);
        };

        let Some(name) = map.remove("title") else {
            return Err(None);
        };

        let user = map.remove("user");
        let initiator = map.remove("initiator");

        let user_login = match (user, initiator) {
            (Some(v1), _) => v1,
            (None, Some(v2)) => v2,
            (None, None) => return Err(None),
        };

        let datetime = match time.parse::<chrono::DateTime<chrono::FixedOffset>>() {
            Ok(v) => v.timestamp_millis(),
            Err(e) => {
                log::debug!("Error parsing line ts: {e:?}.");
                return Err(None);
            }
        };

        let mut tags = Vec::with_capacity(TAGS_NAMES.len());

        for tag_name in TAGS_NAMES {
            if let Some(v) = map.remove(tag_name) {
                tags.push(v);
            }
        }

        let mut params = Vec::with_capacity(map.len());

        for (key, value) in map {
            params.push(Param { key, value });
        }

        Ok(Self {
            tags,
            datetime,
            service_name: unsafe { PICODATA_NAME.clone() },
            service_version: unsafe { PICODATA_VERSION.clone() },
            name,
            params,
            session_id: None,
            user_login,
            user_name: None,
            user_node: None,
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
    #[arg(short, long)]
    url: String,
    #[arg(short, long, default_value = "audit.log")]
    filename: String,
    #[arg(short, long, value_enum, default_value = "pipe")]
    r#type: Type,
    #[arg(short, long, default_value = "false")]
    debug: bool,
    #[arg(short, long, default_value = "picodata")]
    picodata: String,
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
    let client = reqwest::ClientBuilder::new().build()?;
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

        let entry = match Log::try_from(line) {
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
                match client.post(&args.url).json(&entry).send().await {
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
                        log::debug!("Error sending log, bad status: {status:?}.");
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
        let output = process::Command::new(&args.picodata)
            .arg("--version")
            .output()
            .await?;
        if output.status.success() {
            let mut info = String::from_utf8(output.stdout).unwrap();
            if let Some(v) = info.pop() {
                if v != '\n' {
                    info.push(v);
                }
            }
            log::debug!("Picodata info: {info}.");
            let mut parts = info.split(' ').collect::<Vec<&str>>();
            unsafe {
                PICODATA_VERSION = parts.pop().unwrap().to_string();
                PICODATA_NAME = parts.pop().unwrap().to_string();
            };
        } else {
            let stderr = String::from_utf8(output.stderr).unwrap();
            return Err(Error::InfoCollect(stderr));
        }
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

    use std::str::FromStr;
    use std::{convert, future, net, sync::mpsc, thread, time};
    use tokio::{runtime, sync::mpsc as tmpsc, sync::oneshot, time as ttime};

    pub struct Server {
        addr: net::SocketAddr,
        panic_rx: mpsc::Receiver<()>,
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

            if !thread::panicking() {
                self.panic_rx
                    .recv_timeout(time::Duration::from_secs(3))
                    .expect("Test server should not panic.");
            }
        }
    }

    fn server<FN, FT>(addr: &str, sender: tmpsc::Sender<()>, func: FN) -> Server
    where
        FN: Fn(http::Request<hyper::Body>) -> FT + Clone + Send + 'static,
        FT: future::Future<Output = http::Response<hyper::Body>> + Send + 'static,
    {
        let (panic_tx, panic_rx) = mpsc::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (addr_tx, addr_rx) = mpsc::channel();

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

                rt.block_on(async move {
                    let server = hyper::Server::bind(&addr).serve(hyper::service::make_service_fn(
                        move |_| {
                            let func = func.clone();
                            let sender = sender.clone();
                            async move {
                                sender.send(()).await.expect("Failed to send.");
                                Ok::<_, convert::Infallible>(hyper::service::service_fn(
                                    move |req| {
                                        let fut = func(req);
                                        async move { Ok::<_, convert::Infallible>(fut.await) }
                                    },
                                ))
                            }
                        },
                    ));
                    addr_tx
                        .send(server.local_addr())
                        .expect("Failed to send server addr.");

                    let server = server.with_graceful_shutdown(async move {
                        let _ = shutdown_rx.await;
                    });

                    server.await.expect("Failed server.");
                    let _ = panic_tx.send(());
                });
            })
            .expect("Failed to spawn thread.");

        let addr = addr_rx.recv().expect("Failed to receive server address.");

        Server {
            addr,
            panic_rx,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    #[tokio::test]
    async fn logs_sent_success() {
        _ = simple_logger::init_with_level(log::Level::Debug);
        let (s, mut r) = tmpsc::channel(256);
        let server = server("127.0.0.1:4000", s, move |_req| async move {
            http::Response::default()
        });

        let args = sync::Arc::new(Args {
            url: format!("http://{}/log", server.addr()),
            filename: String::from("audit.log"),
            r#type: Type::File,
            debug: true,
            picodata: String::from("picodata"),
        });

        let lines = reader(args.clone()).await.expect("Failed to get reader.");
        let (sender, receiver) = oneshot::channel();
        let task = {
            let args = args.clone();
            tokio::spawn(async move { send_logs(args, lines, receiver).await })
        };

        const EXPECTED_LINES: usize = 11;

        ttime::sleep(time::Duration::from_secs(3)).await;

        _ = sender.send(Ok(()));
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
            picodata: String::from("picodata"),
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
