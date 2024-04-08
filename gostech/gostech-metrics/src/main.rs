use actix_web::web;
use clap::Parser;
use rusty_tarantool::tarantool;
use std::sync as s_sync;
use tokio::{sync, sync::oneshot, time};

type Picodata = s_sync::Arc<sync::Mutex<tarantool::Client>>;

#[derive(Debug, Parser, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "localhost")]
    host: String,
    #[arg(long, default_value = "3301")]
    port: String,
    #[arg(long, default_value = "pico_service")]
    username: String,
    #[arg(long, default_value = "")]
    password: String,
    #[arg(long, default_value = "127.0.0.1:4401")]
    addr: String,
}

fn parse_response(data: &bytes::Bytes) -> Vec<u8> {
    let mut start = 0;
    let mut end = data.len();

    for (idx, &byte) in data.iter().enumerate() {
        start = idx;
        if byte == b'#' {
            break;
        }
    }
    for &byte in data.iter().rev() {
        end -= 1;
        if byte == b'\n' {
            break;
        }
    }

    data.slice(start..end).to_vec()
}

async fn collect(picodata: web::Data<Picodata>) -> impl actix_web::Responder {
    let Ok(response) = picodata
        .lock()
        .await
        .eval(
            String::from("return require('metrics.plugins.prometheus').collect_http(...)"),
            &(0,),
        )
        .await
    else {
        return actix_web::HttpResponse::InternalServerError()
            .content_type("text/plain")
            .body(String::from("failed to collect metrics"));
    };
    let Ok(s) = String::from_utf8(parse_response(&response.data)) else {
        return actix_web::HttpResponse::InternalServerError()
            .content_type("text/plain")
            .body(String::from("failed to parse metrics response"));
    };
    actix_web::HttpResponse::Ok()
        .content_type("text/plain")
        .body(s)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    let picodata = s_sync::Arc::new(sync::Mutex::new(
        tarantool::ClientConfig::new(
            format!("{}:{}", args.host, args.port),
            &args.username,
            &args.password,
        )
        .set_timeout_time_ms(2000)
        .set_reconnect_time_ms(2000)
        .build(),
    ));
    picodata
        .lock()
        .await
        .ping()
        .await
        .expect("Failed to ping picodata instance.");
    let (sender, mut receiver) = oneshot::channel();
    let updater = {
        let args = args.clone();
        let picodata = picodata.clone();
        tokio::spawn(async move {
            loop {
                let mut guard = picodata.lock().await;
                let ping = guard.ping().await;
                if ping.is_err() {
                    *guard = tarantool::ClientConfig::new(
                        format!("{}:{}", args.host, args.port),
                        &args.username,
                        &args.password,
                    )
                    .set_timeout_time_ms(2000)
                    .set_reconnect_time_ms(2000)
                    .build();
                }
                drop(guard);
                match receiver.try_recv() {
                    Ok(()) => break,
                    Err(e) => match e {
                        oneshot::error::TryRecvError::Closed => break,
                        oneshot::error::TryRecvError::Empty => {
                            time::sleep(time::Duration::from_secs(5)).await;
                            continue;
                        }
                    },
                }
            }
        })
    };
    let res = actix_web::HttpServer::new(move || {
        actix_web::App::new()
            .app_data(web::Data::new(picodata.clone()))
            .route("/metrics", web::get().to(collect))
    })
    .bind(&args.addr)?
    .workers(1)
    .run()
    .await;
    _ = sender.send(());
    _ = updater.await;
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_input() {
        let data = bytes::Bytes::from_static(b"some data#interesting part\n");
        assert_eq!(parse_response(&data), b"#interesting part".to_vec());
    }

    #[test]
    fn no_start_marker() {
        let data = bytes::Bytes::from_static(b"no start marker here\n");
        assert_eq!(parse_response(&data), b"".to_vec());
    }

    #[test]
    fn no_trailing_newlines() {
        let data = bytes::Bytes::from_static(b"#data without newlines");
        assert_eq!(parse_response(&data), b"".to_vec());
    }

    #[test]
    fn start_marker_and_newlines() {
        let data = bytes::Bytes::from_static(b"#\n\n");
        assert_eq!(parse_response(&data), b"#\n".to_vec());
    }

    #[test]
    fn empty_input() {
        let data = bytes::Bytes::new();
        assert_eq!(parse_response(&data), b"".to_vec());
    }

    #[test]
    fn only_newlines() {
        let data = bytes::Bytes::from_static(b"\n\n\n");
        assert_eq!(parse_response(&data), b"".to_vec());
    }

    #[test]
    fn multiple_start_markers() {
        let data = bytes::Bytes::from_static(b"##this#is#it\n");
        assert_eq!(parse_response(&data), b"##this#is#it".to_vec());
    }
}
