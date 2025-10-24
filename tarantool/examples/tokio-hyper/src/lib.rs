use std::sync::Arc;
use std::time::Duration;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;

use tarantool::fiber;
use tarantool::index::IteratorType;
use tarantool::space::Space;
use tarantool::tuple;

#[derive(Debug, Clone, ::serde::Deserialize, ::serde::Serialize)]
struct Fruit {
    id: usize,
    name: String,
    weight: f64,
}
impl tuple::Encode for Fruit {}

#[derive(Debug)]
enum Cmd {
    ListAll,
    AddAPieceOfFruit(Fruit),
}

async fn handle_req(
    req: Request<Body>,
    cmd_tx: UnboundedSender<Cmd>,
    fruit_rx: Arc<Mutex<UnboundedReceiver<Vec<Fruit>>>>,
) -> Result<Response<Body>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        // Try doing: `curl '127.0.0.1:3000/list-fruit'`
        (&Method::GET, "/list-fruit") => {
            cmd_tx.send(Cmd::ListAll).unwrap();
            let fruit = fruit_rx.lock().await.recv().await.unwrap();

            let mut msg = String::new();
            for single_fruit in fruit {
                msg.push_str(&format!("{:?}\n", single_fruit));
            }

            Ok(Response::new(Body::from(msg)))
        }

        // Try doing:
        // `curl '127.0.0.1:3000/add-fruit' -XPOST -d '{ "id": 1, "name": "apple", "weight": 13.37 }'`
        (&Method::POST, "/add-fruit") => {
            let body = hyper::body::to_bytes(req.into_body()).await?;
            match serde_json::from_slice(body.as_ref()) {
                Ok(single_fruit @ Fruit { .. }) => {
                    cmd_tx.send(Cmd::AddAPieceOfFruit(single_fruit)).unwrap();
                    Ok(Response::new(Body::from("ok")))
                }
                Err(e) => Ok(Response::new(Body::from(format!("{}", e)))),
            }
        }

        // Return the 404 Not Found for other routes.
        _ => {
            let mut not_found = Response::default();
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

#[tarantool::proc]
fn start_server() {
    let (cmd_tx, cmd_rx) = unbounded_channel();
    let (fruit_tx, fruit_rx) = unbounded_channel::<Vec<Fruit>>();
    let fruit_rx = Arc::new(Mutex::new(fruit_rx));

    let jh = std::thread::spawn(move || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed building the Runtime")
            .block_on(async move {
                let service = make_service_fn(move |_| {
                    let cmd_tx = cmd_tx.clone();
                    let fruit_rx = fruit_rx.clone();
                    async move {
                        Ok::<_, hyper::Error>(service_fn(move |req| {
                            handle_req(req, cmd_tx.clone(), fruit_rx.clone())
                        }))
                    }
                });

                let addr = ([127, 0, 0, 1], 3000).into();
                let server = Server::bind(&addr).serve(service);

                println!("Listening on http://{}", addr);

                server.await?;
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            })
    });

    // Detach the thread
    drop(jh);

    static mut FIBER_JOIN_HANDLE: Option<fiber::JoinHandle<()>> = None;
    let jh = fiber::start(move || {
        let mut cmd_rx = cmd_rx;
        let space_fruit = Space::find("fruit").unwrap();
        loop {
            match cmd_rx.try_recv() {
                Err(TryRecvError::Empty) => {
                    fiber::sleep(Duration::from_millis(100));
                    continue;
                }
                Err(TryRecvError::Disconnected) => break,
                Ok(Cmd::ListAll) => {
                    let fruit = space_fruit
                        .select(IteratorType::All, &())
                        .unwrap()
                        .map(|t| t.decode::<Fruit>().unwrap())
                        .collect();
                    fruit_tx.send(fruit).unwrap();
                }
                Ok(Cmd::AddAPieceOfFruit(single_fruit)) => {
                    space_fruit.replace(&single_fruit).unwrap();
                }
            }
        }
    });
    // There's currently no way of detaching a fiber without leaking memory,
    // so we have to store it's join handle somewhere.
    unsafe {
        FIBER_JOIN_HANDLE = Some(jh);
    }
}
