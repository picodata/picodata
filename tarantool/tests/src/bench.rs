use tester::bench::black_box;
use tester::{
    Bencher, ShouldPanic, TDynBenchFn, TestDesc, TestDescAndFn, TestFn, TestName, TestType,
};

use tarantool::fiber;
use tarantool::net_box::{Conn, ConnOptions, Options};
use tarantool::network::client::Client;
use tarantool::network::protocol::Config as ProtocolConfig;

pub fn collect() -> Vec<TestDescAndFn> {
    fn bench<B: TDynBenchFn + 'static>(name: &'static str, bench: B) -> TestDescAndFn {
        TestDescAndFn {
            desc: TestDesc {
                name: TestName::StaticTestName(name),
                ignore: false,
                should_panic: ShouldPanic::No,
                allow_fail: false,
                test_type: TestType::Unknown,
            },
            testfn: TestFn::DynBenchFn(Box::new(bench)),
        }
    }

    vec![
        bench("call_netbox", CallNetbox),
        bench("call_network_client", CallNetworkClient),
        bench("start_async_runtime", StartAsyncRuntime),
        bench("ping_coio_stream", PingCoIOStream),
        bench("ping_client_tcp_stream", PingClientTcpStream),
    ]
}

pub struct CallNetbox;

impl TDynBenchFn for CallNetbox {
    fn run(&self, harness: &mut Bencher) {
        let conn = Conn::new(
            ("localhost", unsafe { crate::LISTEN }),
            ConnOptions {
                user: "test_user".into(),
                password: "password".into(),
                ..ConnOptions::default()
            },
            None,
        )
        .unwrap();
        harness.iter(|| {
            conn.call("test_stored_proc", &(1, 2), &Options::default())
                .unwrap();
        });
    }
}

pub struct CallNetworkClient;

impl TDynBenchFn for CallNetworkClient {
    fn run(&self, harness: &mut Bencher) {
        let client = fiber::block_on(Client::connect_with_config(
            "localhost",
            unsafe { crate::LISTEN },
            ProtocolConfig {
                creds: Some(("test_user".to_owned(), "password".to_owned())),
            },
        ))
        .unwrap();
        harness.iter(|| {
            fiber::block_on(async {
                client.call("test_stored_proc", &(1, 2)).await.unwrap();
            });
        });
    }
}

pub struct StartAsyncRuntime;

impl TDynBenchFn for StartAsyncRuntime {
    fn run(&self, harness: &mut Bencher) {
        harness.iter(|| {
            fiber::block_on(black_box(async {}));
        });
    }
}

fn start_tcp_listener(port: u16) {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
    // Spawn listener
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            std::thread::spawn(move || loop {
                let mut buf = vec![0; 3];
                if stream.read(&mut buf).is_err() {
                    break;
                }
                if stream.write_all(&buf).is_err() {
                    break;
                }
            });
        }
    });
}

pub struct PingCoIOStream;

impl TDynBenchFn for PingCoIOStream {
    fn run(&self, harness: &mut Bencher) {
        use std::io::{Read, Write};
        use tarantool::coio::CoIOStream;

        start_tcp_listener(3303);
        let mut stream = CoIOStream::connect(("localhost", 3303)).unwrap();

        harness.iter(|| {
            stream.write_all(&[1, 2, 3]).unwrap();
            let mut buf = vec![0; 3];
            stream.read_exact(&mut buf).unwrap();
        });
    }
}

pub struct PingClientTcpStream;

impl TDynBenchFn for PingClientTcpStream {
    fn run(&self, harness: &mut Bencher) {
        use futures::{AsyncReadExt, AsyncWriteExt};
        use tarantool::network::client::tcp::TcpStream;

        start_tcp_listener(3304);
        let mut stream = fiber::block_on(TcpStream::connect("localhost", 3304)).unwrap();

        harness.iter(|| {
            fiber::block_on(async {
                stream.write_all(&[1, 2, 3]).await.unwrap();
                let mut buf = vec![0; 3];
                stream.read_exact(&mut buf).await.unwrap();
            });
        });
    }
}
