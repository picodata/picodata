use std::{future::Future, time::Duration};

use tarantool::{fiber, proc, time::Instant};

const N_ITERS: usize = 10_000;
const PREHEAT_ITERS: usize = 1_000;

#[proc]
fn l_n_iters() -> usize {
    N_ITERS
}

mod iproto_clients {
    use super::{harness_iter, harness_iter_async, print_stats};
    use tarantool::{
        fiber,
        net_box::{Conn, ConnOptions, Options},
        network::client::{AsClient as _, Client},
        proc,
    };

    #[proc]
    fn bench_network_client() {
        let client = fiber::block_on(Client::connect("localhost", 3301)).unwrap();
        let samples = harness_iter_async(|| async {
            client.call("test_stored_proc", &(1, 2)).await.unwrap();
        });
        print_stats("network_client", samples);
    }

    #[proc]
    fn bench_netbox() {
        let conn = Conn::new(
            ("localhost", 3301),
            ConnOptions {
                ..ConnOptions::default()
            },
            None,
        )
        .unwrap();
        conn.wait_connected(None).unwrap();
        let samples = harness_iter(|| {
            conn.call("test_stored_proc", &(1, 2), &Options::default())
                .unwrap();
        });
        print_stats("netbox", samples);
    }
}

mod msgpack_serialization {
    use super::{harness_iter, print_stats};
    use serde::{Deserialize, Serialize};
    use tarantool::msgpack::*;
    use tarantool::proc;

    const HEIGHT: usize = 5;
    const DEGREE: usize = 4;

    #[derive(Encode, Decode, Serialize, Deserialize)]
    enum Foo {
        Bar(usize),
        Baz(usize),
        None,
    }

    #[derive(Encode, Decode, Serialize, Deserialize)]
    struct Node {
        s: String,
        n: usize,
        e: Foo,
        leaves: Vec<Node>,
    }

    fn gen_tree(height: usize, degree: usize) -> Node {
        let mut node = Node {
            leaves: vec![],
            s: format!("height is {}", height),
            n: height * degree,
            e: Foo::Bar(height),
        };
        if height == 0 {
            return node;
        }
        for _ in 0..degree {
            // Recursion should be ok for testing purposes
            node.leaves.push(gen_tree(height - 1, degree));
        }
        node
    }

    #[proc]
    fn bench_custom_encode() {
        let tree = gen_tree(HEIGHT, DEGREE);
        let samples = harness_iter(|| {
            let _bytes = encode(&tree).unwrap();
        });
        print_stats("custom_encode", samples);
    }

    #[proc]
    fn bench_custom_decode() {
        let tree = gen_tree(HEIGHT, DEGREE);
        let bytes = encode(&tree).unwrap();
        let samples = harness_iter(|| {
            let _got_tree: Node = decode(&bytes).unwrap();
        });
        print_stats("custom_decode", samples);
    }

    #[proc]
    fn bench_serde_encode() {
        let tree = gen_tree(HEIGHT, DEGREE);
        let samples = harness_iter(|| {
            let _bytes = rmp_serde::to_vec(&tree).unwrap();
        });
        print_stats("serde_encode", samples);
    }

    #[proc]
    fn bench_serde_decode() {
        let tree = gen_tree(HEIGHT, DEGREE);
        let bytes = rmp_serde::to_vec(&tree).unwrap();
        let samples = harness_iter(|| {
            let _got_tree: Node = rmp_serde::from_slice(&bytes).unwrap();
        });
        print_stats("serde_decode", samples);
    }
}

#[proc]
fn l_print_stats(fn_name: &str, samples: Vec<i64>) {
    assert_eq!(samples.len(), N_ITERS);
    print_stats(fn_name, samples.iter().map(|v| *v as u128).collect())
}

#[allow(clippy::unit_arg)]
fn harness_iter(mut f: impl FnMut()) -> Vec<u128> {
    // Preheating
    for _ in 0..PREHEAT_ITERS {
        std::hint::black_box(f());
    }

    let mut samples = Vec::with_capacity(N_ITERS);
    for _ in 0..N_ITERS {
        let start = Instant::now();
        std::hint::black_box(f());
        samples.push(start.elapsed().as_nanos());
    }
    samples
}

#[allow(clippy::unit_arg)]
fn harness_iter_async<F: Future>(mut f: impl FnMut() -> F) -> Vec<u128> {
    // Preheating
    fiber::block_on(async {
        for _ in 0..PREHEAT_ITERS {
            std::hint::black_box(f().await);
        }
    });

    let mut samples = Vec::with_capacity(N_ITERS);
    fiber::block_on(async {
        for _ in 0..N_ITERS {
            let start = Instant::now();
            std::hint::black_box(f().await);
            samples.push(start.elapsed().as_nanos());
        }
    });
    samples
}

fn print_stats(fn_name: &str, samples: Vec<u128>) {
    let mean: f64 = samples.iter().sum::<u128>() as f64 / N_ITERS as f64;
    let std_dev: f64 = (samples
        .into_iter()
        .fold(0f64, |sum, sample| sum + (mean - sample as f64).powi(2))
        / N_ITERS as f64)
        .sqrt();
    println!(
        "{}: {:?} +- {:?}",
        fn_name,
        Duration::from_nanos(mean as u64),
        Duration::from_nanos(std_dev as u64)
    )
}
