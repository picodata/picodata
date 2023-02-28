use std::{
    future::Future,
    time::{Duration, Instant},
};

use tarantool::{
    fiber,
    net_box::{Conn, ConnOptions, Options},
    network::client::{AsClient as _, Client},
    proc,
};

const N_ITERS: usize = 100_000;

#[proc]
fn l_n_iters() -> usize {
    N_ITERS
}

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

#[proc]
fn l_print_stats(fn_name: &str, samples: Vec<i64>) {
    assert_eq!(samples.len(), N_ITERS);
    print_stats(fn_name, samples.iter().map(|v| *v as u128).collect())
}

fn harness_iter(mut f: impl FnMut()) -> Vec<u128> {
    let mut samples = vec![];
    for _ in 0..N_ITERS {
        let start = Instant::now();
        f();
        samples.push(start.elapsed().as_nanos());
    }
    samples
}

fn harness_iter_async<F: Future>(mut f: impl FnMut() -> F) -> Vec<u128> {
    let mut samples = vec![];
    fiber::block_on(async {
        for _ in 0..N_ITERS {
            let start = Instant::now();
            f().await;
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
