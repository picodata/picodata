//! Criterion benchmarks for `sql-dynfilter`.
//!
//! Three groups:
//!  * `build` — cost of `DynamicFilter::new` + N `insert` + `finalize`
//!    for representative N spanning the SmallSet / Fuse threshold.
//!  * `probe` — cost of `FilterView::contains` on 10M probes at three
//!    hit ratios (0%, 50%, 100%).
//!  * `wire`  — printed in stderr at the end so the diploma can quote
//!    bytes-per-element for SmallSet vs Fuse on a 1k build.
//!
//! Run with `cargo bench -p sql-dynfilter`. CI does not run this; the
//! numbers feed the diploma §5.8 measurements.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use sql_dynfilter::{DynamicFilter, FilterView, FUSE_THRESHOLD};

fn keys_n(n: usize) -> Vec<u64> {
    // Splitmix-style spread, deterministic per n. Independent of
    // the internal hasher so the bench measures filter cost only.
    (0..n as u64)
        .map(|i| {
            let mut x = i.wrapping_add(0x9E3779B97F4A7C15);
            x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
            x ^ (x >> 31)
        })
        .collect()
}

fn build_filter(n: usize) -> Vec<u8> {
    let keys = keys_n(n);
    let mut f = DynamicFilter::new(n);
    for k in &keys {
        f.insert(*k);
    }
    f.finalize().expect("finalize converged");
    let mut buf = Vec::with_capacity(f.encoded_len());
    f.encode_into(&mut buf).expect("encode_into");
    buf
}

fn bench_build(c: &mut Criterion) {
    let mut g = c.benchmark_group("build");
    for &n in &[10usize, 100, 1_000, 10_000, 100_000, 1_000_000] {
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let keys = keys_n(n);
            b.iter(|| {
                let mut f = DynamicFilter::new(n);
                for k in &keys {
                    f.insert(black_box(*k));
                }
                f.finalize().expect("finalize");
                black_box(f);
            });
        });
    }
    g.finish();
}

fn bench_probe(c: &mut Criterion) {
    let mut g = c.benchmark_group("probe");
    let n = 10_000usize;
    let build_keys = keys_n(n);
    let mut f = DynamicFilter::new(n);
    for k in &build_keys {
        f.insert(*k);
    }
    f.finalize().expect("finalize");
    let mut wire = Vec::with_capacity(f.encoded_len());
    f.encode_into(&mut wire).expect("encode");
    let view = FilterView::decode(&wire).expect("decode");

    let probes = 1_000_000usize;
    let mix = |ratio: f64| -> Vec<u64> {
        let n_hit = (probes as f64 * ratio) as usize;
        let mut out = Vec::with_capacity(probes);
        for i in 0..n_hit {
            out.push(build_keys[i % build_keys.len()]);
        }
        // Misses: shift into a disjoint zone.
        for i in n_hit..probes {
            out.push((i as u64).wrapping_mul(0xDEADBEEF) | (1u64 << 63));
        }
        out
    };
    for &ratio in &[0.0f64, 0.5, 1.0] {
        let id = format!("hit_ratio={ratio:.1}");
        let probes_data = mix(ratio);
        g.throughput(Throughput::Elements(probes as u64));
        g.bench_with_input(BenchmarkId::from_parameter(id), &probes_data, |b, p| {
            b.iter(|| {
                let mut hits = 0u64;
                for &k in p.iter() {
                    if view.contains(black_box(k)) {
                        hits += 1;
                    }
                }
                black_box(hits);
            });
        });
    }
    g.finish();
}

fn bench_wire_size(c: &mut Criterion) {
    // Not really a benchmark — print sizes once so the diploma can
    // quote bytes/element. Wrapped in a group with a single noop so
    // `cargo bench` prints the line.
    let smallset_n = FUSE_THRESHOLD.saturating_sub(1).max(1);
    let small = build_filter(smallset_n);
    let fuse = build_filter(1_000);
    eprintln!(
        "wire_size: smallset(n={smallset_n}) = {small_bytes} bytes ({sb_per:.2} B/el), \
         fuse(n=1000) = {fuse_bytes} bytes ({fb_per:.3} B/el)",
        small_bytes = small.len(),
        sb_per = small.len() as f64 / smallset_n as f64,
        fuse_bytes = fuse.len(),
        fb_per = fuse.len() as f64 / 1000f64,
    );
    let mut g = c.benchmark_group("wire");
    g.bench_function("noop", |b| b.iter(|| black_box(0u8)));
    g.finish();
}

criterion_group!(benches, bench_build, bench_probe, bench_wire_size);
criterion_main!(benches);
