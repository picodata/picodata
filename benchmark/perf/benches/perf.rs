use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use picodata::pgproto::backend::describe::MetadataColumn;
use picodata::pgproto::backend::storage::port_read_tuples;
use postgres_types::Type;
use std::hint::black_box;

fn bench_pgproto_port_repack(crit: &mut Criterion) {
    let mut port: Vec<Vec<u8>> = Vec::new();
    for _ in 0..1024 {
        let mut tuple = Vec::new();
        tuple.extend_from_slice(b"\x93\xa7welcome\xa6aboard\xa8username");
        port.push(tuple);
    }
    let metadata = [
        MetadataColumn::new("a".into(), Type::TEXT),
        MetadataColumn::new("b".into(), Type::TEXT),
        MetadataColumn::new("c".into(), Type::TEXT),
    ];
    let mut group = crit.benchmark_group("pgproto");
    group
        .throughput(Throughput::Bytes(port.len() as u64))
        .bench_with_input("port_repack", &(port, metadata), |b, (port, metadata)| {
            b.iter_with_large_drop(|| {
                black_box(
                    port_read_tuples(port.iter().map(|t| t.as_slice()), 1024, metadata).unwrap(),
                );
            });
        });
    group.finish();
}

criterion_group!(benches, bench_pgproto_port_repack,);
criterion_main!(benches);
