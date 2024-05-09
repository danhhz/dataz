// Copyright 2023 Daniel Harrison. All Rights Reserved.

use std::hint::black_box;

use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion, Throughput};
use dataz::col::{Col, Data};
use dataz::kvtd::{Kvtd, KvtdConfig};
use dataz::{DynTable, Set, Table};

fn kvtd(c: &mut Criterion) {
    let cfg = KvtdConfig {
        val_bytes: 4,
        num_rows: 1024 * 1024,
        max_rows_per_batch: 1024,
    };
    let table = Kvtd::init(cfg);
    let mut batch = <<Kvtd as Table>::Data as Data>::Col::default();

    let mut good_bytes = 0;
    for idx in 0..table.num_batches() {
        table.gen_batch(idx, &mut batch);
        good_bytes += batch.good_bytes();
    }

    let mut g = c.benchmark_group("kvtd");
    g.throughput(Throughput::Bytes(good_bytes as u64));
    g.bench_function("kvtd", |b| {
        b.iter(|| {
            for idx in 0..table.num_batches() {
                table.gen_batch(idx, &mut batch);
                black_box(&mut batch);
                table.gen_batch(idx, &mut batch);
                black_box(&mut batch);
            }
        })
    });
}

// The grouping here is an artifact of criterion's interaction with the
// plug-able rust benchmark harness. We use criterion's groups instead.
criterion_group!(benches, kvtd);
criterion_main!(benches);
