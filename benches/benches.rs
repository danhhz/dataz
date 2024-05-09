// Copyright 2023 Daniel Harrison. All Rights Reserved.

use std::hint::black_box;

use codspeed_criterion_compat::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use dataz::col::{Col, Data};
use dataz::{Set, Table, TableFnMut};

fn table_iter_fn<T: Table>(mut table: T) -> impl FnMut() -> usize {
    let mut batch = <<T as Table>::Data as Data>::Col::default();
    move || {
        let mut good_bytes = 0;
        for idx in 0..table.num_batches() {
            batch.clear();
            table.gen_batch(idx, &mut batch);
            good_bytes += black_box(&mut batch).good_bytes();
        }
        good_bytes
    }
}

fn set_iter_fn<S: Set>(set: S) -> impl FnMut() -> usize {
    struct TableIterFns(Vec<Box<dyn FnMut() -> usize>>);
    impl TableFnMut<()> for TableIterFns {
        fn call_mut<T: Table>(&mut self, t: T) -> () {
            self.0.push(Box::new(table_iter_fn(t)));
        }
    }

    let mut table_iter_fns = TableIterFns(Vec::new());
    set.tables(&mut table_iter_fns);
    move || {
        let mut good_bytes = 0;
        for x in table_iter_fns.0.iter_mut() {
            good_bytes += x();
        }
        good_bytes
    }
}

fn bench_gen<S: Set>(c: &mut Criterion, name: &str, set: S) {
    let mut bench_iter_fn = set_iter_fn(set);
    let good_bytes = bench_iter_fn();

    let mut g = c.benchmark_group(name);
    g.throughput(Throughput::Bytes(good_bytes as u64));
    g.bench_function(BenchmarkId::new(name, human_bytes(good_bytes)), |b| {
        b.iter(|| black_box(bench_iter_fn()));
    });
}

fn gen(c: &mut Criterion) {
    bench_gen(
        c,
        "kvtd",
        dataz::kvtd::Kvtd::init(dataz::kvtd::KvtdConfig {
            val_bytes: 4,
            num_rows: 1024 * 1024,
            max_rows_per_batch: 1024,
        }),
    );
    #[cfg(feature = "rand")]
    bench_gen(
        c,
        "tpcc",
        dataz::tpcc::Tpcc::init(dataz::tpcc::TpccConfig {
            warehouses: 1,
            now: dataz::tpcc::TpccConfig::FEB_18_2023_1_PM,
        }),
    );
}

pub fn human_bytes(x: usize) -> String {
    const KIB: usize = 1024;
    const MIB: usize = 1024 * KIB;
    const GIB: usize = 1024 * MIB;
    if x >= GIB {
        format!("{}GiB", x / GIB)
    } else if x >= MIB {
        format!("{}MiB", x / MIB)
    } else if x >= KIB {
        format!("{}KiB", x / KIB)
    } else {
        format!("{}B", x)
    }
}

// The grouping here is an artifact of criterion's interaction with the
// plug-able rust benchmark harness. We use criterion's groups instead.
criterion_group!(benches, gen);
criterion_main!(benches);
