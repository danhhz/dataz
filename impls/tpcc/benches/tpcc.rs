// Copyright 2019 Daniel Harrison. All Rights Reserved.

extern crate bencher;

use bencher::{benchmark_group, benchmark_main};

fn tpcc(b: &mut bencher::Bencher) {
    let g = tpcc::new();
    let mut n: u64 = 0;
    let mut bytes: u64 = 0;
    b.iter(|| {
        n += 1;
        for t in g.tables() {
            let d = t.data;
            for idx in 0..d.num_batches {
                for col in (d.batch)(idx).cols.iter() {
                    bytes += col.size()
                }
            }
        }
    });
    b.bytes = bytes / n;
}

benchmark_group!(benches, tpcc);
benchmark_main!(benches);
