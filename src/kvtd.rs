// Copyright 2022 Daniel Harrison. All Rights Reserved.

//! `(Key, Value, Time, Diff)` tuples a la [Differential Dataflow]
//!
//! [Differential Dataflow]: https://crates.io/crates/differential-dataflow

use crate::{Set, Table};

/// Configuration for [Kvtd].
#[derive(Debug)]
pub struct KvtdConfig {
    /// The number of bytes in the val column of each row.
    pub val_bytes: usize,
    /// The total number of rows in all batches.
    pub num_rows: usize,
    /// The maximum number of rows to include in any given batch.
    pub max_rows_per_batch: usize,
}

/// `(Key, Value, Time, Diff)` tuples a la [Differential Dataflow]
///
/// [Differential Dataflow]: https://crates.io/crates/differential-dataflow
#[derive(Debug)]
pub struct Kvtd {
    pub(crate) config: KvtdConfig,
}

impl Kvtd {}

impl Set for Kvtd {
    type Config = KvtdConfig;

    fn init(config: Self::Config) -> Self {
        Kvtd { config }
    }
}

impl Table for Kvtd {
    type Batch = KvtdBatch;

    fn num_batches(&self) -> usize {
        (self.config.num_rows + self.config.max_rows_per_batch - 1) / self.config.max_rows_per_batch
    }

    fn gen_batch(&self, idx: usize, batch: &mut KvtdBatch) {
        let row_start = idx * self.config.max_rows_per_batch;
        let row_end = std::cmp::min(
            row_start + self.config.max_rows_per_batch,
            self.config.num_rows,
        );
        batch.len = row_end.saturating_sub(row_start);
        batch.val_bytes = self.config.val_bytes;
        if batch.len == 0 {
            return;
        }

        gen_keys(&mut batch.keys, row_start, batch.len);
        gen_vals(&mut batch.vals, row_start, batch.len, self.config.val_bytes);
        gen_times(&mut batch.times, row_start, batch.len);
        gen_diffs(&mut batch.diffs, batch.len);
    }
}

/// A batch of [Kvtd] data.
#[derive(Debug, Default)]
pub struct KvtdBatch {
    len: usize,
    val_bytes: usize,

    keys: String,
    vals: Vec<u8>,
    times: Vec<u64>,
    diffs: Vec<i64>,
}

impl KvtdBatch {
    /// Returns the number of rows in this batch.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the requested row.
    pub fn get(&self, idx: usize) -> Option<(&str, &[u8], u64, i64)> {
        if idx >= self.len {
            return None;
        }
        let key = &self.keys[idx * KEY_BYTES..(idx + 1) * KEY_BYTES];
        let val = &self.vals[idx * self.val_bytes..(idx + 1) * self.val_bytes];
        let time = self.times[idx];
        let diff = self.diffs[idx];
        Some((key, val, time, diff))
    }
}

const KEY_BYTES: usize = 64 / 4;
fn gen_keys(col: &mut String, start: usize, len: usize) {
    col.clear();
    col.reserve(len * KEY_BYTES);
    for x in start..start + len {
        to_hex(col, x);
    }
}

fn gen_vals(col: &mut Vec<u8>, start: usize, len: usize, val_len: usize) {
    col.clear();
    col.reserve(len * val_len);

    const LARGE_PRIME: usize = 18_446_744_073_709_551_557;
    for idx in start..start + len {
        // Generate val_len bytes using Knuth's multiplicative integer hashing
        // method, seeded with the row_idx (plus one so that we don't start with
        // all zeros for idx 0).
        let mut x = idx + 1;
        for _ in 0..val_len {
            x = x.wrapping_mul(LARGE_PRIME);
            // TODO: Do this 8 bytes at a time instead.
            col.push(x as u8);
        }
    }
}

fn gen_times(col: &mut Vec<u64>, start: usize, len: usize) {
    col.clear();
    col.reserve(len);
    for x in start as u64..(start + len) as u64 {
        col.push(x);
    }
}

fn gen_diffs(col: &mut Vec<i64>, len: usize) {
    col.clear();
    col.resize(len, 1i64);
}

fn to_hex(col: &mut String, x: usize) {
    const TOP_FOUR_BITS_MASK: usize = 0xf000_0000_0000_0000;
    const HEX_LOOKUP: &[char; 16] = &[
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    ];
    let mut x = x;
    for _ in 0..KEY_BYTES {
        // TODO: Do this two characters at a time instead.
        col.push(HEX_LOOKUP[(x & TOP_FOUR_BITS_MASK) >> 60]);
        x = x << 4;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex() {
        #[track_caller]
        fn test_case(i: usize, expected: &str) {
            let mut actual = String::new();
            to_hex(&mut actual, i);
            assert_eq!(actual, expected);
        }

        test_case(0, "0000000000000000");
        test_case(1, "0000000000000001");
        test_case(2, "0000000000000002");
        test_case(3, "0000000000000003");
        test_case(4, "0000000000000004");
        test_case(5, "0000000000000005");
        test_case(6, "0000000000000006");
        test_case(7, "0000000000000007");
        test_case(8, "0000000000000008");
        test_case(9, "0000000000000009");
        test_case(10, "000000000000000a");
        test_case(11, "000000000000000b");
        test_case(12, "000000000000000c");
        test_case(13, "000000000000000d");
        test_case(14, "000000000000000e");
        test_case(15, "000000000000000f");
        test_case(16, "0000000000000010");
        test_case(17, "0000000000000011");
        test_case(u64::MAX as usize, "ffffffffffffffff");
    }
}
