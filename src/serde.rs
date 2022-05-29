// Copyright 2022 Daniel Harrison. All Rights Reserved.

//! [serde::Serializer] implementations for crate types.

use serde::ser::{SerializeSeq, SerializeStruct};

use crate::kvtd::{Kvtd, KvtdBatch};
use crate::Table;

/// Serializes the [Table]'s data as a sequence of tuples.
///
/// TODO: Make this generic over any table impl;
#[derive(Debug)]
pub struct Tuples(pub Kvtd);

impl serde::Serialize for Tuples {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.config.num_rows))?;

        let mut batch = KvtdBatch::default();
        for batch_idx in 0..self.0.num_batches() {
            self.0.gen_batch(batch_idx, &mut batch);
            for idx in 0..batch.len() {
                seq.serialize_element(&batch.get(idx).unwrap())?;
            }
        }

        seq.end()
    }
}

/// Serializes the [Table]'s data as a sequence of structs.
///
/// TODO: Make this generic over any table impl;
#[derive(Debug)]
pub struct Structs(pub Kvtd);

impl serde::Serialize for Structs {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.config.num_rows))?;

        let mut batch = KvtdBatch::default();
        for batch_idx in 0..self.0.num_batches() {
            self.0.gen_batch(batch_idx, &mut batch);
            for idx in 0..batch.len() {
                seq.serialize_element(&KvtdRow(batch.get(idx).unwrap()))?;
            }
        }

        seq.end()
    }
}

#[derive(Debug)]
struct KvtdRow<'a>((&'a str, &'a [u8], u64, i64));

impl serde::Serialize for KvtdRow<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let KvtdRow((key, val, time, diff)) = self;
        let mut sct = serializer.serialize_struct("kvtd", 4)?;
        sct.serialize_field("key", key)?;
        sct.serialize_field("val", val)?;
        sct.serialize_field("time", time)?;
        sct.serialize_field("diff", diff)?;
        sct.end()
    }
}

#[cfg(test)]
mod tests {
    use crate::kvtd::KvtdConfig;
    use crate::Set;

    use super::*;

    #[test]
    fn tuples() {
        let table = Kvtd::init(KvtdConfig {
            val_bytes: 4,
            num_rows: 3,
            max_rows_per_batch: 3,
        });

        let actual = serde_json::to_string(&Tuples(table)).unwrap();
        const EXPECTED: &str = "[\
[\"0000000000000000\",[197,153,189,113],0,1],\
[\"0000000000000001\",[138,50,122,226],1,1],\
[\"0000000000000002\",[79,203,55,83],2,1]\
]";
        assert_eq!(actual, EXPECTED);
    }

    #[test]
    fn structs() {
        let table = Kvtd::init(KvtdConfig {
            val_bytes: 4,
            num_rows: 3,
            max_rows_per_batch: 3,
        });

        let actual = serde_json::to_string(&Structs(table)).unwrap();
        const EXPECTED: &str = "[\
{\"key\":\"0000000000000000\",\"val\":[197,153,189,113],\"time\":0,\"diff\":1},\
{\"key\":\"0000000000000001\",\"val\":[138,50,122,226],\"time\":1,\"diff\":1},\
{\"key\":\"0000000000000002\",\"val\":[79,203,55,83],\"time\":2,\"diff\":1}\
]";
        assert_eq!(actual, EXPECTED);
    }
}
