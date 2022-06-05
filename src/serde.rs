// Copyright 2022 Daniel Harrison. All Rights Reserved.

//! [serde::Serializer] implementations for crate types.

use serde::ser::SerializeSeq;

use crate::col::{Col, Data};
use crate::Table;

/// Row-oriented serialization of a [Table]'s data.
#[derive(Debug)]
pub struct Rows<'t, T>(pub &'t T);

impl<T> serde::Serialize for Rows<'_, T>
where
    T: Table,
    for<'a> <<T as Table>::Data as Data>::Ref<'a>: serde::Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let Rows(table) = self;
        let mut table = (*table).clone();
        let mut batch = <T::Data as Data>::Col::default();

        let mut seq = serializer.serialize_seq(None)?;
        for batch_idx in 0..table.num_batches() {
            batch.clear();
            table.gen_batch(batch_idx, &mut batch);
            for idx in 0..batch.len() {
                seq.serialize_element(&batch.get(idx))?;
            }
        }
        seq.end()
    }
}

#[cfg(test)]
mod tests {
    use crate::kvtd::{Kvtd, KvtdConfig};
    use crate::Set;

    use super::*;

    #[test]
    fn rows() {
        let table = Kvtd::init(KvtdConfig {
            val_bytes: 4,
            num_rows: 3,
            max_rows_per_batch: 3,
        });

        let actual = serde_json::to_string(&Rows(&table)).unwrap();
        const EXPECTED: &str = "[\
[\"0000000000000000\",[197,153,189,113],0,1],\
[\"0000000000000001\",[138,50,122,226],1,1],\
[\"0000000000000002\",[79,203,55,83],2,1]\
]";
        assert_eq!(actual, EXPECTED);
    }
}
