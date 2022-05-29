// Copyright 2022 Daniel Harrison. All Rights Reserved.

//! High-throughput computational dataset synthesis

#![warn(missing_docs, missing_debug_implementations)]

pub mod kvtd;

#[cfg(feature = "serde")]
pub mod serde;

/// A named dataset made up of one or more [Table]s.
pub trait Set {
    /// Configuration necessary for construct this dataset.
    type Config;

    /// Construct an instance of this dataset with the given configuration.
    fn init(config: Self::Config) -> Self;
}

/// A named set of data with a uniform schema.
///
/// A table is generated in batches, each of which contains one or more rows.
/// For each parallelization, each batch can be generated purely as a function
/// of its index and dataset configuration. For vectorization, a batch is
/// internally arranged in columns.
pub trait Table {
    /// The columnar batch with this table's schema.
    ///
    /// TODO: Figure out a way to abstract out batches into a columnar trait.
    type Batch;

    /// The number of batches of data in this table.
    fn num_batches(&self) -> usize;

    /// Generates the requested batch's data.
    ///
    /// This clears the given batch and reuses allocations when possible. If the
    /// requested index is out of bounds, an empty batch is generated.
    fn gen_batch(&self, idx: usize, batch: &mut Self::Batch);
}
