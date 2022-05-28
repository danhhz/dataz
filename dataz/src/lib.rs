// Copyright 2019 Daniel Harrison. All Rights Reserved.

use crate::col::Cols;

pub mod col;

pub struct DatasetMeta {
    pub name: &'static str,
    pub new: fn() -> Box<dyn Dataset>,
}

pub trait Dataset {
    fn meta(&self) -> DatasetMeta;
    fn tables(&self) -> Vec<Table>;
}

pub struct Table {
    pub name: &'static str,
    pub data: DataGenerator,
}

pub struct DataGenerator {
    pub num_batches: u64,
    pub batch: fn(idx: u64) -> Cols,
}
