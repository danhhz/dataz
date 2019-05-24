// Copyright 2019 Daniel Harrison. All Rights Reserved.

use serde;
use serde::ser::SerializeTuple;

pub struct GeneratorMeta {
  pub name: &'static str,
  pub new: fn() -> Box<Generator>,
}

pub trait Generator {
  fn meta(&self) -> GeneratorMeta;
  fn tables(&self) -> Vec<Table>;
}

pub struct Table {
  pub name: &'static str,
  pub data: ColGenerator,
}

pub struct ColGenerator {
  pub num_batches: u64,
  pub batch: fn(idx: u64) -> Cols,
}

pub struct Cols {
  pub length: usize,
  pub cols: Vec<Col>,
}

impl Cols {
  pub fn as_rows(self) -> AsRows {
    AsRows(self)
  }
}

pub struct AsRows(Cols);

impl serde::Serialize for AsRows {
  fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    let idx = 0;
    let mut row = s.serialize_tuple(self.0.cols.len())?;
    // TODO(dan): Serialize all the rows.
    for col in self.0.cols.iter() {
      match col {
        Col::I64s(xs) => row.serialize_element(&xs[idx])?,
        Col::F64s(xs) => row.serialize_element(&xs[idx])?,
        Col::Strings(xs) => row.serialize_element(&xs[idx])?,
      };
    }
    row.end()
  }
}

pub enum Col {
  I64s(Vec<i64>),
  F64s(Vec<f64>),
  Strings(Vec<String>),
}

impl Col {
  pub fn size(&self) -> u64 {
    match self {
      Col::I64s(xs) => (xs.len() * 8) as u64,
      Col::F64s(xs) => (xs.len() * 8) as u64,
      Col::Strings(xs) => xs.iter().map(|x| x.len() as u64).sum(),
    }
  }
}
