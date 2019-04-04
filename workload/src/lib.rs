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

// TODO(dan): This serialization is row-oriented, name it like that.
impl serde::Serialize for Cols {
  fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    let idx = 0;
    let mut row = s.serialize_tuple(self.cols.len())?;
    // TODO(dan): Serialize all the rows.
    for col in self.cols.iter() {
      match col {
        Col::I64s(xs) => row.serialize_element(&xs[idx])?,
        Col::F64s(xs) => row.serialize_element(&xs[idx])?,
        Col::Strings(xs) => row.serialize_element(xs[idx])?,
      };
    }
    row.end()
  }
}

pub enum Col {
  I64s(Vec<i64>),
  F64s(Vec<f64>),
  Strings(Vec<&'static str>),
}
