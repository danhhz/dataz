// Copyright 2019 Daniel Harrison. All Rights Reserved.

use rand;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use workload;

pub fn new() -> Box<workload::Generator> {
  Box::new(TPCCGenerator { warehouses: 1 })
}

pub struct TPCCGenerator {
  warehouses: u64,
}

impl workload::Generator for TPCCGenerator {
  fn meta(&self) -> workload::GeneratorMeta {
    workload::GeneratorMeta {
      name: "tpcc",
      new: new,
    }
  }
  fn tables(&self) -> Vec<workload::Table> {
    let warehouse = workload::Table {
      name: "warehouses",
      data: workload::ColGenerator {
        num_batches: self.warehouses,
        batch: warehouse_batch,
      },
    };
    vec![warehouse]
  }
}

fn warehouse_batch(batch_idx: u64) -> workload::Cols {
  use workload::Col::*;

  let mut rng = SmallRng::seed_from_u64(batch_idx);

  // Warehouse ids are 0-indexed, every other table is 1-indexed.
  let w_id = I64s(vec![batch_idx as i64]);
  let name = Strings(vec![rand_int(&mut rng, 6, 10).to_string()]);
  let street_1 = Strings(vec![rand_int(&mut rng, 10, 20).to_string()]);
  let street_2 = Strings(vec![rand_int(&mut rng, 10, 20).to_string()]);
  let city = Strings(vec![rand_int(&mut rng, 10, 20).to_string()]);
  let state = Strings(vec![rand_state(&mut rng)]);
  let zip = Strings(vec![rand_zip(&mut rng)]);
  let tax = F64s(vec![rand_tax(&mut rng)]);
  let ytd = F64s(vec![WAREHOUSE_YTD]);
  workload::Cols {
    length: 1,
    cols: vec![w_id, name, street_1, street_2, city, state, zip, tax, ytd],
  }
}

// rand_int returns a number within [min, max] inclusive. See 2.1.4.
fn rand_int<R: rand::Rng>(rng: &mut R, min_len: usize, max_len: usize) -> usize {
  rng.gen_range(min_len, max_len)
}

// rand_state produces a random US state. Spec just says 2 letters.
fn rand_state<R: rand::Rng>(rng: &mut R) -> String {
  rand_string_from_alphabet(rng, 2, 2, ALPHABET_LETTERS)
}

// rand_zip produces a random "zip code" - a 4-digit number plus the constant
// "11111". See 4.3.2.7.
fn rand_zip<R: rand::Rng>(rng: &mut R) -> String {
  rand_string_from_alphabet(rng, 4, 4, ALPHABET_NUMERIC) + "11111"
}

// rand_tax produces a random tax between [0.0000..0.2000] See 2.1.5.
fn rand_tax<R: rand::Rng>(rng: &mut R) -> f64 {
  return rand_int(rng, 0, 2000) as f64 / 10000.0;
}

// rand_state produces a random US state. Spec just says 2 letters.
fn rand_string_from_alphabet<R: rand::Rng>(
  rng: &mut R,
  min_len: usize,
  max_len: usize,
  alphabet: &'static [u8],
) -> String {
  let size = if min_len == max_len {
    max_len
  } else {
    rand_int(rng, min_len, max_len)
  };
  let buf = (0..size)
    .map(|_| alphabet[rng.gen_range(0, alphabet.len()) as usize])
    .collect();
  String::from_utf8(buf).unwrap()
}

// These constants are all set by the spec - they're not knobs. Don't change
// them.
const WAREHOUSE_YTD: f64 = 300000.00;
const ALPHABET_LETTERS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const ALPHABET_NUMERIC: &[u8] = b"1234567890";

#[cfg(test)]
mod tests {
  use super::*;
  use csv;
  use workload::Generator;

  fn wtr_as_string(wtr: csv::Writer<Vec<u8>>) -> String {
    String::from_utf8(wtr.into_inner().unwrap()).unwrap()
  }

  #[test]
  fn test_tpcc() -> Result<(), Box<csv::Error>> {
    let g = TPCCGenerator { warehouses: 2 };
    let mut wtr = csv::Writer::from_writer(vec![]);
    for t in g.tables() {
      let d = t.data;
      for idx in 0..d.num_batches {
        wtr.serialize((d.batch)(idx))?;
      }
    }
    wtr.flush().unwrap();
    assert_eq!(
      "0,7,18,13,11,KH,155611111,0.143,300000\n1,9,11,17,15,KT,259011111,0.1568,300000\n",
      wtr_as_string(wtr)
    );
    Ok(())
  }
}
