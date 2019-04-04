// Copyright 2019 Daniel Harrison. All Rights Reserved.

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
  let w_id = I64s(vec![batch_idx as i64]);
  let name = Strings(vec!["name"]);
  let street_1 = Strings(vec!["street_1"]);
  let street_2 = Strings(vec!["street_2"]);
  let city = Strings(vec!["city"]);
  let state = Strings(vec!["state"]);
  let zip = Strings(vec!["zip"]);
  let tax = F64s(vec![1.0]);
  let ytd = F64s(vec![300000.00]);
  return workload::Cols {
    length: 1,
    cols: vec![w_id, name, street_1, street_2, city, state, zip, tax, ytd],
  };
}

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
      "0,name,street_1,street_2,city,state,zip,1,300000\n1,name,street_1,street_2,city,state,zip,1,300000\n",
      wtr_as_string(wtr)
    );
    Ok(())
  }
}
