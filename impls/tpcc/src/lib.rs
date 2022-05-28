// Copyright 2019 Daniel Harrison. All Rights Reserved.

use dataz;
use dataz::col::Cols;
use rand;
use rand::rngs::SmallRng;
use rand::SeedableRng;

pub fn new() -> Box<dyn dataz::Dataset> {
    Box::new(TPCCGenerator { warehouses: 1 })
}

pub struct TPCCGenerator {
    warehouses: u64,
}

impl dataz::Dataset for TPCCGenerator {
    fn meta(&self) -> dataz::DatasetMeta {
        dataz::DatasetMeta { name: "tpcc", new }
    }

    fn tables(&self) -> Vec<dataz::Table> {
        let item = dataz::Table {
            name: "item",
            data: dataz::DataGenerator {
                num_batches: NUM_ITEMS,
                batch: item_batch,
            },
        };
        let warehouse = dataz::Table {
            name: "warehouse",
            data: dataz::DataGenerator {
                num_batches: self.warehouses,
                batch: warehouse_batch,
            },
        };
        vec![item, warehouse]
    }
}

fn item_batch(batch_idx: u64) -> Cols {
    use dataz::col::Col::*;

    let mut rng = SmallRng::seed_from_u64(batch_idx);

    // Warehouse ids are 0-indexed, every other table is 1-indexed.
    let i_id = I64s(vec![batch_idx as i64]);
    let i_im_id = I64s(vec![rand_int(&mut rng, 1, 10000) as i64]);
    let i_name = Strings(vec![rand_string_from_alphabet(
        &mut rng,
        14,
        24,
        ALPHABET_ALPHANUM,
    )]);
    let i_price = F64s(vec![rand_int(&mut rng, 100, 10000) as f64 / 100.0]);
    let i_data = Strings(vec![rand_original_string(&mut rng)]);
    Cols {
        length: 1,
        cols: vec![i_id, i_im_id, i_name, i_price, i_data],
    }
}

fn warehouse_batch(batch_idx: u64) -> Cols {
    use dataz::col::Col::*;

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
    Cols {
        length: 1,
        cols: vec![w_id, name, street_1, street_2, city, state, zip, tax, ytd],
    }
}

// rand_int returns a number within [min, max] inclusive. See 2.1.4.
fn rand_int<R: rand::Rng>(rng: &mut R, min_len: usize, max_len: usize) -> usize {
    rng.gen_range(min_len, max_len + 1)
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

// rand_original_string generates a random alphanumeric string with a 10% chance
// of containing the string "ORIGINAL" somewhere in the middle of the string.
// See 4.3.3.1.
fn rand_original_string<R: rand::Rng>(rng: &mut R) -> String {
    let mut buf = rand_string_from_alphabet(rng, 26, 50, ALPHABET_ALPHANUM).into_bytes();
    if rng.gen_range(0, 10) == 0 {
        let offset = rng.gen_range(0, buf.len() - STR_ORIGINAL.len() + 1);
        buf[offset..offset + 8].copy_from_slice(STR_ORIGINAL)
    }
    String::from_utf8(buf.to_vec()).unwrap()
}

// rand_string_from_alphabet produces a random string of length
// [min_len,max_len) from the given alphabet.
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
const NUM_ITEMS: u64 = 100000;
const WAREHOUSE_YTD: f64 = 300000.00;
const STR_ORIGINAL: &[u8] = b"ORIGINAL";
const ALPHABET_LETTERS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const ALPHABET_NUMERIC: &[u8] = b"1234567890";
const ALPHABET_ALPHANUM: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

#[cfg(test)]
mod tests {
    use super::*;
    use csv;
    use dataz::Dataset;

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
                wtr.serialize((d.batch)(idx).as_rows())?;
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
