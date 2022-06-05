// Copyright 2023 Daniel Harrison. All Rights Reserved.

//! Transaction Processing Performance Council Benchmark C ([TPCC])
//!
//! [TPCC]: https://www.tpc.org/tpcc/
//!
//! An OLTP benchmark.

use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use crate::col::{Col, Data};
use crate::{DynTable, Set, Table, TableFnMut};

/// Configuration for [Tpcc].
#[derive(Debug, Clone)]
pub struct TpccConfig {
    /// The number of warehouses and the base unit of scaling.
    pub warehouses: usize,
    /// The value to use as the data generation time.
    pub now: DateTime,
}

impl TpccConfig {
    /// A fixed [DateTime] for testing convenience.
    pub const FEB_18_2023_1_PM: DateTime = DateTime {
        date: 44_973,
        time: 13 * 60 * 60,
    };
}

/// Transaction Processing Performance Council Benchmark C ([TPCC])
///
/// [TPCC]: https://www.tpc.org/tpcc/
///
/// An OLTP benchmark.
#[derive(Debug)]
pub struct Tpcc {
    config: TpccConfig,
}

impl Set for Tpcc {
    type Config = TpccConfig;

    fn init(config: Self::Config) -> Self {
        Tpcc { config }
    }

    fn tables<F: TableFnMut<()>>(&self, f: &mut F) {
        f.call_mut(Item::init(self.config.clone()));
        f.call_mut(Warehouse::init(self.config.clone()));
        f.call_mut(Stock::init(self.config.clone()));
        f.call_mut(District::init(self.config.clone()));
        f.call_mut(Customer::init(self.config.clone()));
        f.call_mut(History::init(self.config.clone()));
        f.call_mut(Order::init(self.config.clone()));
        f.call_mut(OrderLine::init(self.config.clone()));
        f.call_mut(NewOrder::init(self.config.clone()));
    }
}

/// A TPCC DateTime.
#[derive(Clone, Copy, Debug, Default)]
#[cfg_attr(any(test, feature = "serde"), derive(serde::Serialize))]
#[cfg_attr(any(test, feature = "serde"), serde(into = "u64"))]
pub struct DateTime {
    /// The number of days since 01/01/1900.
    pub date: u32,
    /// The number of seconds since midnight.
    pub time: u32,
}

// These constants are set by the spec - they're not knobs. Don't change them.
const NUM_ITEMS: usize = 100_000;
const NUM_STOCK_PER_WAREHOUSE: usize = 100_000;
const NUM_DISTRICTS_PER_WAREHOUSE: usize = 10;
const NUM_CUSTOMERS_PER_DISTRICT: usize = 3_000;
const NUM_HISTORY_PER_CUSTOMER: usize = 1;
const NUM_ORDERS_PER_DISTRICT: usize = 3_000;
const NUM_NEW_ORDERS_PER_DISTRICT: usize = 900;

const INITIAL_YTD: f64 = 300_000.00;
const N_STRING_ALPHABET: &[char] = &['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];
const A_STRING_ALPHABET: &[char] = &[
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
    't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
    'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4',
    '5', '6', '7', '8', '9',
];

/// The TPCC ITEM table.
#[derive(Debug, Clone)]
pub struct Item {
    // For allocation reuse
    i_name: String,
    i_data: String,
}

impl Item {
    /// Construct an instance of this table with the given configuration.
    pub fn init(_config: TpccConfig) -> Self {
        Item {
            i_name: String::with_capacity(24),
            i_data: String::with_capacity(50),
        }
    }
}

impl DynTable for Item {
    fn name(&self) -> &'static str {
        "item"
    }

    fn num_batches(&self) -> usize {
        NUM_ITEMS
    }
}

impl Table for Item {
    type Data = (u64, u64, String, f64, String);

    fn gen_batch<C: Col<Self::Data>>(&mut self, idx: usize, batch: &mut C) {
        if idx >= self.num_batches() {
            return;
        }

        let mut rng = SmallRng::seed_from_u64(idx as u64);

        let i_id = idx as u64;
        let i_im_id = rand_int(&mut rng, 1, 10000) as u64;
        let i_name = reuse(&mut self.i_name, |x| {
            rand_string_len(&mut rng, A_STRING_ALPHABET, 14, 24, x)
        });
        let i_price = rand_int(&mut rng, 100, 10000) as f64 / 100.0;
        let i_data = reuse(&mut self.i_data, |x| rand_original_string(&mut rng, x));

        batch.push((i_id, i_im_id, i_name, i_price, i_data));
    }
}

/// The TPCC WAREHOUSE table.
#[derive(Debug, Clone)]
pub struct Warehouse {
    config: TpccConfig,

    // For allocation reuse
    w_name: String,
    w_street_1: String,
    w_street_2: String,
    w_city: String,
    w_state: String,
    w_zip: String,
}

impl Warehouse {
    /// Construct an instance of this table with the given configuration.
    pub fn init(config: TpccConfig) -> Self {
        Warehouse {
            config,
            w_name: String::with_capacity(10),
            w_street_1: String::with_capacity(20),
            w_street_2: String::with_capacity(20),
            w_city: String::with_capacity(20),
            w_state: String::with_capacity(2),
            w_zip: String::with_capacity(9),
        }
    }
}

impl DynTable for Warehouse {
    fn name(&self) -> &'static str {
        "warehouse"
    }

    fn num_batches(&self) -> usize {
        self.config.warehouses
    }
}

impl Table for Warehouse {
    type Data = (
        u64,
        String,
        String,
        String,
        String,
        String,
        String,
        f64,
        f64,
    );

    fn gen_batch<C: Col<Self::Data>>(&mut self, idx: usize, batch: &mut C) {
        if idx >= self.num_batches() {
            return;
        }

        let mut rng = SmallRng::seed_from_u64(idx as u64);

        let w_id = idx as u64;
        let w_name = reuse(&mut self.w_name, |x| {
            rand_string_len(&mut rng, A_STRING_ALPHABET, 6, 10, x)
        });
        let w_street_1 = reuse(&mut self.w_street_1, |x| {
            rand_string_len(&mut rng, A_STRING_ALPHABET, 10, 20, x)
        });
        let w_street_2 = reuse(&mut self.w_street_2, |x| {
            rand_string_len(&mut rng, A_STRING_ALPHABET, 10, 20, x)
        });
        let w_city = reuse(&mut self.w_city, |x| {
            rand_string_len(&mut rng, A_STRING_ALPHABET, 10, 20, x)
        });
        let w_state = reuse(&mut self.w_state, |x| rand_state(&mut rng, x));
        let w_zip = reuse(&mut self.w_zip, |x| rand_zip(&mut rng, x));
        let w_tax = rand_tax(&mut rng);
        let w_ytd = INITIAL_YTD;
        batch.push((
            w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd,
        ));
    }
}

/// The TPCC STOCK table.
#[derive(Debug, Clone)]
pub struct Stock {
    config: TpccConfig,

    // For allocation reuse
    s_dist_01: String,
    s_dist_02: String,
    s_dist_03: String,
    s_dist_04: String,
    s_dist_05: String,
    s_dist_06: String,
    s_dist_07: String,
    s_dist_08: String,
    s_dist_09: String,
    s_dist_10: String,
    s_data: String,
}

impl Stock {
    /// Construct an instance of this table with the given configuration.
    pub fn init(config: TpccConfig) -> Self {
        Stock {
            config,
            s_dist_01: String::with_capacity(24),
            s_dist_02: String::with_capacity(24),
            s_dist_03: String::with_capacity(24),
            s_dist_04: String::with_capacity(24),
            s_dist_05: String::with_capacity(24),
            s_dist_06: String::with_capacity(24),
            s_dist_07: String::with_capacity(24),
            s_dist_08: String::with_capacity(24),
            s_dist_09: String::with_capacity(24),
            s_dist_10: String::with_capacity(24),
            s_data: String::with_capacity(50),
        }
    }
}

impl DynTable for Stock {
    fn name(&self) -> &'static str {
        "stock"
    }

    fn num_batches(&self) -> usize {
        self.config.warehouses * NUM_STOCK_PER_WAREHOUSE
    }
}

impl Table for Stock {
    type Data = (
        u64,
        u64,
        u64,
        String,
        String,
        String,
        String,
        String,
        // String,
        // String,
        // String,
        // String,
        // String,
        u64,
        u64,
        u64,
        String,
    );

    fn gen_batch<C: Col<Self::Data>>(&mut self, idx: usize, batch: &mut C) {
        if idx >= self.num_batches() {
            return;
        }

        let mut rng = SmallRng::seed_from_u64(idx as u64);

        let s_id = idx as u64;
        let s_w_id = s_id / NUM_STOCK_PER_WAREHOUSE as u64;
        let s_quantity = rand_int(&mut rng, 10, 100) as u64;
        let s_dist_01 = reuse(&mut self.s_dist_01, |x| {
            rand_string(&mut rng, A_STRING_ALPHABET, 24, x)
        });
        let s_dist_02 = reuse(&mut self.s_dist_02, |x| {
            rand_string(&mut rng, A_STRING_ALPHABET, 24, x)
        });
        let s_dist_03 = reuse(&mut self.s_dist_03, |x| {
            rand_string(&mut rng, A_STRING_ALPHABET, 24, x)
        });
        let s_dist_04 = reuse(&mut self.s_dist_04, |x| {
            rand_string(&mut rng, A_STRING_ALPHABET, 24, x)
        });
        let s_dist_05 = reuse(&mut self.s_dist_05, |x| {
            rand_string(&mut rng, A_STRING_ALPHABET, 24, x)
        });
        let s_dist_06 = reuse(&mut self.s_dist_06, |x| {
            rand_string(&mut rng, A_STRING_ALPHABET, 24, x)
        });
        let s_dist_07 = reuse(&mut self.s_dist_07, |x| {
            rand_string(&mut rng, A_STRING_ALPHABET, 24, x)
        });
        let s_dist_08 = reuse(&mut self.s_dist_08, |x| {
            rand_string(&mut rng, A_STRING_ALPHABET, 24, x)
        });
        let s_dist_09 = reuse(&mut self.s_dist_09, |x| {
            rand_string(&mut rng, A_STRING_ALPHABET, 24, x)
        });
        let s_dist_10 = reuse(&mut self.s_dist_10, |x| {
            rand_string(&mut rng, A_STRING_ALPHABET, 24, x)
        });
        let s_ytd = 0;
        let s_order_cnt = 0;
        let s_remote_cnt = 0;
        let s_data = reuse(&mut self.s_data, |x| {
            rand_original_string(&mut rng, x);
        });
        batch.push((
            s_id,
            s_w_id,
            s_quantity,
            s_dist_01,
            s_dist_02,
            s_dist_03,
            s_dist_04,
            s_dist_05,
            // s_dist_06,
            // s_dist_07,
            // s_dist_08,
            // s_dist_09,
            // s_dist_10,
            s_ytd,
            s_order_cnt,
            s_remote_cnt,
            s_data,
        ));
        std::hint::black_box((s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10));
    }
}

/// The TPCC DISTRICT table.
#[derive(Debug, Clone)]
pub struct District {
    config: TpccConfig,

    // For allocation reuse
    d_name: String,
    d_street_1: String,
    d_street_2: String,
    d_city: String,
    d_state: String,
    d_zip: String,
}

impl District {
    /// Construct an instance of this table with the given configuration.
    pub fn init(config: TpccConfig) -> Self {
        District {
            config,
            d_name: String::with_capacity(10),
            d_street_1: String::with_capacity(20),
            d_street_2: String::with_capacity(20),
            d_city: String::with_capacity(20),
            d_state: String::with_capacity(2),
            d_zip: String::with_capacity(9),
        }
    }
}

impl DynTable for District {
    fn name(&self) -> &'static str {
        "district"
    }

    fn num_batches(&self) -> usize {
        self.config.warehouses * NUM_DISTRICTS_PER_WAREHOUSE
    }
}

impl Table for District {
    type Data = (
        u64,
        u64,
        String,
        String,
        String,
        String,
        String,
        String,
        f64,
        f64,
        u64,
    );

    fn gen_batch<C: Col<Self::Data>>(&mut self, idx: usize, batch: &mut C) {
        if idx >= self.num_batches() {
            return;
        }

        let mut rng = SmallRng::seed_from_u64(idx as u64);

        let d_id = idx as u64;
        let d_w_id = d_id / NUM_DISTRICTS_PER_WAREHOUSE as u64;
        let d_name = reuse(&mut self.d_name, |x| {
            rand_string_len(&mut rng, A_STRING_ALPHABET, 6, 10, x)
        });
        let d_street_1 = reuse(&mut self.d_street_1, |x| {
            rand_string_len(&mut rng, A_STRING_ALPHABET, 10, 20, x)
        });
        let d_street_2 = reuse(&mut self.d_street_2, |x| {
            rand_string_len(&mut rng, A_STRING_ALPHABET, 10, 20, x)
        });
        let d_city = reuse(&mut self.d_city, |x| {
            rand_string_len(&mut rng, A_STRING_ALPHABET, 10, 20, x)
        });
        let d_state = reuse(&mut self.d_state, |x| rand_state(&mut rng, x));
        let d_zip = reuse(&mut self.d_zip, |x| rand_zip(&mut rng, x));
        let d_tax = rand_tax(&mut rng);
        let d_ytd = INITIAL_YTD;
        let d_next_o_id = NUM_ORDERS_PER_DISTRICT as u64 + 1;
        batch.push((
            d_id,
            d_w_id,
            d_name,
            d_street_1,
            d_street_2,
            d_city,
            d_state,
            d_zip,
            d_tax,
            d_ytd,
            d_next_o_id,
        ));
    }
}

/// The TPCC CUSTOMER table.
#[derive(Debug, Clone)]
pub struct Customer {
    config: TpccConfig,

    // For allocation reuse
    c_first: String,
    c_street_1: String,
    c_street_2: String,
    c_city: String,
    c_state: String,
    c_zip: String,
    c_phone: String,
    c_data: String,
}

impl Customer {
    /// Construct an instance of this table with the given configuration.
    pub fn init(config: TpccConfig) -> Self {
        Customer {
            config,
            c_first: String::with_capacity(16),
            c_street_1: String::with_capacity(20),
            c_street_2: String::with_capacity(20),
            c_city: String::with_capacity(20),
            c_state: String::with_capacity(2),
            c_zip: String::with_capacity(9),
            c_phone: String::with_capacity(16),
            c_data: String::with_capacity(500),
        }
    }
}

impl DynTable for Customer {
    fn name(&self) -> &'static str {
        "customer"
    }

    fn num_batches(&self) -> usize {
        self.config.warehouses * NUM_DISTRICTS_PER_WAREHOUSE * NUM_CUSTOMERS_PER_DISTRICT
    }
}

impl Table for Customer {
    type Data = (
        u64,
        u64,
        u64,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        // String,
        // DateTime,
        // String,
        // f64,
        // f64,
        // f64,
        // f64,
        // u64,
        // u64,
        String,
    );

    fn gen_batch<C: Col<Self::Data>>(&mut self, idx: usize, batch: &mut C) {
        if idx >= self.num_batches() {
            return;
        }

        let mut rng = SmallRng::seed_from_u64(idx as u64);

        let c_id = idx as u64;
        let c_d_id = c_id / NUM_CUSTOMERS_PER_DISTRICT as u64;
        let c_w_id = c_d_id / NUM_DISTRICTS_PER_WAREHOUSE as u64;
        let c_last = "TODO";
        let c_middle = "OE";
        let c_first = reuse(&mut self.c_first, |x| {
            rand_string_len(&mut rng, A_STRING_ALPHABET, 8, 16, x)
        });
        let c_street_1 = reuse(&mut self.c_street_1, |x| {
            rand_string_len(&mut rng, A_STRING_ALPHABET, 10, 20, x)
        });
        let c_street_2 = reuse(&mut self.c_street_2, |x| {
            rand_string_len(&mut rng, A_STRING_ALPHABET, 10, 20, x)
        });
        let c_city = reuse(&mut self.c_city, |x| {
            rand_string_len(&mut rng, A_STRING_ALPHABET, 10, 20, x)
        });
        let c_state = reuse(&mut self.c_state, |x| rand_state(&mut rng, x));
        let c_zip = reuse(&mut self.c_zip, |x| rand_zip(&mut rng, x));
        let c_phone = reuse(&mut self.c_phone, |x| {
            rand_string(&mut rng, N_STRING_ALPHABET, 16, x)
        });
        let c_since = self.config.now;
        let c_credit = if rng.gen_range(0..10) == 0 {
            "BC"
        } else {
            "GC"
        };
        let c_credit_lim = 50_000.0;
        let c_discount = rng.gen_range(0.0..0.5);
        let c_balance = -10.0;
        let c_ytd_payment = 10.0;
        let c_payment_cnt = 1;
        let c_delivery_cnt = 0;
        let c_data = reuse(&mut self.c_data, |x| {
            rand_string_len(&mut rng, A_STRING_ALPHABET, 300, 500, x)
        });

        batch.push((
            c_id, c_d_id, c_w_id, c_last, c_middle, c_first, c_street_1, c_street_2, c_city,
            c_state, c_zip,
            // c_phone,
            // c_since,
            // c_credit,
            // c_credit_lim,
            // c_discount,
            // c_balance,
            // c_ytd_payment,
            // c_payment_cnt,
            // c_delivery_cnt,
            c_data,
        ));
        std::hint::black_box((
            c_phone,
            c_since,
            c_credit,
            c_credit_lim,
            c_discount,
            c_balance,
            c_ytd_payment,
            c_payment_cnt,
            c_delivery_cnt,
        ));
    }
}

/// The TPCC HISTORY table.
#[derive(Debug, Clone)]
pub struct History {
    config: TpccConfig,

    // For allocation reuse
    h_data: String,
}

impl History {
    /// Construct an instance of this table with the given configuration.
    pub fn init(config: TpccConfig) -> Self {
        History {
            config,
            h_data: String::with_capacity(24),
        }
    }
}

impl DynTable for History {
    fn name(&self) -> &'static str {
        "history"
    }

    fn num_batches(&self) -> usize {
        self.config.warehouses
            * NUM_DISTRICTS_PER_WAREHOUSE
            * NUM_CUSTOMERS_PER_DISTRICT
            * NUM_HISTORY_PER_CUSTOMER
    }
}

impl Table for History {
    type Data = (u64, u64, u64, u64, u64, DateTime, f64, String);

    fn gen_batch<C: Col<Self::Data>>(&mut self, idx: usize, batch: &mut C) {
        if idx >= self.num_batches() {
            return;
        }

        let mut rng = SmallRng::seed_from_u64(idx as u64);

        let h_c_id = idx as u64;
        let h_c_d_id = h_c_id / NUM_CUSTOMERS_PER_DISTRICT as u64;
        let h_c_w_id = h_c_d_id / NUM_DISTRICTS_PER_WAREHOUSE as u64;
        let h_d_id = h_c_d_id;
        let h_w_id = h_c_w_id;
        let h_date = self.config.now;
        let h_amount = 10.00;
        let h_data = reuse(&mut self.h_data, |x| {
            rand_string_len(&mut rng, A_STRING_ALPHABET, 12, 24, x)
        });
        batch.push((
            h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data,
        ));
    }
}

/// The TPCC ORDER table.
#[derive(Debug, Clone)]
pub struct Order {
    config: TpccConfig,

    // For allocation reuse
    o_c_ids: Vec<u64>,
}

impl Order {
    /// Construct an instance of this table with the given configuration.
    pub fn init(config: TpccConfig) -> Self {
        Order {
            config,
            o_c_ids: Vec::new(),
        }
    }
}

impl DynTable for Order {
    fn name(&self) -> &'static str {
        "order"
    }

    fn num_batches(&self) -> usize {
        self.config.warehouses * NUM_DISTRICTS_PER_WAREHOUSE
    }
}

impl Table for Order {
    type Data = (u64, u64, u64, u64, DateTime, Option<u64>, u64, u64);

    fn gen_batch<C: Col<Self::Data>>(&mut self, idx: usize, batch: &mut C) {
        if idx >= self.num_batches() {
            return;
        }

        let mut rng = SmallRng::seed_from_u64(idx as u64);

        let o_d_id = idx as u64;
        let o_w_id = o_d_id / NUM_DISTRICTS_PER_WAREHOUSE as u64;
        let o_entry_d = self.config.now;

        self.o_c_ids.clear();
        self.o_c_ids.extend(0..NUM_ORDERS_PER_DISTRICT as u64);
        self.o_c_ids.shuffle(&mut rng);

        for idx in 0..NUM_ORDERS_PER_DISTRICT {
            let o_id = idx as u64;
            let o_c_id = self.o_c_ids[idx];
            let o_carrier_id = if o_id < 2_001 {
                Some(rand_int(&mut rng, 1, 10) as u64)
            } else {
                None
            };
            let o_ol_cnt = rand_int(&mut rng, 5, 15) as u64;
            let o_all_local = 1;

            batch.push((
                o_id,
                o_c_id,
                o_d_id,
                o_w_id,
                o_entry_d,
                o_carrier_id,
                o_ol_cnt,
                o_all_local,
            ));
        }
    }
}

/// The TPCC ORDER-LINE table.
#[derive(Debug, Clone)]
pub struct OrderLine {
    config: TpccConfig,

    // For allocation reuse
    ol_dist_info: String,
}

impl OrderLine {
    /// Construct an instance of this table with the given configuration.
    pub fn init(config: TpccConfig) -> Self {
        OrderLine {
            config,
            ol_dist_info: String::with_capacity(24),
        }
    }
}

impl DynTable for OrderLine {
    fn name(&self) -> &'static str {
        "order-line"
    }

    fn num_batches(&self) -> usize {
        self.config.warehouses * NUM_DISTRICTS_PER_WAREHOUSE * NUM_ORDERS_PER_DISTRICT
    }
}

impl Table for OrderLine {
    type Data = (
        u64,
        u64,
        u64,
        u64,
        u64,
        u64,
        Option<DateTime>,
        u64,
        f64,
        String,
    );

    fn gen_batch<C: Col<Self::Data>>(&mut self, idx: usize, batch: &mut C) {
        if idx >= self.num_batches() {
            return;
        }

        let mut rng = SmallRng::seed_from_u64(idx as u64);

        let ol_o_id = idx as u64;
        let ol_d_id = ol_o_id / NUM_ORDERS_PER_DISTRICT as u64;
        let ol_w_id = ol_d_id / NUM_DISTRICTS_PER_WAREHOUSE as u64;
        let ol_supply_w_id = ol_w_id;
        let ol_quantity = 5;

        // TODO: Make this match the order.
        let o_ol_cnt = rand_int(&mut rng, 5, 15);
        for idx in 0..o_ol_cnt {
            let ol_number = idx as u64;
            let ol_i_id = rand_int(&mut rng, 1, 100_000) as u64;
            let ol_delivery_id = if ol_o_id < 2_101 {
                Some(self.config.now)
            } else {
                None
            };
            let ol_amount = if ol_o_id < 2_101 {
                rng.gen_range(0.01..9_999.99)
            } else {
                0.0
            };
            let ol_dist_info = reuse(&mut self.ol_dist_info, |x| {
                rand_string(&mut rng, A_STRING_ALPHABET, 24, x)
            });
            batch.push((
                ol_o_id,
                ol_d_id,
                ol_w_id,
                ol_number,
                ol_i_id,
                ol_supply_w_id,
                ol_delivery_id,
                ol_quantity,
                ol_amount,
                ol_dist_info,
            ));
        }
    }
}

/// The TPCC NEW-ORDER table.
#[derive(Debug, Clone)]
pub struct NewOrder {
    config: TpccConfig,
}

impl NewOrder {
    /// Construct an instance of this table with the given configuration.
    pub fn init(config: TpccConfig) -> Self {
        NewOrder { config }
    }
}

impl DynTable for NewOrder {
    fn name(&self) -> &'static str {
        "new-order"
    }

    fn num_batches(&self) -> usize {
        self.config.warehouses * NUM_DISTRICTS_PER_WAREHOUSE * NUM_NEW_ORDERS_PER_DISTRICT
    }
}

impl Table for NewOrder {
    type Data = (u64, u64, u64);

    fn gen_batch<C: Col<Self::Data>>(&mut self, idx: usize, batch: &mut C) {
        if idx >= self.num_batches() {
            return;
        }

        let offset = (NUM_ORDERS_PER_DISTRICT - NUM_NEW_ORDERS_PER_DISTRICT) as u64;
        let no_o_id = offset + (idx as u64 % NUM_NEW_ORDERS_PER_DISTRICT as u64);
        let no_d_id = idx as u64 / NUM_NEW_ORDERS_PER_DISTRICT as u64;
        let no_w_id = no_d_id / NUM_DISTRICTS_PER_WAREHOUSE as u64;

        batch.push((no_o_id, no_d_id, no_w_id));
    }
}

/// Returns a number within [min, max] inclusive. See 2.1.4.
fn rand_int<R: rand::Rng>(rng: &mut R, min: usize, max: usize) -> usize {
    rng.gen_range(min..=max)
}

/// Appends a random US state. Spec just says 2 letters.
fn rand_state<R: rand::Rng>(rng: &mut R, x: &mut String) {
    rand_string(rng, A_STRING_ALPHABET, 2, x)
}

/// Appends a random "zip code" - a 4-digit number plus the constant "11111". See
/// 4.3.2.7.
fn rand_zip<R: rand::Rng>(rng: &mut R, x: &mut String) {
    rand_string(rng, N_STRING_ALPHABET, 4, x);
    x.push_str("11111");
}

/// Returns a random tax between [0.0000..0.2000]. See 2.1.5.
fn rand_tax<R: rand::Rng>(rng: &mut R) -> f64 {
    return rand_int(rng, 0, 2000) as f64 / 10000.0;
}

// rand_original_string appends a random alphanumeric string with a 10% chance
// of containing the string "ORIGINAL" somewhere in the middle of the string.
// See 4.3.3.1.
fn rand_original_string<R: rand::Rng>(rng: &mut R, x: &mut String) {
    const ORIGINAL: &str = "ORIGINAL";
    let len = rand_int(rng, 26, 50);
    if rng.gen_range(0..10) == 0 {
        let before = rng.gen_range(0..len - ORIGINAL.len());
        let after = len - before - ORIGINAL.len();
        debug_assert_eq!(before + ORIGINAL.len() + after, len);
        rand_string(rng, A_STRING_ALPHABET, before, x);
        x.push_str(ORIGINAL);
        rand_string(rng, A_STRING_ALPHABET, after, x);
    } else {
        rand_string(rng, A_STRING_ALPHABET, len, x);
    }
}

/// Appends a random string of length `[min_len..max_len]` from the given
/// alphabet.
fn rand_string_len<R: rand::Rng>(
    rng: &mut R,
    alphabet: &[char],
    min_len: usize,
    max_len: usize,
    x: &mut String,
) {
    if min_len == max_len {
        rand_string(rng, alphabet, min_len, x);
    } else {
        let len = rand_int(rng, min_len, max_len);
        rand_string(rng, alphabet, len, x);
    }
}

/// Appends a random string of length `len` from the given alphabet.
fn rand_string<R: rand::Rng>(rng: &mut R, alphabet: &[char], len: usize, x: &mut String) {
    for _ in 0..len {
        x.push(alphabet[rng.gen_range(0..alphabet.len())]);
    }
}

fn reuse<'a, F>(x: &'a mut String, f: F) -> &'a str
where
    F: FnOnce(&mut String),
{
    x.clear();
    f(x);
    x.as_str()
}

impl Data for DateTime {
    type Ref<'a> = DateTime;
    type Col = Vec<u64>;
}

impl Col<DateTime> for Vec<u64> {
    fn len(&self) -> usize {
        self.len()
    }

    fn get<'a>(&'a self, idx: usize) -> <DateTime as Data>::Ref<'a> {
        let x = self[idx].to_le_bytes();
        let mut date = [0u8; 4];
        let mut time = [0u8; 4];
        date.copy_from_slice(&x[0..4]);
        time.copy_from_slice(&x[4..8]);
        DateTime {
            date: u32::from_le_bytes(date),
            time: u32::from_le_bytes(time),
        }
    }

    fn push(&mut self, t: <DateTime as Data>::Ref<'_>) {
        let mut x = [0u8; 8];
        x[0..4].copy_from_slice(&t.date.to_le_bytes());
        x[4..8].copy_from_slice(&t.time.to_le_bytes());
        self.push(u64::from_le_bytes(x));
    }

    fn clear(&mut self) {
        self.clear()
    }

    fn good_bytes(&self) -> usize {
        self.len() * std::mem::size_of::<u64>()
    }
}

// TODO: Hacks
impl From<DateTime> for u64 {
    fn from(value: DateTime) -> Self {
        ((value.date as u64) << 32) + value.time as u64
    }
}

#[cfg(test)]
mod tests {
    use crate::col::Data;

    use super::*;

    const ITEM_EXPECTED: &str = r"
0,4474,36KCyYo8Nd5nosWSKlp,86.6,8cYB0av5swrQzbPWviBQwuAYNgf5lUlik7F
1,1819,7gzsS2KDKllWMEoYRkt,69.45,9D5kSdAWfHUOAB2Gl3LwFECk7qU3hiWBlI0bp858oClbgOmYa
2,7525,2vKZhcDjzacLvE6M,1.58,HbVdRbBRLrsUp3DhyPdHhJcoYtp35aAKujayvYTGQLL9T2v1G
3,8297,jjEQL74tbVro5HcPFI,50.7,LBbteuf5ojxhpiMaCzsZJpZXGcUFPwnt8GRiwg2
4,6376,8UlUTTvloLlDClBPr,24.21,jBQU31gguFTKd1d2kEzFptORIGINALPyY1eO
5,7328,GUVwvlyCHTZhVFk4mPC2LN,70.05,2W62CDCewC6vYLnuORIGINALLnRwJ
6,8973,sMvn1jd5VK8jK9Ni7umSYnrp,3.3,mOzeedy9wAzlu79UHYLlvermqUP5AIAeRuPI443rKD
7,3140,VGglQvwCvy1dFUNrvW,14.22,0TPu96ClKUlXmMLMAEikD5HRAR8wu5Y2ScmRJVrqGVo5mN5e
8,4536,kvYqRPNG6nXiLoLLm,64.03,5mZndv0q5oCcLLavZXet9BSBMrNdL
9,9304,rxezJzS09P4gPZ,11.33,x7NnnKv33ihS2dt3nYp8rEc3LIoU
";

    const WAREHOUSE_EXPECTED: &str = r"
0,B8C36KCy,o8Nd5nosWSKlp1y38cY,av5swrQzbPWviBQwuAY,f5lUlik7FLM,Ec,160811111,0.165,300000.0
";

    const STOCK_EXPECTED: &str = r"
0,0,50,B8C36KCyYo8Nd5nosWSKlp1y,38cYB0av5swrQzbPWviBQwuA,YNgf5lUlik7FLMEchyOd0ZyA,1W66M9UHzjFp8pT1VwvZvqQ3,WQTxLfCinHti9W2Gjn6E6FfO,0,0,0,wraprwZeyMtcyxC926RhPhp8eJ32UKwNkxgqjPHLeLcWV2x
1,0,13,lvD7gzsS2KDKllWMEoYRktQ5,fu9D5kSdAWfHUOAB2Gl3LwFE,Ck7qU3hiWBlI0bp858oClbgO,mYa1aB1AnbvDA9VKRKEk2kWm,BUTue9JKcQpSqsByKKNEkF9b,0,0,0,hU9YrhMQikeZo8LpieGmPonpKllz
2,0,78,n2vKZhcDjzacLvE6MhKa6mHb,VdRbBRLrsUp3DhyPdHhJcoYt,p35aAKujayvYTGQLL9T2v1G6,lSXd72FUY1wuChJFZ6RPX7PH,4qmt0uC6gLzPPNBvdvZQ9UtS,0,0,0,Rh0NsLzAGF6DHdHQRnGuLfklcyZ77ItEtLH
3,0,55,ZzjjEQL74tbVro5HcPFIFmHL,Bbteuf5ojxhpiMaCzsZJpZXG,cUFPwnt8GRiwg2DD64I6just,e7Ua9zmnMOy2wjWD2TdAEIjw,aVkPhhGq9zdGEVdMfyXvhdtN,0,0,0,CtwSKkyFaMzXbZHzohQfRY871LZ
4,0,68,s8UlUTTvloLlDClBPrgoAaJY,jBQU31gguFTKd1d2kEzFptPy,Y1eOnd2DFOVpYDQuTT8Z4pS0,1hBHkKo2wpLWS7Zr0VxR6FJS,lJJnmNMyV1519ppMx22m7u5y,0,0,0,J9PnRK3mLsuaaqG3BOM2uUjvr74E3ZtjjtrfiFeSXfW93s1
5,0,76,RSvMwUGUVwvlyCHTZhVFk4mP,C2LNRidV2W62CDCewC6vYLnu,LnRwJ3Fum2KVdzN4TidDdaOL,9duEjx0JqFZ4xNieK3oPQoES,ne6YjO1kmQ7KOEtxlGbcDWOu,0,0,0,XNieVQBrgbB9PIGmEtbNh4eefHvB91leDIiHKqDo
6,0,91,8sMvn1jd5VK8jK9Ni7umSYnr,pB7AbPpmOzeedy9wAzlu79UH,YLlvermqUP5AIAeRuPI443rK,DZDgvJUDzNExDMb9IoU1tZRB,c4BoB8mFSoSn7M1Emyllrs18,0,0,0,rD0YPgpXJ89TAJp9iLmORIGINALrbI0WPmHxoowN
7,0,98,tBqzVGglQvwCvy1dFUNrvWhi,93z0TPu96ClKUlXmMLMAEikD,5HRAR8wu5Y2ScmRJVrqGVo5m,N5eETrVjM3giLrkHaGfCxHwV,46ZpHIYmUfyFM6YVqP3gJSE3,0,0,0,LHpXDxcPcvblgakURqop96sxJv7rHDw2iQbo2s0Y904h
8,0,51,ukvYqRPNG6nXiLoLLmNh5mZn,dv0q5oCcLLavZXet9BSBMrNd,LJ6uFQUc6x1bCWDrIWgPabtA,fKZTnpMLLJ1tzNIfJDwdOBq3,64susL6xxAP8klRTT3AeOpQr,0,0,0,fO124sUxQwUsZxex23MWkIwToDx7PtfCr
9,0,94,drxezJzS09P4gPZsngfJ1x7N,nnKv33ihS2dt3nYp8rEc3LIo,U3Gj1YBj7RLKs9NgedUWtTgE,iZcVGOj70c9jOu00xav0T6sj,nN2Qu9Xpyru4T4GOtX4B0CIb,0,0,0,AfITIqfvdBKArVcJcerAEPsgrHMY
";

    const DISTRICT_EXPECTED: &str = r"
0,0,B8C36KCy,o8Nd5nosWSKlp1y38cY,av5swrQzbPWviBQwuAY,f5lUlik7FLM,Ec,160811111,0.165,300000.0,3001
1,0,clvD7gzsS,2KDKllWMEoYRktQ5,u9D5kSdAWfH,OAB2Gl3LwFECk7qU3h,iW,458011111,0.0497,300000.0,3001
2,0,2vKZhcD,zacLvE6MhKa,mHbVdRbBRLrsUp3DhyPd,JcoYtp35aAK,uj,038711111,0.104,300000.0,3001
3,0,ZzjjEQL7,tbVro5HcPFIFmHLBbteu,5ojxhpiMaCzsZJpZXGcU,Pwnt8GRiwg2DD64,I6,133311111,0.0146,300000.0,3001
4,0,s8UlUTTvl,LlDClBPrgoAa,YjBQU31gguFTKd1d,2kEzFptPyY1eO,nd,562811111,0.0963,300000.0,3001
5,0,SvMwUGUVw,CHTZhVFk4mPC2L,idV2W62CDCewC6vYL,uLnRwJ3Fum2K,Vd,469711111,0.1225,300000.0,3001
6,0,8sMvn1jd5V,8jK9Ni7umSYnrpB7,bPpmOzeedy9wAz,lu79UHYLlv,er,794511111,0.0862,300000.0,3001
7,0,BqzVGgl,vwCvy1dFUNrvWhi93,0TPu96ClKUlXmM,MAEikD5HRAR8wu5Y,2S,072511111,0.1519,300000.0,3001
8,0,ukvYqRPN,6nXiLoLLmNh5mZn,v0q5oCcLLa,ZXet9BSBMrNdLJ6uFQU,c6,805111111,0.1345,300000.0,3001
9,0,5drxez,zS09P4gPZsngfJ1x,NnnKv33ihS2dt3nYp8rE,3LIoU3Gj1Y,Bj,636111111,0.0135,300000.0,3001
";

    const CUSTOMER_EXPECTED: &str = r"
0,0,0,TODO,OE,B8C36KCyYo8N,5nosWSKlp1,38cYB0av5swrQz,PWviBQwuAY,Ng,917711111,zjFp8pT1VwvZvqQ3WQTxLfCinHti9W2Gjn6E6FfOyS45mclf2SdR2VE74XhqI7H5qbx3QXhIEF3TCOTqSrkfnpS39JBXj8yvQUR01qjsVQPe9OqETxOFniELGm1QxpZ458gjEwZe4PXray0VaJvLIwJwTsNA2hGK1VwraprwZeyMtcyxC926RhPhp8eJ32UKwNkxgqjPHLeLcWV2xOQ3c26yJyc1gpTKjO7TLspvMulCx1QgX1eWhAQrmFY0NTQf9CHFCGoHXDqprpjNxk4rQFpvHeCFkzL0obtjYpNHuSB9vmOlDG4Cg0CLIGcSLKQOiPEocxwYWsN6bJDv8YiOHKWtkm9eCW6HgMd3EXIlwYnxX28to85WvwrKMptdY4LI5iRYqVVNuJYZooQBbKVXtls
1,0,0,TODO,OE,clvD7gzsS2KDKl,MEoYRktQ5fu9D5kSdA,fHUOAB2Gl3LwFECk7q,3hiWBlI0bp858oClbg,Om,808011111,ByKKNEkF9bTAvOIXcEcevimYbtLGjFr0u22aYxm0dLcWf0nA3N7omkH1d0TITXmFMu8GAFYj54wXooxJANI8SaUBXUzBI0m08cs5n7971KT9lsCC2xUDDSSNPskaXdd6JpIg6hU9YrhMQikeZo8LpieGmPonpKllzEtEcaRQJSYFswbm09w8tuSuIwzFFW2DvkiZBrRclEGDcqgkJs1Jdd3bpdUVgpblAzJqLzRzV2oiMiAXGxz0oqWjGfcn0qX4DIlVlg6VUxR5fOKYOBEWcKu7k2Hg3AiPFb654qcHKV8zVWqXcArtsd6PlTBMoXr3VP3aoD01kf7jAsPb8THJ5tVQOki3AECrijMADF
2,0,0,TODO,OE,2vKZhcDjza,LvE6MhKa6m,VdRbBRLrsU,hyPdHhJcoYtp35a,AK,310311111,uChJFZ6RPX7PH4qmt0uC6gLzPPNBvdvZQ9UtSF5PdHHqUxIOEQhhPBIx7dUCpuNNpSQ09RTQJMcnPcyjmQ1Vq77eg5IbjpxKH8V9AJ0cm3aHqojTS2T85nhzK5sMCvNDDj8ensgVAyympZGHyWqCqyeOVZ4KuwyoRh0NsLzAGF6DHdHQRnGuLfklcyZ77ItEtLHRL9AH2vPyHAbZcfbtUpQYyoU3LuqRTEDGC7l1HRE0m2cYgPiNTcL637Ln4BXNc9Zb7hk5WSVV0AYZ3rLB2wCs2t6yNyiLzv1PSWCvecu0gkSt4Ggvjr8otegp9iSQfT16THQmKegXQqaRQbmajHr2yLp662AMgWZmg09Mq6dQjcBZzARaTO
3,0,0,TODO,OE,ZzjjEQL74tbV,o5HcPFIFmHLBb,euf5ojxhpiMaC,sZJpZXGcUFPwnt,8G,711911111,EIjwaVkPhhGq9zdGEVdMfyXvhdtNHTHB2LwGm5tc5oL1P4laV5ouxlYuSLQdYOrnD7FbL9YL9DB2C2ATmngGTjqa9cOwIaO79WkHGbJX5gnnAPuevywPBc5IM2gyrQL38RTtilVd7LXM716FxUt2e1LCtwSKkyFaMzXbZHzohQfRY871LZzaKAUINJoDpigjEGY0CnywS3H6sX1oaLEq7WAvkVKfEUpu6NGNosjMEWLIiIsCr3eEOHGlZrJ0rKErmrApIwIaBU3MyLrqLDIeysH7iPav5GoABro6ZI9YiurziNaG8EVC5XIZkdEOV7520TebvQQRfZSE4bdI4TbJQcThioq8QW8feZRZxdvZkM0plSZlf2lXoJTiFeDPJG5U5IQ
4,0,0,TODO,OE,loLlDClBPrg,AaJYjBQU31gg,FTKd1d2kEzFpt,yY1eOnd2DFOVpYDQu,TT,892711111,JnmNMyV1519ppMx22m7u5ypMArRXISVyQUbuGHHsdz7jGYjQ2FbhUzg0jrEkX00PK5I9Pk773jJEg7aCTFngXsyuGIhLDndKfI3ttAZIngi2xvhcNcdiW3qmcCaQItJQ2XUYCagncv3vFX1PfnJ9PnRK3mLsuaaqG3BOM2uUjvr74E3ZtjjtrfiFeSXfW93s1QKpTZIyA07TeqgfYHuAn2CRnSIWI7Dw0eaPtP75lrBSYIMxFFkqg7hsYi1zrN9wRrm0XKn3TKrxIshPRjXy9uU3CPeTcrKNzd1p4hcH5VLjQ2syDizVBk4tY5DljRGlgKFw0jAyZUsJYKAicMWEsENOgnMmygvVaQTSjNnP7k03cbeO3IVGIJPZaOTY4dqdDzrumMtZAf6gAYuLyNhQ15gw9gvc5
5,0,0,TODO,OE,SvMwUGUVwvlyCH,hVFk4mPC2LNRidV2W62,DCewC6vYLnuLnRw,3Fum2KVdzN4TidDd,aO,603111111,EtxlGbcDWOuV5myu4dx2kCUaWlh3qkRHW2mXuVfL6jQgmhW4smROo6uPftvDEytRL5S6QGgbJe66Z6C1kR4xXVHBvd9moIKgw7EBqe92Owo1G31vZUNJ7c2t1OnXbEtuq9UKqXNieVQBrgbB9PIGmEtbNh4eefHvB91leDIiHKqDoJxxJR7Z5nNA62hi571uAzr2IcTOusVYDMLb141hUe8ZkqFTsmEJ5ky2hBNbaq6jGG60lhlT8lhq5N00IW5TztiE31ET7yUYJmp9SZ9hVc91MozPfDOCeDrCrClqjjmwssfA2eT8MOYroStMoIYJl5D9pZWLNQ0d9bXl09ryfrRlKFvul2o69HQ1a0Yy3ed2QM17d1vGBcBk1hJ0TnCcbGW13hYVGQ4WH0XgrAJNnmwKwWrQzGJ23QbKbUBafFi8HK9
6,0,0,TODO,OE,8sMvn1jd5VK8jK9N,7umSYnrpB7A,PpmOzeedy9,zlu79UHYLlverm,qU,945411111,RBc4BoB8mFSoSn7M1Emyllrs187hAFnGJRpYm5qDYKHD4be0a1EqT6yUqPivJL00kr6zZ78TiByQbqW1u5RXdOUELmtAPoJnpgdXJZcFUDeANVzUV585nFWgLyzSzqgLmdj4of6kcoUFl5G1QcKaLrD0YPgpXJ89TAJp9iLmrbI0WPmHxoowNoJFNcgMj8NjKBa2mSJio8HDs1LLAcW1EuKUbGPQgXWbYblhgrHRAzM4F0PpAXksKdU5e9eIef5K6JTYdXBvjFDhvVEFTZlzAkGSnSWJlnEl7FP8OZRh7FsTktPKRL6uiUEQm4rzJkkyO4wUJC3JsNTn0gKwMN4eu2EcP98gRsv2rYsvCzIUHubm87TUD1AsjN3lI4Sa9WUzUkjKvO9mBkm9j5pEJu2MftR1fPHBaw28rndLqzWeZlzd5eh8oG2bP5j7CR7n1E7tdlCwZnnZn094gXHd6W
7,0,0,TODO,OE,zVGglQvwCv,1dFUNrvWhi93z0,Pu96ClKUlXmMLMAEik,5HRAR8wu5Y2ScmR,JV,257211111,pHIYmUfyFM6YVqP3gJSE3v5MeUB4vhBLGryVQxcPDXwzKamOChm3yjhtCN234fLX9r31q8xOXViRiSFWcRtVcqwpwoIhEyoPdedR9U3YIRXVYCEUXKfMg0MPlIZtU99H6kKLiodjagHdMU8iLHpXDxcPcvblgakURqop96sxJv7rHDw2iQbo2s0Y904hmGnIhBl1C7qkGkgkjXh1SaHmEJe0hX7HJcymWwFsxCtkIr2zcqPCrqf98dhru9avVz9v8wUdlQxyt93BzHdyzv25iDT2bT76YY4wCBLvWWIsq8HF35bAVz70PWBWuIIsG7ajlAzLPrrbNyaRLlQdd7SSzgSrHvqnFnfdlgBnifFEy5zCr3dvFc5lqSd3nOHvqtGaqLKFOKGvGglL236VPJeEmROgy9tFqwO1JNh50prY5rt5ykxgiAFnxnf5hzBGfaqvUTNDY9dcaXmgzy70sI
8,0,0,TODO,OE,ukvYqRPNG6nX,LoLLmNh5mZn,v0q5oCcLLa,ZXet9BSBMrNdLJ6uFQU,c6,805111111,64susL6xxAP8klRTT3AeOpQr55EZZSMN3KBB8UPsqoNhD3l9zEY4jX0CIil74gs07hfz4dy9S9Pj3DtXPas43cU8yEE8hXKFieH5Embcv1pNyWZcivxfCJAo7m3QPtl5X3RkZIFpYNh4wC8etkfO124sUxQwUsZxex23MWkIwToDx7PtfCriliGNDeDjNTtdCQaHVephWcstpCL3EvmVEnEOUyje3wHFPnl45cECWdttPTLrnQ7T6FdPICcaQmIOWimXdjS6lD169foDWD8MeJljuztXtW6cJAU6MYnZAwvtdovGndP97TBrSdeBNV1VydS51B5bWiU8o8GrP1yS0T1hfzqjbOhNHBXgVDc8gvyz8SZZdUwSWh9HdLVpRuotUhVD8NHP6nwPk94ow8kgKmHKiuPm7l48YQSPUPGlH7OMXLkgqnsedLD8DZiLfzRP0EYdmri4Kj68AcNrmHFEslUkPVybHa
9,0,0,TODO,OE,5drxezJz,9P4gPZsngfJ1x7NnnKv,hS2dt3nYp8r,c3LIoU3Gj1YBj7R,LK,361011111,xav0T6sjnN2Qu9Xpyru4T4GOtX4B0CIbR1LETQvgwJh6aVUvHRyBMknjqWGuarKWjlK1hZLu5TFoSdtXfcoMQDZ22kuqjmCHsNDY2YgOEXMuiI6Jcqulin7Om3rdtgMT4P8kPX3XcMSxgCjY53umYeqifuAfITIqfvdBKArVcJcerAEPsgrHMYKkDNVrpMS0AcNESHHjIMV7XeQ1dFM6WccVJnkRqq5LmYRWQle3V6hj1HrumuZUAWtLTZaXHMmCvobB6YIaPuCHYBkIl4nkVjCz8UZFWk3wwfgORr34pxvX9MxttkjCmgusa55wKTffr11WiqIx35OmIXqjbjk6YN1uxQpDmA41C7opqkXhAZjls2cqG4iJFsQg2mnX4oO5SLxVS8gMmVfTYCfRKyfwG7kREGZ3eAXIDfdqyhVkibKUU3nCul3mVqs8IUhnOqKiWs1fJCKU9iFe2vTCF20Smn
";

    const HISTORY_EXPECTED: &str = r"
0,0,0,0,0,193157564249808,10.0,8C36KCyYo8Nd5nosW
1,0,0,0,0,193157564249808,10.0,clvD7gzsS2KDKllWMEoYR
2,0,0,0,0,193157564249808,10.0,n2vKZhcDjzacLvE6MhKa6
3,0,0,0,0,193157564249808,10.0,ZzjjEQL74tbVro5HcP
4,0,0,0,0,193157564249808,10.0,s8UlUTTvloLlDClBPrgo
5,0,0,0,0,193157564249808,10.0,RSvMwUGUVwvlyCHTZhVFk
6,0,0,0,0,193157564249808,10.0,8sMvn1jd5VK8jK9Ni7umSYn
7,0,0,0,0,193157564249808,10.0,tBqzVGglQvwCvy1dFUNrvWhi
8,0,0,0,0,193157564249808,10.0,kvYqRPNG6nXiLoLL
9,0,0,0,0,193157564249808,10.0,5drxezJzS09P
";

    const ORDER_EXPECTED: &str = r"
0,2214,0,0,193157564249808,5,6,1
1,2732,0,0,193157564249808,7,9,1
2,785,0,0,193157564249808,3,14,1
3,2122,0,0,193157564249808,5,9,1
4,1597,0,0,193157564249808,5,15,1
5,1879,0,0,193157564249808,9,6,1
6,1293,0,0,193157564249808,9,10,1
7,1684,0,0,193157564249808,4,5,1
8,2782,0,0,193157564249808,7,9,1
9,2902,0,0,193157564249808,7,7,1
";

    const ORDER_LINE_EXPECTED: &str = r"
0,0,0,0,58815,0,193157564249808,5,4563.720543686956,yYo8Nd5nosWSKlp1y38cYB0a
0,0,0,1,34601,0,193157564249808,5,9324.527574856582,swrQzbPWviBQwuAYNgf5lUli
0,0,0,2,17637,0,193157564249808,5,9605.969006727235,FLMEchyOd0ZyA1W66M9UHzjF
0,0,0,3,25017,0,193157564249808,5,9811.476349652017,pT1VwvZvqQ3WQTxLfCinHti9
0,0,0,4,78055,0,193157564249808,5,8732.291209534731,Gjn6E6FfOyS45mclf2SdR2VE
0,0,0,5,96622,0,193157564249808,5,9085.943936094973,XhqI7H5qbx3QXhIEF3TCOTqS
0,0,0,6,28701,0,193157564249808,5,1651.206286390627,fnpS39JBXj8yvQUR01qjsVQP
0,0,0,7,6833,0,193157564249808,5,9936.605085283258,OqETxOFniELGm1QxpZ458gjE
0,0,0,8,36372,0,193157564249808,5,8350.31735063976,e4PXray0VaJvLIwJwTsNA2hG
0,0,0,9,76034,0,193157564249808,5,3684.5330644108262,raprwZeyMtcyxC926RhPhp8e
";

    const NEW_ORDER_EXPECTED: &str = r"
2100,0,0
2101,0,0
2102,0,0
2103,0,0
2104,0,0
2105,0,0
2106,0,0
2107,0,0
2108,0,0
2109,0,0
";

    #[track_caller]
    fn test_table<T>(mut table: T, expected: &str)
    where
        T: Table,
        for<'a> <<T as Table>::Data as Data>::Ref<'a>: serde::Serialize,
    {
        let mut actual = Vec::new();
        let mut writer = csv::Writer::from_writer(&mut actual);

        let mut batch = <T::Data as Data>::Col::default();
        for batch_idx in 0..table.num_batches() {
            batch.clear();
            table.gen_batch(batch_idx, &mut batch);
            for idx in 0..batch.len() {
                writer.serialize(&batch.get(idx)).unwrap();
            }
        }
        drop(writer);

        let actual = String::from_utf8(actual).unwrap();
        let actual = actual.split_inclusive('\n').take(10).collect::<String>();
        if actual != expected {
            panic!("\nexpected\n{}\nactual\n{}\n", expected, actual);
        }
    }

    #[test]
    fn tpcc() {
        let config = TpccConfig {
            warehouses: 1,
            now: TpccConfig::FEB_18_2023_1_PM,
        };
        test_table::<Item>(Item::init(config.clone()), ITEM_EXPECTED.trim_start());
        test_table::<Warehouse>(
            Warehouse::init(config.clone()),
            WAREHOUSE_EXPECTED.trim_start(),
        );
        test_table::<Stock>(Stock::init(config.clone()), STOCK_EXPECTED.trim_start());
        test_table::<District>(
            District::init(config.clone()),
            DISTRICT_EXPECTED.trim_start(),
        );
        test_table::<Customer>(
            Customer::init(config.clone()),
            CUSTOMER_EXPECTED.trim_start(),
        );
        test_table::<History>(History::init(config.clone()), HISTORY_EXPECTED.trim_start());
        test_table::<Order>(Order::init(config.clone()), ORDER_EXPECTED.trim_start());
        test_table::<OrderLine>(
            OrderLine::init(config.clone()),
            ORDER_LINE_EXPECTED.trim_start(),
        );
        test_table::<NewOrder>(NewOrder::init(config), NEW_ORDER_EXPECTED.trim_start());
    }
}
