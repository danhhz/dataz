// Copyright 2023 Daniel Harrison. All Rights Reserved.

//! A tiny abstraction for columnar data
//!
//! Inspired heavily by [columnar](https://crates.io/crates/columnar).

/// A type of data storable in a column.
pub trait Data: Sized + 'static {
    /// The associated reference type of [Self] used for reads and writes on
    /// columns of this type.
    type Ref<'a>
    where
        Self: 'a;

    /// An implementation of [Col] for data of this type.
    type Col: Col<Self> + Default;
}

/// A column of data of type `T`.
//
// TODO: Some sort of `reserve` method.
pub trait Col<T: Data> {
    /// Returns the number of elements in the column.
    fn len(&self) -> usize;

    /// Retrieves the value at index.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is out of bounds.
    fn get<'a>(&'a self, idx: usize) -> T::Ref<'a>;

    /// Appends an element to the back of the column.
    fn push(&mut self, t: T::Ref<'_>);

    /// Clears all values from the column, leaving any allocations to be reused.
    fn clear(&mut self);

    /// Returns the size of the data in this column.
    fn good_bytes(&self) -> usize;
}

macro_rules! data_primitive {
    ($data:ident) => {
        impl Data for $data {
            type Ref<'a> = $data;
            type Col = Vec<$data>;
        }
    };
}

impl Data for () {
    type Ref<'a> = ();
    type Col = usize;
}

data_primitive!(bool);
data_primitive!(u8);
data_primitive!(u16);
data_primitive!(u32);
data_primitive!(u64);
data_primitive!(i8);
data_primitive!(i16);
data_primitive!(i32);
data_primitive!(i64);

impl Data for String {
    type Ref<'a> = &'a str;
    type Col = (Vec<usize>, String);
}

impl<T: Data> Data for Option<T>
where
    for<'a> T::Ref<'a>: Default,
{
    type Ref<'a> = Option<T::Ref<'a>>;
    type Col = (Vec<bool>, T::Col);
}

// TODO: Make this generic for any T
impl Data for Vec<u8> {
    type Ref<'a> = &'a [u8];
    type Col = (Vec<usize>, Vec<u8>);
}

// TODO: Macro for tuples up to size N
impl<T1: Data, T2: Data> Data for (T1, T2) {
    type Ref<'a> = (T1::Ref<'a>, T2::Ref<'a>);
    type Col = (T1::Col, T2::Col);
}

impl<T1: Data, T2: Data, T3: Data> Data for (T1, T2, T3) {
    type Ref<'a> = (T1::Ref<'a>, T2::Ref<'a>, T3::Ref<'a>);
    type Col = (T1::Col, T2::Col, T3::Col);
}

impl<T1: Data, T2: Data, T3: Data, T4: Data> Data for (T1, T2, T3, T4) {
    type Ref<'a> = (T1::Ref<'a>, T2::Ref<'a>, T3::Ref<'a>, T4::Ref<'a>);
    type Col = (T1::Col, T2::Col, T3::Col, T4::Col);
}

impl Col<()> for usize {
    fn len(&self) -> usize {
        *self
    }

    fn get<'a>(&'a self, idx: usize) -> <() as Data>::Ref<'a> {
        if idx < *self {
            ()
        } else {
            panic!("get index ({idx}) should be < len ({self})");
        }
    }

    fn push(&mut self, _: ()) {
        *self += 1;
    }

    fn clear(&mut self) {
        *self = 0;
    }

    fn good_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

macro_rules! col_primitive {
    ($data:ident) => {
        impl Col<$data> for Vec<$data> {
            fn len(&self) -> usize {
                self.len()
            }

            fn get<'a>(&'a self, idx: usize) -> <$data as Data>::Ref<'a> {
                self[idx]
            }

            fn push(&mut self, t: $data) {
                self.push(t)
            }

            fn clear(&mut self) {
                self.clear();
            }

            fn good_bytes(&self) -> usize {
                self.len() * std::mem::size_of::<$data>()
            }
        }
    };
}

col_primitive!(bool);
col_primitive!(u8);
col_primitive!(u16);
col_primitive!(u32);
col_primitive!(u64);
col_primitive!(i8);
col_primitive!(i16);
col_primitive!(i32);
col_primitive!(i64);

impl<T: Data, C: Col<T>> Col<Option<T>> for (Vec<bool>, C)
where
    for<'a> T::Ref<'a>: Default,
{
    fn len(&self) -> usize {
        let (set, values) = self;
        debug_assert_eq!(set.len(), values.len());
        set.len()
    }

    fn get<'a>(&'a self, idx: usize) -> <Option<T> as Data>::Ref<'a> {
        let (set, values) = self;
        debug_assert_eq!(set.len(), values.len());
        if set[idx] {
            Some(values.get(idx))
        } else {
            None
        }
    }

    fn push(&mut self, t: <Option<T> as Data>::Ref<'_>) {
        let (set, values) = self;
        match t {
            Some(t) => {
                set.push(true);
                values.push(t);
            }
            None => {
                set.push(false);
                values.push(T::Ref::default());
            }
        }
        debug_assert_eq!(set.len(), values.len());
    }

    fn clear(&mut self) {
        let (set, values) = self;
        set.clear();
        values.clear();
    }

    fn good_bytes(&self) -> usize {
        let (set, values) = self;
        set.len() * std::mem::size_of::<bool>() + values.good_bytes()
    }
}

impl Col<String> for (Vec<usize>, String) {
    fn len(&self) -> usize {
        let (lens, _) = self;
        lens.len()
    }

    fn get<'a>(&'a self, idx: usize) -> <String as Data>::Ref<'a> {
        let (lens, concat) = self;
        let end = lens[idx];
        let start = if idx == 0 { 0 } else { lens[idx - 1] };
        &concat[start..end]
    }

    fn push(&mut self, t: <String as Data>::Ref<'_>) {
        let (lens, concat) = self;
        concat.push_str(t);
        lens.push(concat.len());
    }

    fn clear(&mut self) {
        let (lens, concat) = self;
        lens.clear();
        concat.clear();
    }

    fn good_bytes(&self) -> usize {
        let (lens, concat) = self;
        lens.len() * std::mem::size_of::<usize>() + concat.len()
    }
}

impl Col<Vec<u8>> for (Vec<usize>, Vec<u8>) {
    fn len(&self) -> usize {
        let (lens, _) = self;
        lens.len()
    }

    fn get<'a>(&'a self, idx: usize) -> <Vec<u8> as Data>::Ref<'a> {
        let (lens, concat) = self;
        let end = lens[idx];
        let start = if idx == 0 { 0 } else { lens[idx - 1] };
        &concat[start..end]
    }

    fn push(&mut self, t: <Vec<u8> as Data>::Ref<'_>) {
        let (lens, concat) = self;
        concat.extend_from_slice(t);
        lens.push(concat.len());
    }

    fn clear(&mut self) {
        let (lens, concat) = self;
        lens.clear();
        concat.clear();
    }

    fn good_bytes(&self) -> usize {
        let (lens, concat) = self;
        lens.len() * std::mem::size_of::<usize>() + concat.len()
    }
}

impl<T1: Data, T2: Data, C1: Col<T1>, C2: Col<T2>> Col<(T1, T2)> for (C1, C2) {
    fn len(&self) -> usize {
        let (c1, c2) = self;
        debug_assert_eq!(c1.len(), c2.len());
        c1.len()
    }

    fn get<'a>(&'a self, idx: usize) -> <(T1, T2) as Data>::Ref<'a> {
        let (c1, c2) = self;
        debug_assert_eq!(c1.len(), c2.len());
        (c1.get(idx), c2.get(idx))
    }

    fn push(&mut self, t: <(T1, T2) as Data>::Ref<'_>) {
        let (c1, c2) = self;
        let (t1, t2) = t;
        c1.push(t1);
        c2.push(t2);
        debug_assert_eq!(c1.len(), c2.len());
    }

    fn clear(&mut self) {
        let (c1, c2) = self;
        c1.clear();
        c2.clear();
    }

    fn good_bytes(&self) -> usize {
        let (c1, c2) = self;
        c1.good_bytes() + c2.good_bytes()
    }
}

impl<T1: Data, T2: Data, T3: Data, C1: Col<T1>, C2: Col<T2>, C3: Col<T3>> Col<(T1, T2, T3)>
    for (C1, C2, C3)
{
    fn len(&self) -> usize {
        let (c1, _, _) = self;
        c1.len()
    }

    fn get<'a>(&'a self, idx: usize) -> <(T1, T2, T3) as Data>::Ref<'a> {
        let (c1, c2, c3) = self;
        (c1.get(idx), c2.get(idx), c3.get(idx))
    }

    fn push(&mut self, t: <(T1, T2, T3) as Data>::Ref<'_>) {
        let (c1, c2, c3) = self;
        let (t1, t2, t3) = t;
        c1.push(t1);
        c2.push(t2);
        c3.push(t3);
    }

    fn clear(&mut self) {
        let (c1, c2, c3) = self;
        c1.clear();
        c2.clear();
        c3.clear();
    }

    fn good_bytes(&self) -> usize {
        let (c1, c2, c3) = self;
        c1.good_bytes() + c2.good_bytes() + c3.good_bytes()
    }
}

impl<
        T1: Data,
        T2: Data,
        T3: Data,
        T4: Data,
        C1: Col<T1>,
        C2: Col<T2>,
        C3: Col<T3>,
        C4: Col<T4>,
    > Col<(T1, T2, T3, T4)> for (C1, C2, C3, C4)
{
    fn len(&self) -> usize {
        let (c1, _, _, _) = self;
        c1.len()
    }

    fn get<'a>(&'a self, idx: usize) -> <(T1, T2, T3, T4) as Data>::Ref<'a> {
        let (c1, c2, c3, c4) = self;
        (c1.get(idx), c2.get(idx), c3.get(idx), c4.get(idx))
    }

    fn push(&mut self, t: <(T1, T2, T3, T4) as Data>::Ref<'_>) {
        let (c1, c2, c3, c4) = self;
        let (t1, t2, t3, t4) = t;
        c1.push(t1);
        c2.push(t2);
        c3.push(t3);
        c4.push(t4);
    }

    fn clear(&mut self) {
        let (c1, c2, c3, c4) = self;
        c1.clear();
        c2.clear();
        c3.clear();
        c4.clear();
    }

    fn good_bytes(&self) -> usize {
        let (c1, c2, c3, c4) = self;
        c1.good_bytes() + c2.good_bytes() + c3.good_bytes() + c4.good_bytes()
    }
}
