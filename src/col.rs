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
    ( $data:ident ) => {
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
data_primitive!(f32);
data_primitive!(f64);

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

macro_rules! data_tuple {
    ( $($data:ident)+ ) => (
        impl<$($data: Data),*> Data for ($($data,)*) {
            type Ref<'a> = ($($data::Ref<'a>),*);
            type Col = ($($data::Col),*);
        }
    );
}

data_tuple! { T0 T1 T2 }
data_tuple! { T0 T1 T2 T3 }
data_tuple! { T0 T1 T2 T3 T4 }
data_tuple! { T0 T1 T2 T3 T4 T5 }
data_tuple! { T0 T1 T2 T3 T4 T5 T6 }
data_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 }
data_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 T8 }
data_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 }
data_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 TA }
data_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 TA TB }
// data_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 TA TB TC }
// data_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 TA TB TC TD }
// data_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 TA TB TC TD TE }
// data_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 TA TB TC TD TE TF }

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
    ( $data:ident ) => {
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
col_primitive!(f32);
col_primitive!(f64);

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

macro_rules! col_tuple {
    ( $( $data:ident )* ; $( $col:ident )* ) => {
        #[allow(non_snake_case)]
        impl<$($data: Data),+, $($col: Col<$data>),+> Col<($($data),+)> for ($($col),+) {
            #[allow(unused_assignments)]
            fn len(&self) -> usize {
                let ($($col),+) = self;
                let mut ret = 0;
                $(
                    ret = $col.len();
                )+
                $(
                    debug_assert_eq!($col.len(), ret);
                )+
                ret
            }

            fn get<'a>(&'a self, idx: usize) -> <($($data),*) as Data>::Ref<'a> {
                let ($($col),+) = self;
                ($($col.get(idx)),+)
            }

            fn push(&mut self, t: <($($data),*) as Data>::Ref<'_>) {
                let ($($col),+) = self;
                let ($($data),+) = t;
                $(
                    $col.push($data);
                )+
            }

            fn clear(&mut self) {
                let ($($col),+) = self;
                $(
                    $col.clear();
                )+
            }

            fn good_bytes(&self) -> usize {
                let ($($col),+) = self;
                let mut ret = 0;
                $(
                    ret += $col.good_bytes();
                )+
                ret
            }
        }
    };
}

col_tuple! { T0 T1 T2; C0 C1 C2 }
col_tuple! { T0 T1 T2 T3; C0 C1 C2 C3 }
col_tuple! { T0 T1 T2 T3 T4; C0 C1 C2 C3 C4 }
col_tuple! { T0 T1 T2 T3 T4 T5; C0 C1 C2 C3 C4 C5 }
col_tuple! { T0 T1 T2 T3 T4 T5 T6; C0 C1 C2 C3 C4 C5 C6 }
col_tuple! { T0 T1 T2 T3 T4 T5 T6 T7; C0 C1 C2 C3 C4 C5 C6 C7 }
col_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 T8; C0 C1 C2 C3 C4 C5 C6 C7 C8 }
col_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 T8 T9; C0 C1 C2 C3 C4 C5 C6 C7 C8 C9 }
col_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 TA; C0 C1 C2 C3 C4 C5 C6 C7 C8 C9 CA }
col_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 TA TB; C0 C1 C2 C3 C4 C5 C6 C7 C8 C9 CA CB }
// col_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 TA TB TC; C0 C1 C2 C3 C4 C5 C6 C7 C8 C9 CA CB CC }
// col_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 TA TB TC TD; C0 C1 C2 C3 C4 C5 C6 C7 C8 C9 CA CB CC CD }
// col_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 TA TB TC TD TE; C0 C1 C2 C3 C4 C5 C6 C7 C8 C9 CA CB CC CD CE }
// col_tuple! { T0 T1 T2 T3 T4 T5 T6 T7 T8 T9 TA TB TC TD TE TF; C0 C1 C2 C3 C4 C5 C6 C7 C8 C9 CA CB CC CD CE CF }
