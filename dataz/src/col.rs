// Copyright 2019 Daniel Harrison. All Rights Reserved.

pub trait Col {
    type Impl: ColImpl<Self>;
}

pub trait ColImpl<T: ?Sized> {
    fn len(&self) -> usize;
    fn push(&mut self, t: &T);
    fn get(&self, idx: usize) -> Option<&T>;
    // TODO fn clear(&mut self);
}

impl Col for () {
    type Impl = usize;
}
impl Col for u8 {
    type Impl = Vec<u8>;
}
impl Col for u64 {
    type Impl = Vec<u64>;
}
impl Col for usize {
    type Impl = Vec<usize>;
}
impl Col for str {
    type Impl = (Vec<usize>, String);
}
impl<T: Col + Clone> Col for [T] {
    type Impl = (Vec<usize>, Vec<T>);
}
impl<T1: Col, T2: Col> Col for (T1, T2) {
    type Impl = (Vec<T1>, Vec<T2>);
}

impl ColImpl<()> for usize {
    fn len(&self) -> usize {
        *self
    }

    fn push(&mut self, t: &()) {
        *self += 1;
    }

    fn get(&self, idx: usize) -> Option<&()> {
        todo!()
    }
}

impl<T: ToOwned> ColImpl<T> for Vec<T::Owned> {
    fn len(&self) -> usize {
        self.len()
    }

    fn push(&mut self, t: &T) {
        self.push(t.to_owned())
    }

    fn get(&self, idx: usize) -> Option<&T> {
        self.get(idx)
    }
}

impl<T: Col + Clone> ColImpl<[T]> for (Vec<usize>, Vec<T>) {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn push(&mut self, t: &[T]) {
        self.1.extend_from_slice(t);
        self.0.push(self.1.len())
    }

    fn get(&self, idx: usize) -> Option<&[T]> {
        todo!()
    }
}

impl ColImpl<str> for (Vec<usize>, String) {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn push(&mut self, t: &str) {
        self.1.push_str(t);
        self.0.push(self.1.len())
    }

    fn get(&self, idx: usize) -> Option<&str> {
        todo!()
    }
}

impl<T1: Col, T2: Col> ColImpl<(T1, T2)> for (Vec<T1>, Vec<T2>) {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn push(&mut self, t: &(T1, T2)) {
        self.0.push(t.0);
        self.1.push(t.1);
    }

    // WIP yeah this is where it breaks. no way to return a &(T1, T2)
    fn get(&self, idx: usize) -> Option<&(T1, T2)> {
        todo!()
    }
}
