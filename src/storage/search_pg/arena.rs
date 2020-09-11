use std::cell::UnsafeCell;

pub struct Arena<T> {
    inner: UnsafeCell<Vec<T>>,
}

impl<T> Arena<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: UnsafeCell::new(Vec::with_capacity(capacity)),
        }
    }

    pub fn alloc(&self, v: T) -> &T {
        unsafe {
            let inner = &mut *self.inner.get();
            inner.push(v);
            let len = inner.len();
            &inner[len - 1]
        }
    }
}
