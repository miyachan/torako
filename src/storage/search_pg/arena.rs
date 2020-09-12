use std::cell::UnsafeCell;

pub struct Arena<T> {
    // For soundness, maybe inner should be protected by a Mutex?
    // ex. How do you prevent Arena from being mutated across threads?
    inner: UnsafeCell<Vec<T>>,
}

impl<T> Arena<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: UnsafeCell::new(Vec::with_capacity(capacity)),
        }
    }

    pub fn alloc<'a>(&'a self, v: T) -> &'a T {
        let inner = unsafe { &mut *self.inner.get() };
        if inner.len() >= inner.capacity() {
            panic!("'rena is in a bad state. The 'rena must be explictly sized to hold no more than the amount of objects it was allocated with.")
        }
        inner.push(v);
        let len = inner.len();
        &inner[len - 1]
    }
}
