use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::prelude::*;
use futures::task::AtomicWaker;
use parking_lot::Mutex;

#[derive(Default)]
pub struct IntervalLock {
    inner: Arc<IntervalLockInner>,
}

#[derive(Default, Debug)]
struct IntervalLockInner {
    leased: Mutex<Vec<(u64, u64)>>,
    wakers: Mutex<Vec<Arc<AtomicWaker>>>,
}

#[must_use]
#[derive(Debug)]
pub struct IntervalLockGuard {
    parent: Arc<IntervalLockInner>,
    range: (u64, u64),
}

struct AcquireRange {
    parent: Arc<IntervalLockInner>,
    range: (u64, u64),
    waker: Arc<AtomicWaker>,
}

impl IntervalLockInner {
    fn try_acquire(self: &Arc<Self>, range: (u64, u64)) -> Option<IntervalLockGuard> {
        let mut ranges = self.leased.lock();
        let has_overlap = ranges
            .iter()
            .any(|borrowed| range.0 <= borrowed.1 && borrowed.0 <= range.1);
        if has_overlap {
            None
        } else {
            ranges.push(range);
            Some(IntervalLockGuard {
                parent: self.clone(),
                range,
            })
        }
    }

    fn release(&self, range: (u64, u64)) {
        let mut ranges = self.leased.lock();
        if let Some(pos) = ranges.iter().position(|x| *x == range) {
            ranges.remove(pos);
        }
        drop(ranges);
        self.wakers.lock().iter().for_each(|w| w.wake());
    }
}

impl IntervalLock {
    pub async fn acquire(&self, range: (u64, u64)) -> IntervalLockGuard {
        match self.inner.try_acquire(range) {
            Some(g) => g,
            None => {
                let waker = Arc::new(AtomicWaker::new());
                self.inner.wakers.lock().push(waker.clone());
                AcquireRange {
                    parent: self.inner.clone(),
                    range,
                    waker,
                }
                .await
            }
        }
    }
}

impl Future for AcquireRange {
    type Output = IntervalLockGuard;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IntervalLockGuard> {
        match self.parent.try_acquire(self.range) {
            Some(g) => {
                let waker_ptr = Arc::as_ptr(&self.waker);
                let mut wakers = self.parent.wakers.lock();
                if let Some(pos) = wakers.iter().position(|x| Arc::as_ptr(x) == waker_ptr) {
                    wakers.remove(pos);
                }
                Poll::Ready(g)
            }
            None => {
                self.waker.register(cx.waker());
                Poll::Pending
            }
        }
    }
}

impl Drop for IntervalLockGuard {
    fn drop(&mut self) {
        self.parent.release(self.range)
    }
}
