use std::cell::RefCell;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::prelude::*;
use mysql_async::prelude::*;
use rustc_hash::FxHashMap;
use serde::Serialize;

pub struct MaybeMetric<T> {
    metric: Option<T>,
    modified: Instant,
    lock: Arc<futures::lock::Mutex<()>>,
}

impl<T> Default for MaybeMetric<T> {
    fn default() -> Self {
        MaybeMetric {
            metric: None,
            modified: Instant::now() - Duration::from_secs(300),
            lock: Arc::new(futures::lock::Mutex::new(())),
        }
    }
}

thread_local! {
    static POST_STATS: RefCell<MaybeMetric<Arc<FxHashMap<&'static str, u64>>>> = RefCell::new(MaybeMetric::default());
}

#[derive(Debug, Serialize, Clone)]
pub struct DatabaseMetrics {
    posts: Arc<FxHashMap<&'static str, u64>>,
}

pub(super) async fn database_metrics(
    asagi: Arc<super::AsagiInner>,
    boards: Vec<&'static str>,
) -> Result<DatabaseMetrics, super::Error> {
    loop {
        let (ps, lock) = POST_STATS.with(|ps| {
            let stats = ps.borrow();
            let last_update = stats.modified;
            match &stats.metric {
                Some(stats) if last_update.elapsed() < Duration::from_secs(60) => {
                    (Some(stats.clone()), None)
                }
                _ => (None, Some(stats.lock.clone())),
            }
        });
        if let Some(ps) = ps {
            return Ok(DatabaseMetrics { posts: ps });
        }
        let lock = lock.unwrap();
        let guard = lock.try_lock();
        if guard.is_none() {
            drop(lock.lock().await);
            continue;
        };

        // https://github.com/rust-lang/rust/issues/64552#issuecomment-669728225
        let stats: std::pin::Pin<Box<dyn Stream<Item = _> + Send>> = Box::pin(
            futures::stream::iter(boards.iter())
                .map(|board| {
                    let conn = asagi.direct_db_pool.get_conn();
                    async move {
                        let mut conn = conn.await?;
                        let sz: Option<u64> = conn
                            .query_first(format!("SELECT COUNT(*) FROM `{}`", board))
                            .await?;

                        Ok::<_, super::Error>((*board, sz.unwrap()))
                    }
                })
                .buffer_unordered(usize::MAX),
        );

        let fut = stats.try_collect::<FxHashMap<&'static str, u64>>();
        let stats = match tokio::time::timeout(Duration::from_secs(15), fut).await {
            Ok(f) => {
                let mut stats = f?;
                let total: u64 = stats.iter().map(|x| x.1).copied().sum();
                stats.insert("_total", total);
                stats
            }
            Err(_) => {
                let mut stats = FxHashMap::default();
                stats.insert("_timedout", 1);
                stats
            }
        };

        POST_STATS.with(move |ps| {
            let mut l = ps.borrow_mut();
            l.metric = Some(Arc::new(stats));
            l.modified = Instant::now();
        });
        drop(guard);
        continue;
    }
}
