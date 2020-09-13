use std::borrow::Cow;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use backoff::backoff::Backoff;
use futures::prelude::*;
use futures::task::AtomicWaker;
use log::{debug, error, warn};
use serde::Serialize;
use thiserror::Error;
use tokio_postgres::types::ToSql;

mod arena;
mod builder;
mod placeholders;

use crate::imageboard;
pub use builder::SearchBuilder;
use placeholders::PLACEHOLDERS;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid database pool size")]
    InvalidPoolSize,
    #[error("invalid database URL provided: {}", .0)]
    InvalidDatabase(tokio_postgres::Error),
    #[error("A fatal error occured when trying to archive posts")]
    ArchiveError,
    #[error("database connection error: {}", .0)]
    Pool(#[from] deadpool_postgres::PoolError),
    #[error("database error: {}", .0)]
    DB(#[from] tokio_postgres::Error),
    #[error("io error: {}", .0)]
    IO(#[from] std::io::Error),
}

struct SearchInner {
    db_pool: deadpool_postgres::Pool,
    max_inflight_posts: usize,
    fail_on_save_error: bool,
    retries_on_save_error: usize,

    failed: AtomicBool,
    inflight_posts: AtomicUsize,
    waker: Arc<AtomicWaker>,
    flush_waker: Arc<AtomicWaker>,
    close_waker: Arc<AtomicWaker>,
    metrics: Arc<SearchMetrics>,
    process_tx: tokio::sync::mpsc::UnboundedSender<Option<Vec<imageboard::Post>>>,
}

#[derive(Debug, Serialize)]
pub struct Metrics {
    pub posts: u64,
    pub avg_insert_time_ms: f64,
    pub save_errors: u64,
}

#[derive(Default, Debug)]
struct SearchMetrics {
    posts: AtomicU64,
    queries: AtomicU64,
    query_time_ns: AtomicU64,
    save_errors: AtomicU64,
}

impl SearchMetrics {
    pub fn incr_posts(&self, count: u64) {
        self.posts.fetch_add(count, Ordering::Relaxed);
    }

    pub fn incr_query_time(&self, dur: Duration) {
        self.queries.fetch_add(1, Ordering::Relaxed);
        self.query_time_ns
            .fetch_add(dur.as_nanos() as u64, Ordering::Relaxed);
    }

    pub fn incr_save_error(&self, count: u64) {
        self.save_errors.fetch_add(count, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct SearchMetricsProvider {
    inner: Arc<SearchInner>,
}

impl super::MetricsProvider for SearchMetricsProvider {
    fn name(&self) -> &'static str {
        "pg_search"
    }

    fn metrics(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Box<dyn erased_serde::Serialize + Send>> + Send>>
    {
        let queries = self.inner.metrics.queries.load(Ordering::Acquire) as f64;
        let tt = self.inner.metrics.query_time_ns.load(Ordering::Acquire) as f64;
        let m = Metrics {
            posts: self.inner.metrics.posts.load(Ordering::Acquire),
            avg_insert_time_ms: queries / tt * 1_000_000.,
            save_errors: self.inner.metrics.save_errors.load(Ordering::Acquire),
        };
        let m: Box<dyn erased_serde::Serialize + Send> = Box::new(m);
        futures::future::ready(m).boxed()
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Search {
    inner: Arc<SearchInner>,
}

impl Search {
    #[allow(dead_code)]
    pub fn builder() -> SearchBuilder {
        SearchBuilder::default()
    }

    pub fn metrics_provider(&self) -> impl super::MetricsProvider {
        SearchMetricsProvider {
            inner: self.inner.clone(),
        }
    }
}

impl SearchInner {
    async fn save_posts(&self, mut item: Vec<imageboard::Post>) -> Result<(), Error> {
        let client = self.db_pool.get().await?;
        while item.len() > 0 {
            let start = Instant::now();
            // Postgres only supports a maximum of 2^15 params
            let (remain, posts) = if item.len() > 1280 {
                let remain = item.split_off(1280);
                (remain, item)
            } else {
                (vec![], item)
            };
            item = remain;
            let rows = posts.len();
            let query = "INSERT INTO
                            posts
                (board, thread_no, post_no, subject, username, tripcode,
                email, unique_id, since4_pass, country, filename,
                image_hash, image_width, image_height, ts, comment, deleted,
                ghost, sticky, spoiler, op, capcode) VALUES ";
            let stmt = std::iter::once(Cow::Borrowed(query))
                .chain((0..rows).map(|i| {
                    let z = i * 22;
                    Cow::Owned(
                        [
                            if i == 0 { "(" } else { "\n,(" },
                            PLACEHOLDERS[z], // board
                            ",",
                            PLACEHOLDERS[z + 1], // thread_no
                            ",",
                            PLACEHOLDERS[z + 2], // post_no
                            ",to_tsvector(",
                            PLACEHOLDERS[z + 3], // subject
                            "),to_tsvector(",
                            PLACEHOLDERS[z + 4], // username
                            "),to_tsvector(",
                            PLACEHOLDERS[z + 5], // tripcode
                            "),to_tsvector(",
                            PLACEHOLDERS[z + 6], // email
                            "),",
                            PLACEHOLDERS[z + 7], // unique_id
                            ",",
                            PLACEHOLDERS[z + 8], // since4_pass
                            ",",
                            PLACEHOLDERS[z + 9], // country
                            ",to_tsvector(REPLACE(",
                            PLACEHOLDERS[z + 10], // filename
                            ",'.',' ')),",
                            PLACEHOLDERS[z + 11], // image_hash
                            ",",
                            PLACEHOLDERS[z + 12], // image_width
                            ",",
                            PLACEHOLDERS[z + 13], // image_height
                            ",TO_TIMESTAMP(CAST(",
                            PLACEHOLDERS[z + 14], // ts
                            "::INT8 AS FLOAT8)),to_tsvector(",
                            PLACEHOLDERS[z + 15], // comment
                            "),",
                            PLACEHOLDERS[z + 16], // deleted
                            ",",
                            PLACEHOLDERS[z + 17], // ghost
                            ",",
                            PLACEHOLDERS[z + 18], // sticky
                            ",",
                            PLACEHOLDERS[z + 19], // spoiler
                            ",",
                            PLACEHOLDERS[z + 20], // op
                            ",CAST(",
                            PLACEHOLDERS[z + 21], // capcode
                            "::INT8 AS INT4))",
                        ]
                        .join(""),
                    )
                }))
                .chain(std::iter::once(Cow::Borrowed(
                    " ON CONFLICT (board, post_no) DO UPDATE SET
                    deleted = EXCLUDED.deleted,
                    sticky = EXCLUDED.sticky,
                    comment = EXCLUDED.comment;
                ",
                )))
                .collect::<String>();

            let i64_rena = arena::Arena::new(posts.len() * 4);
            let str_rena = arena::Arena::new(posts.len() * 4);

            let params = (0..posts.len())
                .into_iter()
                .map(|i| {
                    let values: Box<[&(dyn ToSql + Sync)]> = Box::new([
                        str_rena.alloc(Some(posts[i].board.to_string())),
                        i64_rena.alloc(Some(posts[i].thread_no() as i64)),
                        i64_rena.alloc(Some(posts[i].no as i64)),
                        &posts[i].sub,
                        &posts[i].name,
                        &posts[i].trip,
                        &posts[i].email,
                        &posts[i].id,
                        &posts[i].since4pass,
                        str_rena.alloc(posts[i].poster_country()),
                        str_rena.alloc(posts[i].media_filename()),
                        &posts[i].md5,
                        &posts[i].w,
                        &posts[i].h,
                        i64_rena.alloc(Some(posts[i].time as i64)),
                        str_rena.alloc(posts[i].comment()),
                        &posts[i].deleted,
                        &false,
                        &posts[i].sticky,
                        &posts[i].spoiler,
                        if posts[i].is_op() { &true } else { &false },
                        i64_rena.alloc(posts[i].short_capcode().chars().next().map(|c| c as i64)),
                    ]);
                    values.into_vec()
                })
                .flatten()
                .collect::<Vec<_>>();

            let mut attempts = 0;
            let mut backoff = backoff::ExponentialBackoff::default();
            backoff.max_elapsed_time = None;
            loop {
                let r = client.execute(stmt.as_str(), &params).await;
                match r {
                    Ok(_) => break,
                    Err(err) => {
                        if attempts >= self.retries_on_save_error {
                            return Err(Error::from(err));
                        }
                        attempts += 1;
                        if let Some(b) = backoff.next_backoff() {
                            tokio::time::delay_for(b).await;
                        }
                        continue;
                    }
                }
            }
            self.metrics.incr_posts(rows as u64);
            self.metrics.incr_query_time(start.elapsed());
            self.notify_post(rows);

            // Since values contains references to data in the 'renas,
            // the values must be dropped before we drop the 'renas
            drop(params);
            drop(i64_rena);
            drop(str_rena);
        }

        Ok(())
    }

    async fn send_posts(self: Arc<Self>, item: Vec<imageboard::Post>) {
        let board = item[0].board;
        let thread_no = item[0].thread_no();
        let post_no = item[0].no;
        let sz = item.len();
        match self.save_posts(item).await {
            Ok(_) => debug!(
                "Flushed {} posts to postgres. [First]: {}/{}/{}",
                sz, board, thread_no, post_no
            ),
            Err(err) => {
                error!(
                    "Failed to save data for {} posts [First]: {}/{}/{}: {}",
                    sz, board, thread_no, post_no, err
                );
                if !self.fail_on_save_error {
                    warn!("Some posts were unable to be archived, however the error isn't being treated as fatal. Some posts may be lost.")
                }
                self.metrics.incr_save_error(1);
                self.failed.store(true, Ordering::SeqCst);
            }
        }
    }

    fn notify_post(&self, no_posts: usize) {
        let old = self.inflight_posts.fetch_sub(no_posts, Ordering::AcqRel);
        let curr = old - no_posts;
        if curr < self.max_inflight_posts {
            self.waker.wake();
        }
        if curr == 0 {
            self.flush_waker.wake();
            self.close_waker.wake();
        }
    }

    fn is_ready(&self) -> bool {
        let posts = self.inflight_posts.load(Ordering::Acquire);
        posts < self.max_inflight_posts
    }

    fn is_empty(&self) -> bool {
        let posts = self.inflight_posts.load(Ordering::Acquire);
        posts == 0
    }

    fn has_failed(&self) -> bool {
        return self.fail_on_save_error && self.failed.load(Ordering::Relaxed);
    }
}

impl Sink<Vec<imageboard::Post>> for Search {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        if self.inner.has_failed() {
            return Poll::Ready(Err(Error::ArchiveError));
        }
        self.inner.waker.register(cx.waker());
        match self.inner.is_ready() {
            true => Poll::Ready(Ok(())),
            false => Poll::Pending,
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Vec<imageboard::Post>) -> Result<(), Self::Error> {
        if item.len() > 0 {
            self.inner
                .inflight_posts
                .fetch_add(item.len(), Ordering::AcqRel);
            self.inner.process_tx.send(Some(item)).unwrap();
        }
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        if self.inner.has_failed() {
            return Poll::Ready(Err(Error::ArchiveError));
        }
        self.inner.flush_waker.register(cx.waker());
        match self.inner.is_empty() {
            true => Poll::Ready(Ok(())),
            false => Poll::Pending,
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let _ = self.inner.process_tx.send(None);
        if self.inner.has_failed() {
            return Poll::Ready(Err(Error::ArchiveError));
        }
        self.inner.close_waker.register(cx.waker());
        match self.inner.is_empty() {
            true => Poll::Ready(Ok(())),
            false => Poll::Pending,
        }
    }
}
