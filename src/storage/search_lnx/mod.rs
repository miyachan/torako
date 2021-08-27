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

mod builder;
mod post;

use crate::imageboard;
pub use builder::SearchBuilder;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid index name")]
    InvalidIndex,
    #[error("A fatal error occured when trying to archive posts")]
    ArchiveError,
    #[error("database error: {}", .0)]
    DB(reqwest::Error),
    #[error("io error: {}", .0)]
    IO(#[from] std::io::Error),
}

struct SearchInner {
    client: reqwest::Client,
    upload_url: url::Url,
    commit_url: url::Url,
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
        "lnx_search"
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
    async fn save_posts(&self, item: Vec<imageboard::Post>) -> Result<(), Error> {
        let posts = item.iter().map(|p| p.into()).collect::<Vec<post::Post>>();
        let mut err = None;
        let mut backoff = backoff::ExponentialBackoff::default();
        backoff.max_elapsed_time = None;
        let start = Instant::now();
        let rows = posts.len();
        for _ in 0..=self.retries_on_save_error {
            let r = self
                .client
                .post(self.upload_url.clone())
                .json(&posts)
                .send()
                .await
                .and_then(|resp| resp.error_for_status());

            if let Err(r) = r {
                err = Some(Err(Error::DB(r)));
                if let Some(b) = backoff.next_backoff() {
                    tokio::time::delay_for(b).await;
                }
                continue;
            }

            let r = self
                .client
                .post(self.commit_url.clone())
                .send()
                .await
                .and_then(|resp| resp.error_for_status());

            if let Err(r) = r {
                err = Some(Err(Error::DB(r)));
                if let Some(b) = backoff.next_backoff() {
                    tokio::time::delay_for(b).await;
                }
                continue;
            }

            self.metrics.incr_posts(rows as u64);
            self.metrics.incr_query_time(start.elapsed());
            self.notify_post(rows);

            return Ok(());
        }
        return err.unwrap();
    }

    async fn send_posts(self: Arc<Self>, item: Vec<imageboard::Post>) {
        let board = item[0].board;
        let thread_no = item[0].thread_no();
        let post_no = item[0].no;
        let sz = item.len();
        match self.save_posts(item).await {
            Ok(_) => debug!(
                "Flushed {} posts to lnx. [First]: {}/{}/{}",
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
