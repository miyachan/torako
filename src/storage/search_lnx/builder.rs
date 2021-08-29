use std::{num::NonZeroUsize, sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
}};
use std::time::Duration;

use futures::prelude::*;
use futures::task::AtomicWaker;
use log::info;

use super::{Error, Search, SearchInner};

pub struct SearchBuilder {
    db_url: Option<url::Url>,
    index: String,
    inflight_posts: usize,
    fail_on_save_error: bool,
    retries_on_save_error: usize,
    authentication_key: String,
    commit_sync_interval: Duration,
    concurrent_requests: usize,
}

impl Default for SearchBuilder {
    fn default() -> Self {
        SearchBuilder {
            db_url: None,
            index: Default::default(),
            inflight_posts: usize::MAX,
            concurrent_requests: usize::MAX,
            fail_on_save_error: true,
            retries_on_save_error: 0,
            authentication_key: Default::default(),
            commit_sync_interval: Duration::from_secs(5),
        }
    }
}

impl SearchBuilder {
    pub fn with_database(mut self, database_url: url::Url) -> Self {
        self.db_url = Some(database_url);
        self
    }

    pub fn with_index<T: AsRef<str>>(mut self, index: T) -> Self {
        self.index = String::from(index.as_ref());
        self
    }

    pub fn commit_sync_interval(mut self, interval: Duration) -> Self {
        self.commit_sync_interval = interval;
        self
    }

    pub fn concurrent_requests(mut self, requests: NonZeroUsize) -> Self {
        self.concurrent_requests = requests.get();
        self
    }

    pub fn authentication_key<T: AsRef<str>>(mut self, index: T) -> Self {
        self.authentication_key = String::from(index.as_ref());
        self
    }

    pub fn max_inflight_posts(mut self, posts: usize) -> Self {
        self.inflight_posts = posts;
        self
    }

    pub fn fail_on_save_error(mut self, yes: bool) -> Self {
        self.fail_on_save_error = yes;
        self
    }

    pub fn retries_on_save_error(mut self, retries: usize) -> Self {
        self.retries_on_save_error = retries;
        self
    }

    pub async fn build(self) -> Result<Search, Error> {
        info!("Initializing Asagi Lnx Search Backend...");
        if self.index.is_empty() {
            return Err(Error::InvalidIndex);
        }
        let mut headers = reqwest::header::HeaderMap::new();
        if !self.authentication_key.is_empty() {
            headers.insert(
                "Authorization",
                reqwest::header::HeaderValue::from_str(&format!(
                    "Bearer {}",
                    self.authentication_key.as_str()
                ))
                .unwrap(),
            );
        }
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .unwrap();

        // info!("Creating index (if needed)...");
        // TODO

        let mut upload_url = self.db_url.clone().unwrap();
        upload_url.set_path(&format!("/indexes/{}/documents", self.index));
        upload_url.set_query(Some("wait=true"));

        let mut commit_url = self.db_url.clone().unwrap();
        commit_url.set_path(&format!("/indexes/{}/commit", self.index));

        let (process_tx, process_rx) = tokio::sync::mpsc::unbounded_channel();
        let commit_lock = Arc::new(tokio::sync::RwLock::new(()));
        let dirty = Arc::new(AtomicBool::new(false));
        let dirty_weak = Arc::downgrade(&dirty);
        let commit_sync_interval = self.commit_sync_interval;

        let search = SearchInner {
            client: client.clone(),
            upload_url,
            max_inflight_posts: self.inflight_posts,
            fail_on_save_error: self.fail_on_save_error,
            retries_on_save_error: self.retries_on_save_error,
            requests: tokio::sync::Semaphore::new(self.concurrent_requests),

            failed: AtomicBool::new(false),
            inflight_posts: AtomicUsize::new(0),
            waker: Arc::new(AtomicWaker::new()),
            flush_waker: Arc::new(AtomicWaker::new()),
            close_waker: Arc::new(AtomicWaker::new()),
            metrics: Arc::new(super::SearchMetrics::default()),
            process_tx,
            commit_lock: commit_lock.clone(),
            dirty,
        };

        let search = Arc::new(search);
        let search2 = search.clone();

        tokio::spawn(
            process_rx
                .take_while(|x| future::ready(x.is_some()))
                .zip(stream::repeat(search2))
                .map(|(x, search2)| match x {
                    Some(p) => search2.send_posts(p),
                    None => unreachable!(),
                })
                .buffer_unordered(usize::MAX)
                .for_each(|_| future::ready(())),
        );
        tokio::spawn(async move {
            loop {
                tokio::time::delay_for(commit_sync_interval).await;
                if let Some(dirty) = dirty_weak.upgrade() {
                    if dirty.load(Ordering::Relaxed) {
                        let started = std::time::Instant::now();
                        log::trace!("[lnx] Starting commit");
                        let t = commit_lock.write().await;
                        let r = client
                            .post(commit_url.clone())
                            .send()
                            .and_then(|resp| futures::future::ready(resp.error_for_status()))
                            .await;
                        drop(t);
                        if let Err(err) = r {
                            log::warn!("Failed to commit lnx search index: {}", err);
                        } else {
                            log::trace!("[lnx] Commit completed, took {}ms", started.elapsed().as_secs_f32() / 1000.);
                            dirty.store(false, Ordering::Relaxed)
                        }
                    }
                } else {
                    break;
                }
            }
        });

        Ok(Search { inner: search })
    }
}

impl From<&crate::config::AsagiLnxSearch> for SearchBuilder {
    fn from(config: &crate::config::AsagiLnxSearch) -> Self {
        let mut builder = SearchBuilder::default();
        builder = builder.with_database(config.database_url.clone());
        builder = builder.with_index(config.index.as_str());
        if let Some(inflight_posts) = config.inflight_posts {
            builder = builder.max_inflight_posts(inflight_posts.into());
        }
        if let Some(fail_on_save_error) = config.fail_on_save_error {
            builder = builder.fail_on_save_error(fail_on_save_error);
        }
        if let Some(retries_on_save_error) = config.retries_on_save_error {
            builder = builder.retries_on_save_error(retries_on_save_error);
        }
        if let Some(authentication_key) = config.authentication_key.as_ref() {
            builder = builder.authentication_key(authentication_key);
        }
        if let Some(commit_sync_interval) = config.commit_sync_interval.as_ref() {
            builder = builder.commit_sync_interval(*commit_sync_interval);
        }
        if let Some(concurrent_requests) = config.concurrent_requests.as_ref() {
            builder = builder.concurrent_requests(*concurrent_requests);
        }

        builder
    }
}
