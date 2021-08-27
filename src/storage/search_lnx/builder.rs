use std::sync::{
    atomic::{AtomicBool, AtomicUsize},
    Arc,
};

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
}

impl Default for SearchBuilder {
    fn default() -> Self {
        SearchBuilder {
            db_url: None,
            index: Default::default(),
            inflight_posts: usize::MAX,
            fail_on_save_error: true,
            retries_on_save_error: 0,
            authentication_key: Default::default(),
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

        let mut upload_url = self.db_url.as_ref().unwrap().clone();
        upload_url.set_path(&format!("/indexes/{}/documents", self.index));
        upload_url.set_query(Some("wait=true"));

        let mut commit_url = self.db_url.unwrap().clone();
        commit_url.set_path(&format!("/indexes/{}/commit", self.index));

        let (process_tx, process_rx) = tokio::sync::mpsc::unbounded_channel();

        let search = SearchInner {
            client,
            upload_url,
            commit_url,
            max_inflight_posts: self.inflight_posts,
            fail_on_save_error: self.fail_on_save_error,
            retries_on_save_error: self.retries_on_save_error,

            failed: AtomicBool::new(false),
            inflight_posts: AtomicUsize::new(0),
            waker: Arc::new(AtomicWaker::new()),
            flush_waker: Arc::new(AtomicWaker::new()),
            close_waker: Arc::new(AtomicWaker::new()),
            metrics: Arc::new(super::SearchMetrics::default()),
            process_tx,
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

        builder
    }
}
