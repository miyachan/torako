use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize},
    Arc,
};

use futures::prelude::*;
use futures::task::AtomicWaker;
use log::info;

use super::{Error, Search, SearchInner};

pub struct SearchBuilder {
    postgres_url: Option<url::Url>,
    inflight_posts: usize,
    fail_on_save_error: bool,
    retries_on_save_error: usize,
}

impl Default for SearchBuilder {
    fn default() -> Self {
        SearchBuilder {
            postgres_url: None,
            inflight_posts: usize::MAX,
            fail_on_save_error: true,
            retries_on_save_error: 0,
        }
    }
}

impl SearchBuilder {
    pub fn with_database(mut self, database_url: url::Url) -> Self {
        self.postgres_url = Some(database_url);
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
        info!("Initializing Asagi Postgres Search Backend...");
        info!("Connecting to Postgres...");
        let mut pg_url = self.postgres_url.clone().unwrap();

        let pool_size = pg_url
            .query_pairs()
            .find(|x| x.0 == "pool_size")
            .map(|x| x.1.parse::<usize>());
        let pool_size = match pool_size {
            Some(p) => match p {
                Ok(s) => s,
                Err(_) => return Err(Error::InvalidPoolSize),
            },
            None => 16,
        };

        pg_url.query_pairs_mut().clear().extend_pairs(
            self.postgres_url
                .as_ref()
                .unwrap()
                .query_pairs()
                .filter(|x| x.0 != "pool_size"),
        );

        let config = match tokio_postgres::Config::from_str(&pg_url.to_string()) {
            Ok(c) => c,
            Err(err) => return Err(Error::InvalidDatabase(err)),
        };
        let manager = deadpool_postgres::Manager::new(config, tokio_postgres::NoTls);
        let pool = deadpool_postgres::Pool::new(manager, pool_size);

        let client = pool.get().await?;

        info!("Creating tables (if needed)...");
        client.batch_execute(include_str!("posts.sql")).await?;

        drop(client);

        let (process_tx, process_rx) = tokio::sync::mpsc::unbounded_channel();

        let search = SearchInner {
            db_pool: pool,
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

impl From<&crate::config::AsagiSearch> for SearchBuilder {
    fn from(config: &crate::config::AsagiSearch) -> Self {
        let mut builder = SearchBuilder::default();
        builder = builder.with_database(config.database_url.clone());
        if let Some(inflight_posts) = config.inflight_posts {
            builder = builder.max_inflight_posts(inflight_posts.into());
        }
        if let Some(fail_on_save_error) = config.fail_on_save_error {
            builder = builder.fail_on_save_error(fail_on_save_error);
        }
        if let Some(retries_on_save_error) = config.retries_on_save_error {
            builder = builder.retries_on_save_error(retries_on_save_error);
        }

        builder
    }
}
