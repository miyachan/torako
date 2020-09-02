use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize},
    Arc,
};
use std::time::SystemTime;

use futures::future::Either;
use futures::prelude::*;
use futures::task::AtomicWaker;
use log::{info, warn};
use mysql_async::prelude::*;
use rustc_hash::FxHashMap;

use super::{Asagi, AsagiInner, AsagiTask, BoardOpts, Error};

pub struct AsagiBuilder {
    boards: FxHashMap<String, BoardOpts>,
    mysql_url: Option<String>,
    mysql_charset: String,
    media_path: Option<PathBuf>,
    without_triggers: bool,
    with_stats: bool,
    old_dir_structure: bool,
    web_unix_group: Option<OsString>,
    tmp_dir: Option<PathBuf>,
    http_client: Option<reqwest::Client>,
    media_url: Option<reqwest::Url>,
    thumb_url: Option<reqwest::Url>,
    download_thumbs: bool,
    download_media: bool,
    concurrent_downloads: usize,
    inflight_posts: usize,
    persist_is_fatal: bool,
    media_backpressure: bool,
    truncate_fields: bool,
}

impl Default for AsagiBuilder {
    fn default() -> Self {
        AsagiBuilder {
            boards: FxHashMap::default(),
            mysql_url: None,
            mysql_charset: String::from("utf8mb4"),
            media_path: None,
            without_triggers: false,
            with_stats: false,
            old_dir_structure: false,
            web_unix_group: None,
            tmp_dir: None,
            http_client: None,
            media_url: None,
            thumb_url: None,
            download_thumbs: true,
            download_media: true,
            concurrent_downloads: 128,
            inflight_posts: usize::MAX,
            persist_is_fatal: true,
            media_backpressure: false,
            truncate_fields: true,
        }
    }
}

impl AsagiBuilder {
    pub fn with_board<T: AsRef<str>>(
        mut self,
        board: T,
        save_thumbnails: bool,
        save_media: bool,
    ) -> Self {
        self.boards.insert(
            String::from(board.as_ref()),
            BoardOpts {
                thumbs: save_thumbnails,
                media: save_media,
                ..Default::default()
            },
        );

        self
    }

    pub fn with_mysql_database<T: AsRef<str>>(mut self, database_url: T) -> Self {
        self.mysql_url = Some(String::from(database_url.as_ref()));
        self
    }

    pub fn with_mysql_charset<T: AsRef<str>>(mut self, charset: T) -> Self {
        self.mysql_charset = String::from(charset.as_ref());
        self
    }

    pub fn with_media_path<T: AsRef<Path>>(mut self, path: T) -> Self {
        self.media_path = Some(PathBuf::from(path.as_ref()));
        self
    }

    pub fn with_tmp_path<T: AsRef<Path>>(mut self, path: T) -> Self {
        self.tmp_dir = Some(PathBuf::from(path.as_ref()));
        self
    }

    pub fn with_http_client(mut self, client: reqwest::Client) -> Self {
        self.http_client = Some(client);
        self
    }

    pub fn with_media_url<T: reqwest::IntoUrl>(mut self, url: T) -> Self {
        self.media_url = Some(url.into_url().unwrap());
        self
    }

    pub fn with_thumb_url<T: reqwest::IntoUrl>(mut self, url: T) -> Self {
        self.thumb_url = Some(url.into_url().unwrap());
        self
    }

    pub fn download_thumbs(mut self, enable: bool) -> Self {
        self.download_thumbs = enable;
        self
    }

    pub fn download_media(mut self, enable: bool) -> Self {
        self.download_media = enable;
        self
    }

    pub fn with_triggers(mut self, triggers: bool) -> Self {
        self.without_triggers = !triggers;
        self
    }

    pub fn with_old_dir_structure(mut self, enable: bool) -> Self {
        self.old_dir_structure = enable;
        self
    }

    pub fn compute_stats(mut self, stats: bool) -> Self {
        self.with_stats = stats;
        self
    }

    pub fn max_concurrent_downloads(mut self, downloads: usize) -> Self {
        self.concurrent_downloads = downloads;
        self
    }

    pub fn max_inflight_posts(mut self, posts: usize) -> Self {
        self.inflight_posts = posts;
        self
    }

    pub fn persist_error_is_fatal(mut self, yes: bool) -> Self {
        self.persist_is_fatal = yes;
        self
    }

    pub fn media_backpressure(mut self, yes: bool) -> Self {
        self.media_backpressure = yes;
        self
    }

    pub fn truncate_fields(mut self, yes: bool) -> Self {
        self.truncate_fields = yes;
        self
    }

    #[cfg(target_family = "windows")]
    pub fn media_unix_group<T: AsRef<OsStr>>(mut self, _group: T) -> Self {
        self.web_unix_group = None;
        self
    }

    #[cfg(not(target_family = "windows"))]
    pub fn media_unix_group<T: AsRef<OsStr>>(mut self, group: T) -> Self {
        self.web_unix_group = Some(OsString::from(group.as_ref()));
        self
    }

    #[cfg(target_family = "windows")]
    fn group(&self) -> Result<(), Error> {
        Ok(())
    }

    #[cfg(not(target_family = "windows"))]
    fn group(&self) -> Result<Option<users::Group>, Error> {
        self.web_unix_group
            .as_ref()
            .map(|g| users::get_group_by_name(g).ok_or(Error::InvalidUserGroup))
            .transpose()
    }

    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    fn io_ring(&self) -> rio::Rio {
        rio::new().expect("create uring")
    }

    #[cfg(not(all(feature = "io-uring", target_os = "linux")))]
    fn io_ring(&self) -> () {
        ()
    }

    pub async fn build(mut self) -> Result<Asagi, Error> {
        if self.boards.len() == 0 {
            return Err(Error::NoBoards);
        }
        if self.with_stats && !self.without_triggers {
            return Err(Error::InvalidStatsNoTriggers);
        }
        info!("Initializing Asagi...");
        info!(
            "Asagi Boards: {}",
            self.boards
                .iter()
                .map(|x| x.0.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        match &self.media_path {
            Some(m) => {
                info!("Asagi Media Path: {:?}", m);
                tokio::fs::create_dir_all(m)
                    .map_err(|_| Error::InvalidMediaDirectory)
                    .await?;
            }
            None => info!("Media saving disabled"),
        }
        let mysql_url = match self.mysql_url.as_ref() {
            Some(m) => m,
            None => return Err(Error::InvalidDatabase),
        };
        info!("Connecting to MySQL...");
        let pool = mysql_async::Pool::new(mysql_url);
        let mut conn = pool.get_conn().await?;

        info!("Creating common tables (if needed)...");
        conn.query_drop(include_str!("common.sql")).await?;
        info!("Creating board tables (if needed)...");
        for (board, _) in &self.boards {
            info!("Creating board table '{}' (if needed)...", board);
            let q = include_str!("boards.sql")
                .replace("%%BOARD%%", &board)
                .replace("%%CHARSET%%", &self.mysql_charset);
            conn.query_drop(q).await?;
        }
        info!("Creating triggers (if needed)...");
        if self.without_triggers {
            warn!("Insert/Update triggers have been set to be disabled. This will drop any triggers you have set up with Asagi. Rerun torako with triggers enable to recreate these triggers.");
            for (board, _) in &self.boards {
                info!("Creating triggers for table '{}' (if needed)...", board);
                let q = include_str!("triggers_v2.sql")
                    .replace("%%BOARD%%", &board)
                    .replace("%%CHARSET%%", &self.mysql_charset);
                conn.query_drop(q).await?;
            }
        } else {
            for (board, _) in &self.boards {
                info!("Creating triggers for table '{}' (if needed)...", board);
                let q = include_str!("triggers.sql")
                    .replace("%%BOARD%%", &board)
                    .replace("%%CHARSET%%", &self.mysql_charset);
                conn.query_drop(q).await?;
            }
        }
        let database_name = conn.opts().db_name().unwrap().to_owned();
        for (board, opts) in (&mut self).boards.iter_mut() {
            let col: Option<String> = conn.exec_first("SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`= :table_schema AND `TABLE_NAME`= :table_name AND `COLUMN_NAME` = :column_name",
                params! {
                    "table_schema" => database_name.clone(),
                    "table_name" => board,
                    "column_name" => "unix_timestamp",
                }
            ).await?;
            if col.is_some() {
                opts.with_unix_timestamp = true
            }
        }

        drop(conn);

        let tmp_dir = match self.tmp_dir.clone() {
            Some(t) => t,
            None => std::env::temp_dir().join("torako"),
        };

        match tokio::fs::metadata(&tmp_dir).await {
            Ok(m) => {
                if !m.is_dir() {
                    return Err(Error::InvalidTempDir(tmp_dir.to_owned()));
                }
            }
            Err(_) => tokio::fs::create_dir_all(&tmp_dir).await?,
        };

        let (process_tx, process_rx) = tokio::sync::mpsc::unbounded_channel();

        let asagi = AsagiInner {
            io_ring: self.io_ring(),
            media_group: self.group()?,
            client: self.http_client.unwrap_or(reqwest::Client::new()),
            media_url: self
                .media_url
                .unwrap_or("https://i.4cdn.org/".parse().unwrap()),
            thumb_url: self
                .thumb_url
                .unwrap_or("https://i.4cdn.org/".parse().unwrap()),
            download_thumbs: self.download_thumbs,
            download_media: self.download_media,
            tmp_dir,
            boards: self.boards,
            without_triggers: self.without_triggers,
            media_path: self.media_path,
            with_stats: self.with_stats,
            db_pool: pool,
            asagi_start_time: match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                Ok(n) => n.as_secs() as i64,
                Err(_) => panic!("SystemTime before UNIX EPOCH!"),
            },
            old_dir_structure: self.old_dir_structure,
            persist_fatal: self.persist_is_fatal,
            max_concurrent_downloads: self.concurrent_downloads,
            max_inflight_posts: self.inflight_posts,
            media_backpressure: self.media_backpressure,
            truncate_fields: self.truncate_fields,
            concurrent_downloads: AtomicUsize::new(0),
            download_tokens: tokio::sync::Semaphore::new(self.concurrent_downloads),
            inflight_posts: AtomicUsize::new(0),
            inflight_media: AtomicUsize::new(0),
            waker: Arc::new(AtomicWaker::new()),
            flush_waker: Arc::new(AtomicWaker::new()),
            close_waker: Arc::new(AtomicWaker::new()),
            failed: AtomicBool::new(false),
            metrics: Arc::new(super::AsagiMetrics::default()),
            process_tx,
        };

        let asagi = Arc::new(asagi);
        let asagi2 = asagi.clone();

        tokio::spawn(
            process_rx
                .take_while(|x| future::ready(x.is_some()))
                .zip(stream::repeat(asagi2))
                .map(|(x, asagi2)| match x {
                    AsagiTask::Posts(p) => Either::Left(asagi2.send_posts(p)),
                    AsagiTask::Media(a, b) => {
                        Either::Right(asagi2.retry_save_media(a, b).map(|_| ()))
                    }
                    AsagiTask::Closed => unreachable!(),
                })
                .buffer_unordered(usize::MAX)
                .for_each(|_| future::ready(())),
        );

        Ok(Asagi { inner: asagi })
    }
}

impl From<&crate::config::Asagi> for AsagiBuilder {
    fn from(config: &crate::config::Asagi) -> Self {
        let mut builder = AsagiBuilder::default();
        builder = builder.with_mysql_database(config.database.url.to_string());
        if let Some(charset) = &config.database.charset {
            builder = builder.with_mysql_charset(charset);
        }
        if let Some(media_path) = &config.media_path {
            builder = builder.with_media_path(media_path);
        }
        if let Some(tmp_dir) = &config.tmp_dir {
            builder = builder.with_tmp_path(tmp_dir);
        }
        if let Some(media_url) = &config.media_url {
            builder = builder.with_media_url(media_url.clone());
        }
        if let Some(thumb_url) = &config.thumb_url {
            builder = builder.with_thumb_url(thumb_url.clone());
        }
        if let Some(download_thumbs) = config.thumbs {
            builder = builder.download_thumbs(download_thumbs);
        }
        if let Some(download_media) = config.media {
            builder = builder.download_media(download_media);
        }
        if let Some(use_triggers) = config.database.use_triggers {
            builder = builder.with_triggers(use_triggers);
        }
        if let Some(compute_stats) = config.database.compute_stats {
            builder = builder.compute_stats(compute_stats);
        }
        if let Some(old_dir_structure) = config.old_dir_structure {
            builder = builder.with_old_dir_structure(old_dir_structure);
        }
        if let Some(max_concurrent_downloads) = config.concurrent_downloads {
            builder = builder.max_concurrent_downloads(max_concurrent_downloads.into());
        }
        if let Some(inflight_posts) = config.inflight_posts {
            builder = builder.max_inflight_posts(inflight_posts.into());
        }
        if let Some(persist_error_is_fatal) = config.persist_error_is_fatal {
            builder = builder.persist_error_is_fatal(persist_error_is_fatal);
        }
        if let Some(web_unix_group) = &config.web_unix_group {
            builder = builder.media_unix_group(web_unix_group);
        }
        if let Some(media_backpressure) = config.media_backpressure {
            builder = builder.media_backpressure(media_backpressure);
        }
        if let Some(truncate_fields) = config.database.truncate_fields {
            builder = builder.truncate_fields(truncate_fields);
        }

        builder
    }
}
