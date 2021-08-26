use std::ffi::OsStr;
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

use super::{Asagi, AsagiInner, AsagiStorage, AsagiTask, BoardOpts, Error};
use crate::SeaHashMap;

#[derive(Debug, Default)]
struct AsagiBuilderStorage {
    filesystem_config: Option<crate::config::AsagiFilesystemStorage>,
    s3_config: Option<crate::config::AsagiS3Storage>,
    b2_config: Option<crate::config::AsagiB2Storage>,
}

pub struct AsagiBuilder {
    boards: SeaHashMap<&'static str, BoardOpts>,
    board_configs: SeaHashMap<
        &'static str,
        (
            Option<crate::config::AsagiStorage>,
            Option<crate::config::AsagiStorage>,
        ),
    >,
    mysql_url: Option<String>,
    mysql_charset: String,
    media_path: Option<PathBuf>,
    without_triggers: bool,
    with_stats: bool,
    old_dir_structure: bool,
    sha_dir_structure: bool,
    tmp_dir: Option<PathBuf>,
    http_client: Option<reqwest::Client>,
    media_url: Option<reqwest::Url>,
    thumb_url: Option<reqwest::Url>,
    download_thumbs: bool,
    download_media: bool,
    concurrent_downloads: usize,
    inflight_posts: usize,
    fail_on_save_error: bool,
    retries_on_save_error: usize,
    media_backpressure: bool,
    truncate_fields: bool,
    sql_set_utc: bool,
    mysql_engine: String,
    storage: AsagiBuilderStorage,
    storage_thumbs: Option<AsagiBuilderStorage>,
}

impl Default for AsagiBuilder {
    fn default() -> Self {
        AsagiBuilder {
            boards: SeaHashMap::default(),
            board_configs: SeaHashMap::default(),
            mysql_url: None,
            mysql_charset: String::from("utf8mb4"),
            media_path: None,
            without_triggers: false,
            with_stats: false,
            old_dir_structure: false,
            sha_dir_structure: false,
            tmp_dir: None,
            http_client: None,
            media_url: None,
            thumb_url: None,
            download_thumbs: true,
            download_media: true,
            concurrent_downloads: 128,
            inflight_posts: usize::MAX,
            fail_on_save_error: true,
            retries_on_save_error: 0,
            media_backpressure: false,
            truncate_fields: true,
            sql_set_utc: true,
            mysql_engine: String::from("InnoDB"),
            storage: Default::default(),
            storage_thumbs: None,
        }
    }
}

impl AsagiBuilder {
    pub fn with_board<T: AsRef<str>>(
        mut self,
        board: T,
        save_thumbnails: bool,
        save_media: bool,
        media_config: Option<crate::config::AsagiStorage>,
        thumb_config: Option<crate::config::AsagiStorage>,
    ) -> Self {
        let static_board: &'static str = Box::leak(board.as_ref().to_string().into_boxed_str());
        self.boards.insert(
            static_board,
            BoardOpts {
                thumbs: save_thumbnails,
                media: save_media,
                ..Default::default()
            },
        );
        if media_config.is_some() || thumb_config.is_some() {
            self.board_configs
                .insert(static_board, (media_config, thumb_config));
        }

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

    pub fn with_sha_dir_structure(mut self, enable: bool) -> Self {
        self.sha_dir_structure = enable;
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

    pub fn fail_on_save_error(mut self, yes: bool) -> Self {
        self.fail_on_save_error = yes;
        self
    }

    pub fn retries_on_save_error(mut self, retries: usize) -> Self {
        self.retries_on_save_error = retries;
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

    pub fn sql_set_utc(mut self, yes: bool) -> Self {
        self.sql_set_utc = yes;
        self
    }

    pub fn set_mysql_engine<T: AsRef<str>>(mut self, engine: T) -> Self {
        self.mysql_engine = String::from(engine.as_ref());
        self
    }

    async fn new_storage(
        http_client: reqwest::Client,
        filesystem_config: Option<&crate::config::AsagiFilesystemStorage>,
        s3_config: Option<&crate::config::AsagiS3Storage>,
        b2_config: Option<&crate::config::AsagiB2Storage>,
    ) -> Result<AsagiStorage, Error> {
        let fs_storage = match &filesystem_config {
            Some(conf) if !conf.disabled => Some(
                super::storage::FileSystem::new(
                    &conf.media_path,
                    conf.tmp_dir
                        .clone()
                        .unwrap_or(std::env::temp_dir().join("torako")),
                    conf.web_unix_group.as_ref().map(|x| OsStr::new(x)),
                )
                .await?,
            ),
            _ => None,
        };

        let s3_storage = match s3_config.as_ref() {
            Some(conf) if !conf.disabled => Some(
                super::storage::S3::new(
                    &conf.access_key_id,
                    &conf.secret_access_key,
                    &conf.bucket,
                    &conf.region,
                    conf.endpoint.as_ref().map(|x| x.to_string()),
                    conf.acl.clone(),
                    conf.check_exists.unwrap_or(true),
                )
                .await?,
            ),
            _ => None,
        };

        let b2_storage = match b2_config.as_ref() {
            Some(conf) if !conf.disabled => Some(
                super::storage::Backblaze::new(
                    http_client,
                    &conf.bucket_id,
                    &conf.application_key_id,
                    &conf.application_key,
                    conf.check_exists.unwrap_or(true),
                    conf.bloom.clone(),
                )
                .await?,
            ),
            _ => None,
        };

        Ok(AsagiStorage {
            fs: fs_storage,
            s3: s3_storage,
            b2: b2_storage,
        })
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
        if let Some(m) = &self.media_path {
            info!("Asagi Filesystem Media Path: {:?}", m);
            tokio::fs::create_dir_all(m)
                .map_err(|_| Error::InvalidMediaDirectory)
                .await?;
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
                .replace("%%CHARSET%%", &self.mysql_charset)
                .replace("%%ENGINE%%", &self.mysql_engine);
            conn.query_drop(q).await?;
        }
        info!("Creating triggers (if needed)...");
        if self.without_triggers {
            warn!("Insert/Update triggers have been set to be disabled. This will drop any triggers you have set up with Asagi. Rerun torako with triggers enable to recreate these triggers.");
            for (board, _) in &self.boards {
                info!("Creating triggers for table '{}' (if needed)...", board);
                let q = include_str!("triggers_v2.sql")
                    .replace("%%BOARD%%", &board)
                    .replace("%%CHARSET%%", &self.mysql_charset)
                    .replace("%%ENGINE%%", &self.mysql_engine);
                conn.query_drop(q).await?;
            }
        } else {
            for (board, _) in &self.boards {
                info!("Creating triggers for table '{}' (if needed)...", board);
                let q = include_str!("triggers.sql")
                    .replace("%%BOARD%%", &board)
                    .replace("%%CHARSET%%", &self.mysql_charset)
                    .replace("%%ENGINE%%", &self.mysql_engine);
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

            let col: Option<String> = conn.exec_first("SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`= :table_schema AND `TABLE_NAME`= :table_name AND `COLUMN_NAME` = :column_name",
                params! {
                    "table_schema" => database_name.clone(),
                    "table_name" => format!("{}_images", board),
                    "column_name" => "media_sha256",
                }
            ).await?;
            if col.is_some() {
                opts.with_sha256 = true
            }
        }

        drop(conn);
        let http_client = self.http_client.unwrap_or(reqwest::Client::new());
        let (process_tx, process_rx) = tokio::sync::mpsc::unbounded_channel();

        let storage = Self::new_storage(
            http_client.clone(),
            self.storage.filesystem_config.as_ref(),
            self.storage.s3_config.as_ref(),
            self.storage.b2_config.as_ref(),
        )
        .await?;

        // if let Some(config) =
        let storage_thumbs = match self.storage_thumbs {
            Some(storage) => Some(
                Self::new_storage(
                    http_client.clone(),
                    storage.filesystem_config.as_ref(),
                    storage.s3_config.as_ref(),
                    storage.b2_config.as_ref(),
                )
                .await?,
            ),
            None => None,
        };

        for (board, config) in self.board_configs.iter() {
            if let Some(board_opt) = self.boards.get_mut(board) {
                if let Some(config) = config.0.as_ref() {
                    let storage = Self::new_storage(
                        http_client.clone(),
                        config.filesystem.as_ref(),
                        config.s3.as_ref(),
                        config.b2.as_ref(),
                    )
                    .await?;
                    board_opt.storage = Some(storage);
                }
                if let Some(config) = config.1.as_ref() {
                    let storage = Self::new_storage(
                        http_client.clone(),
                        config.filesystem.as_ref(),
                        config.s3.as_ref(),
                        config.b2.as_ref(),
                    )
                    .await?;
                    board_opt.storage_thumbs = Some(storage);
                }
            }
        }

        let tmp_dir = self.tmp_dir.unwrap_or(std::env::temp_dir().join("torako"));
        tokio::fs::create_dir_all(&tmp_dir).await?;

        let asagi = AsagiInner {
            client: http_client,
            media_url: self
                .media_url
                .unwrap_or("https://i.4cdn.org/".parse().unwrap()),
            thumb_url: self
                .thumb_url
                .unwrap_or("https://i.4cdn.org/".parse().unwrap()),
            download_thumbs: self.download_thumbs,
            download_media: self.download_media,
            boards: self.boards,
            without_triggers: self.without_triggers,
            with_stats: self.with_stats,
            direct_db_pool: pool,
            asagi_start_time: match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                Ok(n) => n.as_secs() as i64,
                Err(_) => panic!("SystemTime before UNIX EPOCH!"),
            },
            old_dir_structure: self.old_dir_structure,
            sha_dir_structure: self.sha_dir_structure,
            tmp_dir,
            fail_on_save_error: self.fail_on_save_error,
            retries_on_save_error: self.retries_on_save_error,
            max_concurrent_downloads: self.concurrent_downloads,
            max_inflight_posts: self.inflight_posts,
            media_backpressure: self.media_backpressure,
            sql_set_utc: self.sql_set_utc,
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
            storage,
            storage_thumbs,
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
        if let Some(sha_dir_structure) = config.sha_dir_structure {
            builder = builder.with_sha_dir_structure(sha_dir_structure);
        }
        if let Some(max_concurrent_downloads) = config.concurrent_downloads {
            builder = builder.max_concurrent_downloads(max_concurrent_downloads.into());
        }
        if let Some(inflight_posts) = config.inflight_posts {
            builder = builder.max_inflight_posts(inflight_posts.into());
        }
        if let Some(fail_on_save_error) = config.fail_on_save_error {
            builder = builder.fail_on_save_error(fail_on_save_error);
        }
        if let Some(retries_on_save_error) = config.retries_on_save_error {
            builder = builder.retries_on_save_error(retries_on_save_error);
        }
        if let Some(media_backpressure) = config.media_backpressure {
            builder = builder.media_backpressure(media_backpressure);
        }
        if let Some(truncate_fields) = config.database.truncate_fields {
            builder = builder.truncate_fields(truncate_fields);
        }
        if let Some(sql_set_utc) = config.database.sql_set_utc {
            builder = builder.sql_set_utc(sql_set_utc);
        }
        if let Some(mysql_engine) = &config.database.mysql_engine {
            builder = builder.set_mysql_engine(mysql_engine);
        }

        let filesystem_config = match config
            .media_storage
            .as_ref()
            .map(|x| x.filesystem.as_ref())
            .flatten()
        {
            Some(c) => {
                let mut c = c.clone();
                c.tmp_dir = c.tmp_dir.or(config.tmp_dir.clone());
                Some(c)
            }
            None => match &config.media_path {
                Some(m) => Some(crate::config::AsagiFilesystemStorage {
                    disabled: false,
                    media_path: m.clone(),
                    tmp_dir: config.tmp_dir.clone(),
                    web_unix_group: config.web_unix_group.clone(),
                }),
                None => None,
            },
        };
        builder.storage.filesystem_config = filesystem_config;
        builder.storage.s3_config = config
            .media_storage
            .as_ref()
            .map(|x| x.s3.as_ref())
            .flatten()
            .cloned();
        builder.storage.b2_config = config
            .media_storage
            .as_ref()
            .map(|x| x.b2.as_ref())
            .flatten()
            .cloned();

        if let Some(thumb_storage) = config.thumb_storage.as_ref() {
            let s = AsagiBuilderStorage {
                filesystem_config: thumb_storage.filesystem.clone(),
                s3_config: thumb_storage.s3.clone(),
                b2_config: thumb_storage.b2.clone(),
            };
            builder.storage_thumbs = Some(s);
        }

        builder
    }
}
