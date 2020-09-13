use std::collections::hash_map::Entry;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use backoff::backoff::Backoff;
use futures::future::Either;
use futures::prelude::*;
use futures::task::AtomicWaker;
use log::{debug, error, info, warn};
use mysql_async::{prelude::*, TxOpts};
use rand::Rng;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::Serialize;
use thiserror::Error;
use tokio::io::AsyncWriteExt;

mod builder;
mod db_metrics;
mod interval_lock;
mod stats;
pub mod storage;
mod thread;

use crate::imageboard;
pub use builder::AsagiBuilder;
use thread::Thread;

const MYSQL_ERR_CODE_DEADLOCK: u16 = 1213;

#[derive(Debug, Error)]
pub enum Error {
    #[error("no boards selected")]
    NoBoards,
    #[error("invalid database URL provided")]
    InvalidDatabase,
    #[error("invalid media directory")]
    InvalidMediaDirectory,
    #[error("triggers must be disabled to generate stats within Torako")]
    InvalidStatsNoTriggers,
    #[error("A unix user group was set however the group could not be found.")]
    #[cfg(not(target_family = "windows"))]
    InvalidUserGroup,
    #[error("Asagi was told to use the old directory structure which doesn't support low thread numbers.")]
    InvalidThreadOldDir,
    #[error("The image had an invalid content length: {}: {}", .1, .0)]
    ContentLength(String, reqwest::StatusCode),
    #[error("A fatal error occured when trying to archive posts")]
    ArchiveError,
    #[error("A database deadlock error occured")]
    Deadlock,
    #[error("Failed to configure Backblaze: {}", .0)]
    B2Config(&'static str),
    #[error("Failed to authorize against Backblaze")]
    B2Unauthorized,
    #[error("A fatal error occured trying to communicate with Backblaze: {}", .0)]
    B2(reqwest::Error),
    #[error("A fatal error occured trying to communicate with S3: {}", .0)]
    S3(String),
    #[error("A fatal error occured trying to communicate with S3: {}", .0)]
    Rusoto(Box<dyn std::error::Error + Send + Sync>),
    #[error("Invalid Temporary Directory: {:?}", .0)]
    InvalidTempDir(PathBuf),
    #[error("The request was blocked by Cloudflare's bot detection")]
    CloudFlareBlocked(reqwest::Error),
    #[error("mysql error: {}", .0)]
    MySQL(#[from] mysql_async::Error),
    #[error("io error: {}", .0)]
    IO(#[from] std::io::Error),
    #[error("other io error")]
    OtherIO,
    #[error("http error: {}", .0)]
    Http(#[from] reqwest::Error),
}

impl Error {
    fn is_deadlock(&self) -> bool {
        if let Error::MySQL(err) = self {
            if let mysql_async::Error::Server(err) = err {
                if err.code == MYSQL_ERR_CODE_DEADLOCK {
                    return true;
                }
            }
        }
        false
    }

    fn is_request_failure(&self) -> bool {
        match self {
            Error::Http(err) => err.is_body() || err.is_request() || err.is_timeout(),
            Error::CloudFlareBlocked(_) => true,
            _ => false,
        }
    }

    fn is_cloudflare(&self) -> bool {
        match self {
            Error::CloudFlareBlocked(_) => true,
            _ => false,
        }
    }

    fn is_s3(&self) -> bool {
        match self {
            Error::S3(_) => true,
            Error::Rusoto(_) => true,
            _ => false,
        }
    }

    fn is_b2(&self) -> bool {
        match self {
            Error::B2(_) => true,
            Error::B2Unauthorized => true,
            _ => false,
        }
    }

    fn try_downcast(self) -> Self {
        match self {
            Error::IO(err) => match err.kind() {
                std::io::ErrorKind::Other => match err.into_inner() {
                    Some(err) => match err.downcast::<Error>() {
                        Ok(err) => *err,
                        Err(err) => Error::IO(std::io::Error::new(std::io::ErrorKind::Other, err)),
                    },
                    None => Error::OtherIO,
                },
                _ => Error::from(err),
            },
            _ => self,
        }
    }
}

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
enum MediaKind {
    Thumb,
    Image,
}

#[derive(Eq, PartialEq, Clone, Debug)]
struct MediaDescriptor {
    board: &'static str,
    hash: smallstr::SmallString<[u8; 32]>,
    is_op: bool,
    thread_no: u64,
    preview: Option<String>,
}

impl MediaDescriptor {
    fn new(post: &imageboard::Post, preview: Option<String>) -> Self {
        Self {
            board: post.board,
            hash: smallstr::SmallString::from_str(post.md5.as_ref().unwrap()),
            is_op: post.is_op(),
            thread_no: post.thread_no(),
            preview,
        }
    }
}

impl Hash for MediaDescriptor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.board.hash(state);
        self.hash.hash(state);
        self.is_op.hash(state);
        self.thread_no.hash(state);
    }
}

#[derive(Default)]
struct BoardOpts {
    thumbs: bool,
    media: bool,
    with_unix_timestamp: bool,
    interval_lock: interval_lock::IntervalLock,
}

#[derive(Debug, Serialize)]
pub struct Metrics {
    pub posts: u64,
    pub thumbs: u64,
    pub media: u64,
    pub bytes_downloaded: u64,
    pub inflight_posts: usize,
    pub concurrent_downloads: usize,
    pub inflight_media: usize,
    pub cloudflare_blocked: u64,
    pub save_errors: u64,
    pub database: Option<db_metrics::DatabaseMetrics>,
}

#[derive(Default, Debug)]
struct AsagiMetrics {
    posts: AtomicU64,
    thumbs: AtomicU64,
    media: AtomicU64,
    bytes_downloaded: AtomicU64,
    cf_blocked: AtomicU64,
    save_errors: AtomicU64,
}

impl AsagiMetrics {
    pub fn incr_posts(&self, count: u64) {
        self.posts.fetch_add(count, Ordering::Relaxed);
    }

    pub fn incr_thumbs(&self, count: u64) {
        self.thumbs.fetch_add(count, Ordering::Relaxed);
    }

    pub fn incr_media(&self, count: u64) {
        self.media.fetch_add(count, Ordering::Relaxed);
    }

    pub fn incr_bytes(&self, count: u64) {
        self.bytes_downloaded.fetch_add(count, Ordering::Relaxed);
    }

    pub fn incr_cfblocked(&self, count: u64) {
        self.cf_blocked.fetch_add(count, Ordering::Relaxed);
    }

    pub fn incr_save_error(&self, count: u64) {
        self.save_errors.fetch_add(count, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct AsagiMetricsProvider {
    inner: Arc<AsagiInner>,
}

impl super::MetricsProvider for AsagiMetricsProvider {
    fn name(&self) -> &'static str {
        "asagi"
    }

    fn metrics(
        &self,
    ) -> Pin<Box<dyn std::future::Future<Output = Box<dyn erased_serde::Serialize + Send>> + Send>>
    {
        let mut m = Metrics {
            posts: self.inner.metrics.posts.load(Ordering::Acquire),
            thumbs: self.inner.metrics.thumbs.load(Ordering::Acquire),
            media: self.inner.metrics.media.load(Ordering::Acquire),
            bytes_downloaded: self.inner.metrics.bytes_downloaded.load(Ordering::Acquire),
            inflight_posts: self.inner.inflight_posts.load(Ordering::Acquire),
            concurrent_downloads: self.inner.max_concurrent_downloads
                - self.inner.download_tokens.available_permits(),
            inflight_media: self.inner.inflight_media.load(Ordering::Acquire),
            cloudflare_blocked: self.inner.metrics.cf_blocked.load(Ordering::Acquire),
            save_errors: self.inner.metrics.save_errors.load(Ordering::Acquire),
            database: None,
        };

        let boards: Vec<&'static str> = self.inner.boards.iter().map(|b| *b.0).collect();
        db_metrics::database_metrics(self.inner.clone(), boards)
            .map(move |metrics| {
                m.database = metrics.ok();
                let m: Box<dyn erased_serde::Serialize + Send> = Box::new(m);
                m
            })
            .boxed()
    }
}

struct AsagiInner {
    client: reqwest::Client,
    boards: FxHashMap<&'static str, BoardOpts>,
    without_triggers: bool,
    with_stats: bool,
    direct_db_pool: mysql_async::Pool,
    asagi_start_time: i64,
    old_dir_structure: bool,
    media_url: reqwest::Url,
    thumb_url: reqwest::Url,
    download_thumbs: bool,
    download_media: bool,
    max_concurrent_downloads: usize,
    max_inflight_posts: usize,
    media_backpressure: bool,
    fail_on_save_error: bool,
    retries_on_save_error: usize,
    truncate_fields: bool,
    sql_set_utc: bool,

    failed: AtomicBool,
    concurrent_downloads: AtomicUsize,
    download_tokens: tokio::sync::Semaphore,
    inflight_posts: AtomicUsize,
    inflight_media: AtomicUsize,
    waker: Arc<AtomicWaker>,
    flush_waker: Arc<AtomicWaker>,
    close_waker: Arc<AtomicWaker>,
    metrics: Arc<AsagiMetrics>,
    process_tx: tokio::sync::mpsc::UnboundedSender<AsagiTask>,

    fs_storage: Option<storage::FileSystem>,
    s3_storage: Option<storage::S3>,
    b2_storage: Option<storage::Backblaze>,
}

#[derive(Debug)]
enum AsagiTask {
    Posts(Vec<imageboard::Post>),
    Media(thread::Media, MediaDescriptor),
    Closed,
}

impl AsagiTask {
    fn is_some(&self) -> bool {
        match self {
            AsagiTask::Posts(_) => true,
            AsagiTask::Media(_, _) => true,
            AsagiTask::Closed => false,
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Asagi {
    inner: Arc<AsagiInner>,
}

impl Asagi {
    #[allow(dead_code)]
    pub fn builder() -> AsagiBuilder {
        AsagiBuilder::default()
    }

    pub fn metrics_provider(&self) -> impl super::MetricsProvider {
        AsagiMetricsProvider {
            inner: self.inner.clone(),
        }
    }
}

impl AsagiInner {
    fn get_db_conn(&self) -> impl Future<Output = mysql_async::Result<mysql_async::Conn>> {
        match self.sql_set_utc {
            true => Either::Left(self.direct_db_pool.get_conn().and_then(|mut conn| async {
                conn.query_drop("SET time_zone='+00:00';").await?;
                Ok(conn)
            })),
            false => Either::Right(self.direct_db_pool.get_conn()),
        }
    }

    /// Persist a list posts to the database. This function is
    /// designed to be compatible with the Asagi imageboard archiver.
    ///
    /// Depending on how Asagi is configured. If it is configured
    /// to not depend on triggers, it will make batch inserts to
    /// derivative tables.
    ///
    /// Returns a list of media hash ids. These hash ids can be
    /// used to begin the image archival process.
    async fn save_posts(
        &self,
        mut all_posts: Vec<imageboard::Post>,
    ) -> Result<FxHashSet<MediaDescriptor>, Error> {
        let mut processed_posts = all_posts.len();
        let trunc_100 = |v| self.truncate_meta_field(v);
        let scope = async {
            let mut images_to_save = FxHashSet::default();
            let mut conn = self.get_db_conn().await?;
            while all_posts.len() > 0 {
                let save_start = Instant::now();
                let board = all_posts[0].board;
                let (mut posts, remaining): (Vec<imageboard::Post>, Vec<imageboard::Post>) =
                    all_posts.into_iter().partition(|p| p.board == board);
                all_posts = remaining;

                // Trying to insert more than 2,000 posts at once will
                // generate more than 2^16 placeholders which will cause the
                // insert to fail
                if posts.len() > 2000 {
                    all_posts.extend(posts.split_off(2000));
                }

                /*
                 * When inserting many posts concurrently
                 * MySQL will take a gap lock over the index we are working
                 * with. If two concurrent sets of posts are being updated
                 * we may hit a deadlock in the transaction.
                 *
                 * Normally we just retry the transacion, but ensuring
                 * we do not have overlapping sets of posts being updated
                 * reduces the amount of times we hit this deadlock.
                 * This heavily affects us during startup when we insert
                 * a lot of posts.
                 */
                let post_range = posts.iter().fold((u64::MAX, u64::MIN), |acc, x| {
                    (acc.0.min(x.no), acc.1.max(x.no))
                });

                let with_unix_timestamp = self.boards.get(board).unwrap().with_unix_timestamp;

                /*
                 * In order for our stats to work we assume that every post
                 * we see is brand new OR has the `is_retransmission` flag
                 * set. We can assume that every post that was created
                 * after Asagi has started is not in the database.
                 * However for posts found before asagi is started,
                 * we must check the database to see if it has been inserted.
                 *
                 * For these posts we take the slightly slower path of looking
                 * up each post. Ultimately, this is done so Torako can
                 * be responsible for building stats which moves some of the
                 * CPU load from MySQL to Torako (useful if you have other
                 * services pounding MySQL.
                 *
                 * If you are running in triggers mode, then we assume MySQL
                 * will handle this.
                 */
                if self.without_triggers {
                    let old_posts = posts
                        .iter()
                        .enumerate()
                        .filter_map(|(i, post)| {
                            if post.time < (self.asagi_start_time as u64) {
                                Some((post.no, i))
                            } else {
                                None
                            }
                        })
                        .collect::<FxHashMap<_, _>>();
                    if old_posts.len() > 0 {
                        debug!(
                            "Looking up {} old posts on board {}",
                            old_posts.len(),
                            board
                        );
                        let values = old_posts
                            .keys()
                            .map(|post_no| mysql_async::Value::from(post_no))
                            .collect::<Vec<_>>();
                        let placeholders = std::iter::once("?")
                            .chain((0..values.len() - 1).map(|_| ", ?"))
                            .collect::<String>();
                        let stmt = format!(
                            "SELECT num FROM `{}` WHERE num IN ({}) AND subnum = 0",
                            board, placeholders
                        );
                        conn.exec_map(stmt.as_str(), values, |post_no| {
                            posts[old_posts[&post_no]].is_retransmission = true;
                        })
                        .await?;
                    }
                }
                let posts = posts;
                let mut backoff = backoff::ExponentialBackoff::default();
                backoff.max_elapsed_time = None;
                let mut attempts = 0;
                loop {
                    debug!("Locking range: {:?}", post_range);
                    let interval_lock = self
                        .boards
                        .get(board)
                        .unwrap()
                        .interval_lock
                        .acquire(post_range)
                        .await;

                    let mut tx = conn.start_transaction(TxOpts::default()).await?;
                    let result = async {
                        let mut media_map = FxHashMap::default();
                        let media_params = posts
                            .iter()
                            .filter(|p| {
                                p.has_media()
                                    && self.without_triggers
                                    && !(p.is_retransmission || p.deleted)
                            })
                            .enumerate()
                            .filter_map(|(i, post)| {
                                match media_map.entry(post.md5.clone().unwrap()) {
                                    Entry::Occupied(_) => return None,
                                    Entry::Vacant(v) => {
                                        v.insert(i);
                                    }
                                };
                                let (preview_op, preview_reply) = if post.is_op() {
                                    (post.preview_orig().clone(), None)
                                } else {
                                    (None, post.preview_orig().clone())
                                };
                                let values: Box<[mysql_async::Value]> = Box::new([
                                    post.md5.clone().unwrap().into(),  // media_hash
                                    post.media_orig().unwrap().into(), // media
                                    preview_op.into(),                 // preview_op
                                    preview_reply.into(),              // preview_reply
                                    1usize.into(),                     // total
                                ]);
                                Some(values.into_vec())
                            })
                            .flatten()
                            .collect::<Vec<_>>();

                        let id_lowmark = if self.without_triggers && media_params.len() > 0 {
                            let rows = media_params.len() / 5;

                            /*
                             * When batch inserting into MySQL, the generated IDs are
                             * guaranteed to be monotonically increasing for a table
                             * with an autoincrement ID.
                             *
                             * Using this, by having the lowest id (via last_insert_id)
                             * we can find the generated id for our media_image hash
                             * by looking up the order it was inserted in, and adding
                             * the lowest id
                             */
                            let media_image_table = format!("INSERT INTO `{}_images` (media_hash, media, preview_op, preview_reply, total) VALUES (?, ?, ?, ?, ?)", &board);
                            let media_query = std::iter::once(media_image_table.as_str())
                                .chain((0..(rows - 1)).map(|_| ", (?, ?, ?, ?, ?)"))
                                .chain(std::iter::once(
                                    " ON DUPLICATE KEY UPDATE
                                        media_id = LAST_INSERT_ID(media_id),
                                        total = (total + 1),
                                        preview_op = COALESCE(preview_op, VALUES(preview_op)),
                                        preview_reply = COALESCE(preview_reply, VALUES(preview_reply)),
                                        media = COALESCE(media, VALUES(media));",
                                ))
                                .collect::<String>();

                            let media_results = match tx
                                .exec_iter(media_query.as_str(), media_params)
                                .map_err(|err| Error::from(err))
                                .await
                            {
                                Ok(r) => r,
                                Err(ref err) if err.is_deadlock() => {
                                    debug!("Hit MySQL deadlock when processing board images on board {}. Retrying...", board);
                                    return Err(Error::Deadlock);
                                }
                                Err(err) => return Err(err),
                            };

                            media_results.last_insert_id().unwrap() as usize
                        } else {
                            0
                        };

                        let mut threads: FxHashMap<u64, Thread> = FxHashMap::default();
                        let mut daily: FxHashMap<u64, stats::Daily> = FxHashMap::default();
                        let mut users: FxHashMap<String, stats::User> = FxHashMap::default();
                        let mut media_ids: FxHashSet<MediaDescriptor> = FxHashSet::default();

                        let rows = posts.len();
                        let board_values = posts
                            .iter()
                            .map(|post| {
                                if self.boards.get(post.board).is_none() {
                                    panic!(
                                "Received board {} without being configured for it. This is fatal.",
                                post.board
                            );
                                }
                                if self.without_triggers
                                    && !(post.deleted || post.is_retransmission)
                                {
                                    match threads.entry(post.thread_no()) {
                                        Entry::Occupied(mut v) => v.get_mut().update(&post),
                                        Entry::Vacant(v) => {
                                            let mut t = Thread::new(post.thread_no());
                                            t.update(&post);
                                            v.insert(t);
                                        }
                                    };
                                    if self.with_stats {
                                        let day = ((post.nyc_timestamp() / 86400) * 86400) as u64;
                                        match daily.entry(day) {
                                            Entry::Occupied(mut v) => v.get_mut().update(&post),
                                            Entry::Vacant(v) => {
                                                let mut d = stats::Daily::new(day);
                                                d.update(&post);
                                                v.insert(d);
                                            }
                                        };

                                        if let Some(user) = post.poster_nametrip() {
                                            match users.entry(user) {
                                                Entry::Occupied(mut v) => v.get_mut().update(&post),
                                                Entry::Vacant(v) => {
                                                    let username = post.name.clone();
                                                    let trip = post.trip.clone();
                                                    let mut u = stats::User::new(username, trip);
                                                    u.update(&post);
                                                    v.insert(u);
                                                }
                                            };
                                        }
                                    }
                                }

                                if post.has_media() {
                                    let preview = match self.old_dir_structure {
                                        true => Some(post.preview_orig().as_ref().unwrap().clone()),
                                        false => None,
                                    };
                                    media_ids.insert(MediaDescriptor::new(&post, preview));
                                }

                                let media_id = post
                                    .md5
                                    .as_ref()
                                    .map(|md5| media_map.get(md5))
                                    .flatten()
                                    .map(|idx| idx + id_lowmark)
                                    .unwrap_or(0);

                                // have to move these values out
                                // to appease the borrow checker
                                let poster_name =
                                    mysql_async::Value::from(post.poster_name().map(trunc_100));
                                let post_title =
                                    mysql_async::Value::from(post.title().map(trunc_100));
                                let poster_email = mysql_async::Value::from(
                                    post.email.as_ref().cloned().map(trunc_100),
                                );
                                let poster_hash = mysql_async::Value::from(post.poster_hash());
                                let poster_country =
                                    mysql_async::Value::from(post.poster_country());
                                let media_orig = mysql_async::Value::from(post.media_orig());
                                let capcode = mysql_async::Value::from(post.short_capcode());
                                let exif = mysql_async::Value::from(post.exif());
                                let comment = mysql_async::Value::from(post.comment());
                                let deleted_at =
                                    mysql_async::Value::from(post.deleted_at.unwrap_or(0));
                                let unix_timestamp = match post.datetime() {
                                    Some(d) => d,
                                    None => chrono::NaiveDateTime::from_timestamp(1, 0),
                                };
                                let values: Box<[mysql_async::Value]> = Box::new([
                                    post.no.into(),               // num
                                    0usize.into(),                // subnum,
                                    post.thread_no().into(),      // thread_num,
                                    post.is_op().into(),          // op,
                                    post.nyc_timestamp().into(),  // timestamp,
                                    deleted_at,                   // timestamp_expired
                                    post.preview_orig().into(),   // preview_orig,
                                    post.tn_w.into(),             // preview_w
                                    post.tn_h.into(),             // preview_h,
                                    media_id.into(),              // media_id
                                    post.media_filename().into(), // media_filename,
                                    post.w.into(),                // media_w,
                                    post.h.into(),                // media_h,
                                    post.fsize.into(),            // media_size,
                                    post.md5.as_ref().into(),     // media_hash,
                                    media_orig,                   // media_orig,
                                    post.spoiler.into(),          // spoiler,
                                    post.deleted.into(),          // deleted,
                                    capcode,                      // capcode,
                                    poster_email,                 // email,
                                    poster_name,                  // name
                                    post.trip.as_ref().into(),    // trip
                                    post_title,                   // title
                                    comment,                      // comment
                                    mysql_async::Value::NULL,     // delpass,
                                    post.sticky.into(),           // sticky
                                    post.closed.into(),           // locked
                                    poster_hash,                  // poster_hash,
                                    poster_country,               // poster_country,
                                    exif,                         // exif
                                    unix_timestamp.into(),        // unix_timestamp
                                ]);
                                match with_unix_timestamp {
                                    true => values.into_vec(),
                                    false => {
                                        let mut v = values.into_vec();
                                        v.truncate(v.len() - 1);
                                        v
                                    }
                                }
                            })
                            .flatten()
                            .collect::<Vec<_>>();

                        let board_table = match with_unix_timestamp {
                            true => format!("INSERT INTO `{}` (num, subnum, thread_num, op, timestamp, timestamp_expired, preview_orig,
                                                preview_w, preview_h, media_id, media_filename, media_w, media_h, media_size,
                                                media_hash, media_orig, spoiler, deleted, capcode, email, name, trip, title, comment,
                                                delpass, sticky, locked, poster_hash, poster_country, exif, unix_timestamp
                                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", &board),
                            false => format!("INSERT INTO `{}` (num, subnum, thread_num, op, timestamp, timestamp_expired, preview_orig,
                                                preview_w, preview_h, media_id, media_filename, media_w, media_h, media_size,
                                                media_hash, media_orig, spoiler, deleted, capcode, email, name, trip, title, comment,
                                                delpass, sticky, locked, poster_hash, poster_country, exif
                                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", &board),
                        };
                        let placeholders = match with_unix_timestamp {
                            true => ", (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            false => ", (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        };
                        let board_query = std::iter::once(board_table.as_str())
                            .chain((0..(rows - 1)).map(|_| placeholders))
                            .chain(
                                std::iter::once(
                                    " ON DUPLICATE KEY UPDATE
                                    deleted = VALUES(deleted),
                                    timestamp_expired = COALESCE(NULLIF(VALUES(timestamp_expired), 0), timestamp_expired),
                                    sticky = COALESCE(VALUES(sticky), sticky),
                                    locked = COALESCE(VALUES(locked), locked),
                                    comment = COALESCE(VALUES(comment), comment),
                                    exif = COALESCE(VALUES(exif), exif);
                                ",
                                ),
                            )
                            .collect::<String>();

                        match tx
                            .exec_drop(board_query.as_str(), board_values)
                            .map_err(|err| Error::from(err))
                            .await
                        {
                            Ok(r) => r,
                            Err(err) => match err.is_deadlock() {
                                true => {
                                    debug!("Hit MySQL deadlock when processing board posts on board {}. Retrying...", board);
                                    return Err(Error::Deadlock);
                                }
                                false => return Err(err),
                            },
                        };

                        if self.without_triggers {
                            // Update threads
                            if threads.len() > 0 {
                                let rows = threads.len();
                                let thread_table = format!("INSERT INTO `{}_threads` (thread_num, time_op, time_last, time_bump, time_ghost, time_ghost_bump, time_last_modified, nreplies, nimages, sticky, locked)
                                    VALUES (?, COALESCE(?, 0), ?, ?, ?, ?, ?, ?, ?, COALESCE(?, false), COALESCE(?, false))", &board);
                                let thread_query = std::iter::once(thread_table.as_str())
                                    .chain((0..(rows - 1)).map(|_| ", (?, COALESCE(?, 0), ?, ?, ?, ?, ?, ?, ?, COALESCE(?, false), COALESCE(?, false))"))
                                    .chain(
                                        std::iter::once(
                                            " ON DUPLICATE KEY UPDATE
                                            time_op = GREATEST(time_op, VALUES(time_op)),
                                            sticky = COALESCE(VALUES(sticky), sticky),
                                            locked = COALESCE(VALUES(locked), locked),
                                            time_last = GREATEST(time_last, VALUES(time_last)),
                                            time_last_modified = GREATEST(time_last_modified, VALUES(time_last_modified)),
                                            time_bump = GREATEST(time_bump, VALUES(time_bump)),
                                            nreplies = VALUES(nreplies) + nreplies,
                                            nimages = VALUES(nimages) + nimages;
                                        ",
                                        )
                                    )
                                    .collect::<String>();
                                            let thread_values = threads
                                                .into_iter()
                                                .map(|(_, thread)| {
                                                    let values: Box<[mysql_async::Value]> = Box::new([
                                                        thread.thread_num.into(),         // thread_num
                                                        thread.time_op.into(),            // time_op
                                                        thread.time_last.into(),          // time_last,
                                                        thread.time_bump.into(),          // time_bump,
                                                        thread.time_ghost.into(),         // time_ghost,
                                                        thread.time_ghost_bump.into(),    // time_ghost_bump,
                                                        thread.time_last_modified.into(), // time_last_modified
                                                        thread.n_replies.into(),          // nreplies,
                                                        thread.n_images.into(),           // nimages
                                                        thread.sticky.into(),             // sticky,
                                                        thread.locked.into(),             // locked
                                                    ]);
                                                    values.into_vec()
                                                })
                                                .flatten()
                                                .collect::<Vec<_>>();

                                            tx.exec_drop(thread_query.as_str(), thread_values).await?;
                                        }

                                if self.with_stats {
                                    // Update daily stats
                                    if daily.len() > 0 {
                                        let rows = daily.len();
                                        let daily_table = format!(
                                            "INSERT INTO `{}_daily` (day, posts, images, sage, anons, trips, names)
                                        VALUES (?, ?, ?, ?, ?, ?, ?)",
                                            &board
                                        );
                                        let daily_query = std::iter::once(daily_table.as_str())
                                            .chain((0..(rows - 1)).map(|_| ", (?, ?, ?, ?, ?, ?, ?)"))
                                            .chain(std::iter::once(
                                                " ON DUPLICATE KEY UPDATE
                                                    posts = VALUES(posts) + posts,
                                                    images = VALUES(images) + images,
                                                    sage = VALUES(sage) + sage,
                                                    anons = VALUES(anons) + anons,
                                                    trips = VALUES(trips) + trips,
                                                    names = VALUES(names) + names;
                                                ",
                                            ))
                                            .collect::<String>();

                                    let daily_values = daily
                                        .into_iter()
                                        .map(|(_, daily)| {
                                            let values: Box<[mysql_async::Value]> = Box::new([
                                                daily.day.into(),    // day
                                                daily.posts.into(),  // posts
                                                daily.images.into(), // images,
                                                daily.sage.into(),   // sage,
                                                daily.anons.into(),  // anons,
                                                daily.trips.into(),  // trips,
                                                daily.names.into(),  // names
                                            ]);
                                            values.into_vec()
                                        })
                                        .flatten()
                                        .collect::<Vec<_>>();

                                    match tx
                                        .exec_drop(daily_query.as_str(), daily_values)
                                        .map_err(|err| Error::from(err))
                                        .await
                                    {
                                        Ok(r) => r,
                                        Err(err) => match err.is_deadlock() {
                                            true => {
                                                debug!("Hit MySQL deadlock when processing board daily on board {}. Retrying...", board);
                                                return Err(Error::Deadlock);
                                            }
                                            false => return Err(err),
                                        },
                                    };
                                }

                                if users.len() > 0 {
                                    let rows = users.len();
                                    let users_table = format!(
                                        "INSERT INTO `{}_users` (name, trip, firstseen, postcount)
                                        VALUES (?, ?, ?, ?)",
                                                        &board
                                    );
                                    let users_query = std::iter::once(users_table.as_str())
                                        .chain((0..(rows - 1)).map(|_| ", (?, ?, ?, ?)"))
                                        .chain(std::iter::once(
                                            " ON DUPLICATE KEY UPDATE
                                            firstseen = LEAST(VALUES(firstseen), firstseen),
                                            postcount = VALUES(postcount) + postcount;
                                        ",
                                        ))
                                        .collect::<String>();
                                    let users_values = users
                                        .into_iter()
                                        .map(|(_, user)| {
                                            let values: Box<[mysql_async::Value]> = Box::new([
                                                Some(user.name).map(trunc_100).into(), // name
                                                user.trip.into(),                      // trip
                                                user.first_seen.into(),                // firstseen,
                                                user.post_count.into(),                // postcount,
                                            ]);
                                            values.into_vec()
                                        })
                                        .flatten()
                                        .collect::<Vec<_>>();

                                    match tx
                                        .exec_drop(users_query.as_str(), users_values)
                                        .map_err(|err| Error::from(err))
                                        .await
                                    {
                                        Ok(r) => r,
                                        Err(err) => match err.is_deadlock() {
                                            true => {
                                                debug!("Hit MySQL deadlock when processing board users on board {}. Retrying...", board);
                                                return Err(Error::Deadlock);
                                            }
                                            false => return Err(err),
                                        },
                                    };
                                }
                            }
                        }

                        Ok((rows, media_ids))
                    }.await;
                    let result = match result {
                        Ok(x) => tx.commit().await.map_err(|err| Error::from(err)).map(|_| x),
                        Err(err) => {
                            let _ = tx.rollback().await;
                            Err(err)
                        }
                    };
                    drop(interval_lock);
                    match result {
                        Ok((rows, media_ids)) => {
                            debug!(
                                "Committed data for {} posts (board: {}) to MySQL. Took {}ms",
                                rows,
                                board,
                                save_start.elapsed().as_millis(),
                            );
                            images_to_save.extend(media_ids);
                            self.metrics.incr_posts(rows as u64);
                            self.notify_post(rows);
                            processed_posts -= rows;
                            break;
                        }
                        Err(err) => match err {
                            Error::Deadlock => {
                                sleep_jitter(5, 50).await;
                                continue;
                            }
                            err => {
                                if attempts >= self.retries_on_save_error {
                                    return Err(err);
                                } else {
                                    let board = posts[0].board;
                                    let thread_no = posts[0].thread_no();
                                    let post_no = posts[0].no;
                                    error!(
                                        "Failed to save posts, will retry [First]: {}/{}/{}: {}",
                                        board, thread_no, post_no, err
                                    );
                                    attempts += 1;
                                    if let Some(b) = backoff.next_backoff() {
                                        tokio::time::delay_for(b).await;
                                    }
                                    continue;
                                }
                            }
                        },
                    }
                }
            }
            Ok(images_to_save)
        };
        let r = scope.await;
        if processed_posts > 0 {
            self.notify_post(processed_posts);
        }
        r
    }

    async fn download_path<T: AsRef<str>, P: AsRef<str>>(
        &self,
        board: T,
        media_kind: MediaKind,
        filename: P,
        thread_no: u64,
    ) -> Result<PathBuf, Error> {
        let filename = filename.as_ref();
        let thread_no = thread_no.to_string();

        let subdir = match self.old_dir_structure {
            true => {
                let old_subdir = match thread_no.len() {
                    0..=2 => return Err(Error::InvalidThreadOldDir),
                    3..=6 => {
                        PathBuf::from(format!("{:0>4}/{:0>2}", &thread_no[0..1], &thread_no[1..3]))
                    }
                    _ => PathBuf::from(format!(
                        "{:0>4}/{:0>2}",
                        &thread_no[0..thread_no.len() - 5],
                        &thread_no[thread_no.len() - 5..thread_no.len() - 3]
                    )),
                };
                old_subdir
            }
            false => PathBuf::from(format!("{}/{}", &filename[0..4], &filename[4..6])),
        };
        let subdir = Path::new(board.as_ref())
            .join(match media_kind {
                MediaKind::Thumb => "thumb",
                MediaKind::Image => "image",
            })
            .join(subdir);
        //if !subdir.exists() {

        return Ok(subdir.join(filename));
    }

    // TODO: lookup_media does not support looking up data
    // when each media item may belong to a different org.
    //
    // The Sink isn't gauranted this property (i.e. it is
    // not documented anywhere), but this is mostly true.
    async fn lookup_media(
        &self,
        hashes: FxHashSet<MediaDescriptor>,
    ) -> Result<Vec<(thread::Media, MediaDescriptor)>, Error> {
        if hashes.len() == 0 {
            return Ok(vec![]);
        }
        let board = hashes.iter().next().unwrap().board;
        let values = hashes
            .iter()
            .map(|h| mysql_async::Value::from(h.hash.as_ref()))
            .collect::<Vec<_>>();
        let placeholders = std::iter::once("?")
            .chain((0..values.len() - 1).map(|_| ", ?"))
            .collect::<String>();
        let stmt = format!(
            "SELECT media_hash, media, preview_op, preview_reply, banned FROM `{}_images` WHERE media_hash IN ({})",
            board, placeholders
        );
        let m = hashes
            .into_iter()
            .map(|h| (String::from(h.hash.as_ref()), h))
            .collect::<FxHashMap<_, _>>();
        let mut conn = self.get_db_conn().await?;
        Ok(conn
            .exec_map(
                stmt.as_str(),
                values,
                |(media_hash, media, preview_op, preview_reply, banned)| {
                    let media = thread::Media {
                        media_id: 0,
                        media_hash,
                        media,
                        preview_op,
                        preview_reply,
                        total: 0,
                        banned,
                    };
                    let d = m.get(&media.media_hash).unwrap().clone();
                    (media, d)
                },
            )
            .await?)
    }

    async fn download_media(
        &self,
        kind: MediaKind,
        media: &thread::Media,
        meta: &MediaDescriptor,
    ) -> Result<(), Error> {
        let (media_name, filename, url) = match kind {
            MediaKind::Thumb => {
                let filename = match self.old_dir_structure {
                    true => meta.preview.as_ref(),
                    false => match meta.is_op {
                        true => media.preview_op.as_ref(),
                        false => media.preview_reply.as_ref(),
                    },
                };
                if filename.is_none() {
                    warn!("Attempted to download thumbnail, but the thumbnail colums were empty for board/thread: {}/{}", meta.board, meta.thread_no);
                    return Ok(());
                }
                let image_name = filename.unwrap();
                let filename = match self
                    .download_path(meta.board, MediaKind::Thumb, image_name, meta.thread_no)
                    .await
                {
                    Ok(f) => f,
                    Err(err) => return Err(err),
                };

                let mut thumb_url = self.thumb_url.clone();
                thumb_url.set_path(&format!("/{}/{}", meta.board, image_name));
                (image_name, filename, thumb_url)
            }
            MediaKind::Image => {
                if media.media.is_none() {
                    warn!("Attempted to download media, but the media column was empty for board/thread: {}/{}", meta.board, meta.thread_no);
                    return Ok(());
                }
                let media_orig = media.media.as_ref().unwrap();
                let filename = match self
                    .download_path(meta.board, MediaKind::Image, media_orig, meta.thread_no)
                    .await
                {
                    Ok(f) => f,
                    Err(err) => return Err(err),
                };
                let mut media_url = self.media_url.clone();
                media_url.set_path(&format!("/{}/{}", meta.board, &media_orig,));

                (media_orig, filename, media_url)
            }
        };

        let fs_download = match self.fs_storage.as_ref() {
            Some(fs) => !fs.exists(&filename).await?,
            None => false,
        };

        let s3_download = match self.s3_storage.as_ref() {
            Some(fs) => !fs.exists(&filename).await?,
            None => false,
        };

        let b2_download = match self.b2_storage.as_ref() {
            Some(fs) => !fs.exists(&filename).await?,
            None => false,
        };

        // if !filename.exists() {
        if fs_download || s3_download || b2_download {
            match kind {
                MediaKind::Thumb => debug!("Downloading media {:?}", url),
                MediaKind::Image => debug!("Downloading thumb {:?}", url),
            };
            let dl_start = Instant::now();
            let dl_req = self
                .client
                .get(url)
                .send()
                .and_then(|resp| async { resp.error_for_status() })
                .await?;

            let sz = match dl_req.content_length() {
                Some(a) => a as usize,
                None => {
                    if s3_download || b2_download {
                        return Err(Error::ContentLength(
                            dl_req.url().to_string(),
                            dl_req.status(),
                        ));
                    } else {
                        0
                    }
                }
            };

            let file_sink = match &self.fs_storage {
                Some(fs) if fs_download => match fs.open(&filename, sz).await {
                    Ok(f) => Some(f),
                    Err(err) => Err(err)?,
                },
                _ => None,
            };

            let s3_sink = match &self.s3_storage {
                Some(fs) if s3_download => match fs.open(&filename, sz).await {
                    Ok(f) => Some(f),
                    Err(err) => {
                        error!("Open failed: {}", err);
                        Err(err)?
                    }
                },
                _ => None,
            };

            let b2_sink = match &self.b2_storage {
                Some(fs) if b2_download => match fs.open(&filename, sz).await {
                    Ok(f) => Some(f),
                    Err(err) => {
                        error!("Open failed: {}", err);
                        Err(err)?
                    }
                },
                _ => None,
            };

            let (sinks, downloaded) = dl_req
                .bytes_stream()
                .map_err(move |err| match err.status() {
                    Some(reqwest::StatusCode::SERVICE_UNAVAILABLE) => Error::CloudFlareBlocked(err),
                    Some(reqwest::StatusCode::TOO_MANY_REQUESTS) => Error::CloudFlareBlocked(err),
                    Some(reqwest::StatusCode::FORBIDDEN) => Error::CloudFlareBlocked(err),
                    _ => Error::from(err),
                })
                .try_fold(
                    ((file_sink, s3_sink, b2_sink), 0),
                    |(mut f, bytes), buf| async move {
                        if f.0.is_some() {
                            if let Err(e) = f.0.as_mut().unwrap().write_all(&buf).await {
                                return Err(Error::from(e));
                            }
                        }
                        if f.1.is_some() {
                            if let Err(e) = f.1.as_mut().unwrap().write_all(&buf).await {
                                return Err(Error::from(e));
                            }
                        }
                        if f.2.is_some() {
                            if let Err(e) = f.2.as_mut().unwrap().write_all(&buf).await {
                                return Err(Error::from(e));
                            }
                        }
                        return Ok((f, bytes + buf.len()));
                    },
                )
                .await?;

            if sinks.0.is_some() {
                sinks.0.unwrap().shutdown().await?;
            }
            if sinks.1.is_some() {
                sinks.1.unwrap().shutdown().await?;
            }
            if sinks.2.is_some() {
                sinks.2.unwrap().shutdown().await?;
            }

            info!(
                "Downloaded {} {}/{}. {} bytes. Took {}ms",
                meta.board,
                match kind {
                    MediaKind::Thumb => "thumbnail",
                    MediaKind::Image => "media",
                },
                media_name,
                downloaded,
                dl_start.elapsed().as_millis()
            );

            self.metrics.incr_bytes(downloaded as u64);

            match kind {
                MediaKind::Thumb => self.metrics.incr_thumbs(1),
                MediaKind::Image => self.metrics.incr_media(1),
            };
        }

        Ok(())
    }

    async fn save_media(&self, media: &thread::Media, meta: &MediaDescriptor) -> Result<(), Error> {
        if media.banned {
            return Ok(());
        }
        let (dl_thumb, dl_media) = self
            .boards
            .get(meta.board)
            .map(|x| {
                (
                    x.thumbs && self.download_thumbs,
                    x.media && self.download_media,
                )
            })
            .unwrap();
        if !(dl_thumb || dl_media) {
            return Ok(());
        }

        let thumb_future = async {
            self.concurrent_downloads.fetch_add(1, Ordering::AcqRel);
            self.download_media(MediaKind::Thumb, media, meta).await
        };
        let thumb_future = thumb_future.inspect(|_| self.notify_download(1));

        let media_future = async {
            self.concurrent_downloads.fetch_add(1, Ordering::AcqRel);
            self.download_media(MediaKind::Image, media, meta).await
        };
        let media_future = media_future.inspect(|_| self.notify_download(1));

        match (dl_media, dl_thumb) {
            (true, true) => {
                futures::future::join(thumb_future, media_future)
                    .map(|(a, b)| a.and(b))
                    .await
            }
            (true, _) => media_future.await,
            (_, true) => thumb_future.await,
            _ => unreachable!(),
        }
    }

    async fn retry_save_media(
        self: Arc<Self>,
        media: thread::Media,
        meta: MediaDescriptor,
    ) -> Result<(), Error> {
        let mut backoff = backoff::ExponentialBackoff::default();
        backoff.max_elapsed_time = Some(Duration::from_secs(3600));
        loop {
            self.inflight_media.fetch_add(1, Ordering::Relaxed);
            let r = self.download_tokens.acquire().then(|permit| async {
                let r = self.save_media(&media, &meta).await;
                drop(permit);
                self.inflight_media.fetch_sub(1, Ordering::Relaxed);
                r
            });
            match r.await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    let err = err.try_downcast();
                    if err.is_request_failure() || err.is_b2() || err.is_s3() {
                        if err.is_cloudflare() {
                            self.metrics.incr_cfblocked(1);
                        }
                        if let Some(b) = backoff.next_backoff() {
                            error!("Downloading media failed, will retry later: {}", err);
                            tokio::time::delay_for(b).await;
                            continue;
                        }
                    }
                    error!("Downloading media failed: {}", err);
                    return Err(err);
                }
            }
        }
    }

    async fn send_posts(self: Arc<Self>, item: Vec<imageboard::Post>) {
        let board = item[0].board;
        let thread_no = item[0].thread_no();
        let post_no = item[0].no;
        let sz = item.len();
        match self.save_posts(item).await {
            Ok(images) => match self.lookup_media(images).await {
                Ok(images) => {
                    stream::iter(images)
                        .map(|(media, meta)| {
                            self.inflight_media.fetch_add(1, Ordering::Relaxed);
                            let inner2 = &self;
                            self.download_tokens
                                .acquire()
                                .then(move |permit| async move {
                                    let r = inner2.save_media(&media, &meta).await;
                                    drop(permit);
                                    inner2.inflight_media.fetch_sub(1, Ordering::Relaxed);
                                    match r {
                                        Ok(_) => Ok(()),
                                        Err(err) => Err((err, media, meta)),
                                    }
                                })
                        })
                        .buffer_unordered(self.max_concurrent_downloads)
                        .for_each(|download| match download {
                            Ok(_) => futures::future::ready(()),
                            Err((err, media, meta)) => {
                                let err = err.try_downcast();
                                if err.is_request_failure() || err.is_b2() || err.is_s3() {
                                    if err.is_cloudflare() {
                                        self.metrics.incr_cfblocked(1);
                                    }
                                    self.process_tx.send(AsagiTask::Media(media, meta)).unwrap();
                                    futures::future::ready(debug!(
                                        "Downloading media failed, will retry later: {}",
                                        err
                                    ))
                                } else {
                                    futures::future::ready(error!(
                                        "Downloading media failed: {}",
                                        err
                                    ))
                                }
                            }
                        })
                        .await
                }
                Err(err) => {
                    error!("Failed to lookup media hashes: {}", err);
                }
            },
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
            if self.concurrent_downloads.load(Ordering::Acquire) == 0 {
                self.flush_waker.wake();
                self.close_waker.wake();
            }
        }
    }

    fn notify_download(&self, no_downloads: usize) {
        let old = self
            .concurrent_downloads
            .fetch_sub(no_downloads, Ordering::AcqRel);
        let curr = old - no_downloads;
        if curr < self.max_concurrent_downloads {
            self.waker.wake();
        }
        if curr == 0 {
            if self.inflight_posts.load(Ordering::Acquire) == 0 {
                self.flush_waker.wake();
                self.close_waker.wake();
            }
        }
    }

    fn is_ready(&self) -> bool {
        let downloads = self.concurrent_downloads.load(Ordering::Acquire);
        let posts = self.inflight_posts.load(Ordering::Acquire);
        (!self.media_backpressure || downloads < self.max_concurrent_downloads)
            && posts < self.max_inflight_posts
    }

    fn is_empty(&self) -> bool {
        let downloads = self.concurrent_downloads.load(Ordering::Acquire);
        let posts = self.inflight_posts.load(Ordering::Acquire);
        downloads == 0 && posts == 0
    }

    fn has_failed(&self) -> bool {
        return self.fail_on_save_error && self.failed.load(Ordering::Relaxed);
    }

    fn truncate_meta_field(&self, v: String) -> String {
        if self.truncate_fields && v.len() > 100 {
            String::from(&v[..100])
        } else {
            v
        }
    }
}

impl Sink<Vec<imageboard::Post>> for Asagi {
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
            self.inner.process_tx.send(AsagiTask::Posts(item)).unwrap();
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
        let _ = self.inner.process_tx.send(AsagiTask::Closed);
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

pub async fn sleep_jitter(min: u64, max: u64) {
    let secs = Duration::from_millis(rand::thread_rng().gen_range(min, max));
    tokio::time::delay_for(secs).await;
}
