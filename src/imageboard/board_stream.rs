use std::collections::hash_map::Entry;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    Arc,
};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::Either;
use futures::prelude::*;
use log::{debug, error, info};
use rand::distributions::Alphanumeric;
use rand::Rng;
use reqwest::{self, Url};
use rustc_hash::FxHashMap;
use thiserror::Error;
use tokio::time::{delay_until, Delay};

use super::{CatalogPage, CatalogThread, Post, Thread};

#[derive(Debug, Error)]
pub enum Error {
    #[error("resource has not been modified")]
    NotModified(Option<String>),
    #[error("request error: {}", .0)]
    Request(#[from] reqwest::Error),
    #[error("The request was blocked by Cloudflare's bot detection")]
    CloudFlareBlocked(reqwest::Error),
}

impl Error {
    fn is_status(&self) -> bool {
        match self {
            Error::Request(req) => req.is_status(),
            _ => false,
        }
    }

    fn status(&self) -> Option<reqwest::StatusCode> {
        match self {
            Error::Request(req) => req.status(),
            _ => None,
        }
    }

    fn is_cloudflare(&self) -> bool {
        match self {
            Error::CloudFlareBlocked(_) => true,
            _ => false,
        }
    }
}

enum State {
    Sleeping(Option<String>),
    Query(Pin<Box<dyn Future<Output = Result<(Vec<CatalogPage>, Option<String>), Error>> + Send>>),
    Archive(
        Pin<Box<dyn Stream<Item = Result<Vec<Post>, ((u64, usize), Error)>> + Send>>,
        Option<String>,
    ),
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field(
                "action",
                match self {
                    State::Sleeping(_) => &"sleeping",
                    State::Query(_) => &"query",
                    State::Archive(_, _) => &"archive",
                },
            )
            .finish()
    }
}

struct WatchedThread {
    page: usize,
    no: u64,
    replies: u16,
    last_modified: u64,
    sticky: bool,
    closed: bool,
    posts: FxHashMap<u64, WatchedPost>,
}

impl WatchedThread {
    fn modified(&self, other: &CatalogThread) -> bool {
        self.last_modified != other.last_modified
            || self.replies != other.replies
            || self.sticky != other.sticky
            || self.closed != other.closed
    }

    fn update(&mut self, other: &CatalogThread) {
        self.page = other.page;
        self.no = other.no;
        self.replies = other.replies;
        self.last_modified = other.last_modified;
        self.sticky = other.sticky;
        self.closed = other.closed;
    }
}

impl From<&CatalogThread> for WatchedThread {
    fn from(thread: &CatalogThread) -> Self {
        Self {
            page: thread.page,
            no: thread.no,
            replies: thread.replies,
            last_modified: thread.last_modified,
            sticky: thread.sticky,
            closed: thread.closed,
            posts: FxHashMap::default(),
        }
    }
}

struct WatchedPost {
    #[allow(dead_code)]
    no: u64,
    #[allow(dead_code)]
    resto: u64,
    sticky: bool,
    closed: bool,
    comment_hash: u64,
}

impl WatchedPost {
    fn modified(&self, other: &Post) -> bool {
        self.sticky != other.sticky
            || self.closed != other.closed
            || self.comment_hash != other.comment_hash()
    }
}

impl From<&Post> for WatchedPost {
    fn from(post: &Post) -> Self {
        Self {
            no: post.no,
            resto: post.resto,
            sticky: post.sticky,
            closed: post.closed,
            comment_hash: post.comment_hash(),
        }
    }
}

#[derive(Debug, Default)]
pub struct Metrics {
    pub board: String,
    pub warmed_up: AtomicBool,
    pub posts: AtomicU64,
    pub deleted: AtomicU64,
    pub last_modified: AtomicI64,
    pub cloudflare_blocked: AtomicU64,
}

impl Metrics {
    pub fn incr_posts(&self, count: u64) {
        self.posts.fetch_add(count, Ordering::Relaxed);
    }

    pub fn incr_deleted(&self, count: u64) {
        self.deleted.fetch_add(count, Ordering::Relaxed);
    }

    pub fn incr_cf_blocked(&self, count: u64) {
        self.cloudflare_blocked.fetch_add(count, Ordering::Relaxed);
    }

    pub fn set_last_modified(&self, unix_timestamp: i64) {
        self.last_modified.store(unix_timestamp, Ordering::Relaxed);
    }
}

pub struct BoardStream {
    client: reqwest::Client,
    host: String,
    board: String,
    state: State,
    delay: Delay,
    refresh_rate: Duration,
    base_url: &'static str,
    board_url: reqwest::Url,
    watched: FxHashMap<u64, WatchedThread>,
    rate_limiter: Option<
        Arc<
            governor::RateLimiter<
                governor::state::direct::NotKeyed,
                governor::state::InMemoryState,
                governor::clock::DefaultClock,
            >,
        >,
    >,
    deleted_page_threshold: usize,
    metrics: Arc<Metrics>,
}

impl BoardStream {
    pub fn new<T: AsRef<str>, U: AsRef<str>>(
        client: reqwest::Client,
        tls: bool,
        host: T,
        board: U,
        refresh_rate: Duration,
        rate_limiter: Option<
            Arc<
                governor::RateLimiter<
                    governor::state::direct::NotKeyed,
                    governor::state::InMemoryState,
                    governor::clock::DefaultClock,
                >,
            >,
        >,
        deleted_page_threshold: usize,
    ) -> Self {
        let base_url = match tls {
            true => "https://localhost/",
            false => "http://localhost/",
        };
        let mut board_url = Url::parse(base_url).unwrap();
        board_url.set_host(Some(host.as_ref())).unwrap();
        board_url.set_path(&format!("/{}/catalog.json", board.as_ref()));
        Self {
            client,
            host: String::from(host.as_ref()),
            board: String::from(board.as_ref()),
            refresh_rate,
            delay: delay_until(tokio::time::Instant::now()),
            state: State::Sleeping(None),
            board_url,
            watched: FxHashMap::default(),
            rate_limiter,
            base_url,
            deleted_page_threshold,
            metrics: Arc::new(Metrics {
                board: String::from(board.as_ref()),
                ..Default::default()
            }),
        }
    }

    fn get_api_token(&self) -> impl Future<Output = ()> {
        match &self.rate_limiter {
            Some(r) => {
                let r = r.clone();
                Either::Left(async move {
                    r.until_ready().await;
                    drop(r);
                    ()
                })
            }
            None => Either::Right(futures::future::ready(())),
        }
    }

    fn last_modified(&self) -> Option<&String> {
        match &self.state {
            State::Sleeping(lm) => lm.as_ref(),
            _ => unreachable!(),
        }
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }
}

impl Stream for BoardStream {
    type Item = Vec<Post>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Vec<Post>>> {
        loop {
            match &mut self.state {
                State::Sleeping(_) => match Pin::new(&mut self.delay).poll(cx) {
                    Poll::Ready(_) => {
                        let t = self.delay.deadline();
                        let mut board_url = self.board_url.clone();
                        board_url.set_query(Some(&format!("_={}", t.elapsed().as_nanos())));
                        let req = self.client.get(board_url.as_str());
                        let req = match self.last_modified() {
                            Some(modified) => req.header(
                                reqwest::header::IF_MODIFIED_SINCE,
                                reqwest::header::HeaderValue::from_str(modified).unwrap(),
                            ),
                            None => req,
                        }
                        .send();
                        let req = self
                            .get_api_token()
                            .then(move |_| {
                                info!("Loading catalog URL: {}", board_url);
                                req.map_err(move |err| match err.status() {
                                    Some(reqwest::StatusCode::SERVICE_UNAVAILABLE) => {
                                        Error::CloudFlareBlocked(err)
                                    }
                                    Some(reqwest::StatusCode::TOO_MANY_REQUESTS) => {
                                        Error::CloudFlareBlocked(err)
                                    }
                                    Some(reqwest::StatusCode::FORBIDDEN) => {
                                        Error::CloudFlareBlocked(err)
                                    }
                                    _ => Error::from(err),
                                })
                            })
                            .and_then(|resp| async { resp.error_for_status().map_err(Error::from) })
                            .and_then(|resp| {
                                let last_modified = resp
                                    .headers()
                                    .get(reqwest::header::LAST_MODIFIED)
                                    .map(|x| String::from(x.to_str().unwrap()));
                                match resp.status() {
                                    reqwest::StatusCode::NOT_MODIFIED => {
                                        Either::Left(futures::future::ready(Err(
                                            Error::NotModified(last_modified),
                                        )))
                                    }
                                    _ => Either::Right(
                                        resp.json::<Vec<CatalogPage>>()
                                            .map_ok(|t| (t, last_modified))
                                            .map_err(|e| Error::from(e)),
                                    ),
                                }
                            });
                        self.state = State::Query(req.boxed());
                        continue;
                    }
                    Poll::Pending => return Poll::Pending,
                },
                State::Query(req) => {
                    match req.as_mut().poll(cx) {
                        Poll::Ready(response) => {
                            match response {
                                Ok((mut threads, last_modified)) => {
                                    debug!(
                                        "Loaded catalog for board {} on host {}.",
                                        self.board, self.host
                                    );
                                    for page in threads.iter_mut() {
                                        let page_no = page.page;
                                        for thread in page.threads.iter_mut() {
                                            thread.page = page_no;
                                        }
                                    }
                                    let threads_no = threads
                                        .iter()
                                        .map(|x| x.threads.iter().map(move |y| (y.no, x.page)))
                                        .flatten()
                                        .collect::<Vec<_>>();
                                    // list all threads that have been modified
                                    // or are new to us
                                    let c = threads
                                        .into_iter()
                                        .map(|x| x.threads)
                                        .flatten()
                                        .filter(|thread| match self.watched.entry(thread.no) {
                                            Entry::Occupied(mut v) => {
                                                let x = v.get_mut();
                                                let r = x.modified(&thread);
                                                x.update(&thread);
                                                r
                                            }
                                            Entry::Vacant(v) => {
                                                v.insert(thread.into());
                                                true
                                            }
                                        })
                                        .map(|thread| (thread.no, thread.page))
                                        .collect::<Vec<_>>();
                                    let f = self
                                        .watched
                                        .values()
                                        .filter_map(move |watched_thread| {
                                            // retrieve all threads that we *know* exist
                                            // but are not being returned
                                            match threads_no
                                                .iter()
                                                .find(|&&t| t.0 == watched_thread.no)
                                            {
                                                Some(_) => None,
                                                None => {
                                                    Some((watched_thread.no, watched_thread.page))
                                                }
                                            }
                                        })
                                        .chain(c)
                                        .map(|(thread_no, page_no)| {
                                            let mut thread_url = Url::parse(self.base_url).unwrap();
                                            thread_url.set_host(Some(self.host.as_ref())).unwrap();
                                            thread_url.set_path(&format!(
                                                "/{}/thread/{}.json",
                                                self.board, thread_no
                                            ));
                                            thread_url.set_query(Some(&format!(
                                                "_={}",
                                                rand::thread_rng()
                                                    .sample_iter(&Alphanumeric)
                                                    .take(10)
                                                    .collect::<String>()
                                            )));
                                            let req = self.client.get(thread_url.as_str()).send();
                                            let board = self.board.clone();
                                            self.get_api_token()
                                                .then(move |_| {
                                                    debug!("Loading thread URL: {}", thread_url);
                                                    req
                                                })
                                                .and_then(|resp| async { resp.error_for_status() })
                                                .and_then(|resp| resp.json::<Thread>())
                                                .map_ok(move |mut t| {
                                                    t.posts.iter_mut().for_each(|x| {
                                                        x.board.clear();
                                                        x.board.push_str(board.as_str());
                                                    });
                                                    t.posts
                                                })
                                                .map_err(move |err| match err.status() {
                                                    Some(
                                                        reqwest::StatusCode::SERVICE_UNAVAILABLE,
                                                    ) => Error::CloudFlareBlocked(err),
                                                    Some(
                                                        reqwest::StatusCode::TOO_MANY_REQUESTS,
                                                    ) => Error::CloudFlareBlocked(err),
                                                    Some(reqwest::StatusCode::FORBIDDEN) => {
                                                        Error::CloudFlareBlocked(err)
                                                    }
                                                    _ => Error::from(err),
                                                })
                                                .map_err(move |err| ((thread_no, page_no), err))
                                        });
                                    let f =
                                        f.collect::<futures::stream::FuturesUnordered<_>>().boxed();
                                    self.state = State::Archive(f, last_modified);
                                    continue;
                                }
                                Err(Error::NotModified(last_modified)) => {
                                    info!("Board {} on host {} has not been modified since last request.", self.board, self.host);
                                    let last_run = tokio::time::Instant::now();
                                    let next = last_run + self.refresh_rate;
                                    self.delay.reset(next);
                                    self.state = State::Sleeping(last_modified);
                                    continue;
                                }
                                Err(err) => {
                                    if err.is_cloudflare() {
                                        self.metrics.incr_cf_blocked(1);
                                    }
                                    error!(
                                        "Failed to load catalog for board {} on host {}: {}",
                                        self.board, self.host, err
                                    );
                                    let last_run = tokio::time::Instant::now();
                                    let next = last_run + self.refresh_rate;
                                    self.delay.reset(next);
                                    self.state = State::Sleeping(None);
                                    continue;
                                }
                            }
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                State::Archive(threads, last_modified) => match threads.as_mut().poll_next(cx) {
                    Poll::Ready(Some(posts)) => match posts {
                        Ok(mut posts) => match self.watched.get_mut(&posts[0].no) {
                            Some(p) => {
                                let thread_no = p.no;
                                let board = &posts[0].board;
                                let is_archived = posts[0].archived;
                                // check for any deleted posts
                                let deleted = p
                                    .posts
                                    .iter()
                                    .filter_map(|wr| match posts.iter().find(|r| r.no == *wr.0) {
                                        Some(_) => None,
                                        None => Some(Post::deleted(board, *wr.0)),
                                    })
                                    .collect::<Vec<_>>();

                                // keep the OP post, and any posts that have changed
                                posts.retain(|r| {
                                    let keep = p
                                        .posts
                                        .get_mut(&r.no)
                                        .map(|z| z.modified(&r))
                                        .unwrap_or(true);
                                    keep
                                });

                                // mark watched posts as retransmission,
                                // make sure all posts are now watched
                                for r in posts.iter_mut() {
                                    if p.posts.contains_key(&r.no) {
                                        r.is_retransmission = true
                                    }
                                    p.posts.insert(r.no, (&*r).into());
                                }
                                posts.extend_from_slice(&deleted);

                                if is_archived {
                                    self.watched.remove(&thread_no);
                                } else {
                                    deleted.iter().for_each(|x| {
                                        p.posts.remove(&x.no);
                                    });
                                }
                                if posts.len() == 0 {
                                    continue;
                                }
                                debug!(
                                    "Flushing {} new posts for thread {}/{}",
                                    posts.len(),
                                    posts[0].board,
                                    thread_no
                                );
                                self.metrics
                                    .incr_posts((posts.len() - deleted.len()) as u64);
                                self.metrics.incr_deleted(deleted.len() as u64);
                                return Poll::Ready(Some(posts));
                            }
                            None => unreachable!(),
                        },
                        Err(((thread_no, page_no), err)) => {
                            if err.is_status() {
                                if err.status().unwrap() == reqwest::StatusCode::NOT_FOUND {
                                    debug!(
                                        "Thread {}/{}/{} returned 404",
                                        self.host, self.board, thread_no
                                    );
                                    if page_no < self.deleted_page_threshold {
                                        debug!(
                                            "Thread {}/{}/{} being marked as deleted",
                                            self.host, self.board, thread_no
                                        );
                                        match self.watched.get(&thread_no) {
                                            Some(p) => {
                                                let deleted = p
                                                    .posts
                                                    .iter()
                                                    .map(|p| Post::deleted(&self.board, *p.0))
                                                    .collect::<Vec<_>>();
                                                self.watched.remove(&thread_no);
                                                return Poll::Ready(Some(deleted));
                                            }
                                            None => continue,
                                        }
                                    }
                                    continue;
                                }
                            } else if err.is_cloudflare() {
                                self.metrics.incr_cf_blocked(1);
                            }
                            error!(
                                "Failed to load thread {} on board {} on host {}: {}",
                                thread_no, self.board, self.host, err
                            );
                            continue;
                        }
                    },
                    Poll::Ready(None) => {
                        let last_modified = last_modified.take();
                        if let Some(last_modified) = last_modified.as_ref() {
                            if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(last_modified) {
                                self.metrics.set_last_modified(dt.timestamp());
                            }
                        }
                        info!(
                            "Finished scraping board {}. Sleeping for {}ms.",
                            self.board,
                            self.refresh_rate.as_millis()
                        );
                        let last_run = tokio::time::Instant::now();
                        let next = last_run + self.refresh_rate;
                        self.delay.reset(next);
                        self.state = State::Sleeping(last_modified);
                        self.metrics.warmed_up.store(true, Ordering::Relaxed);
                        continue;
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }
}
