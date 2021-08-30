use std::iter::IntoIterator;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use clap::ArgMatches;
use futures::prelude::*;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{error, info};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("pg database error: {}", .0)]
    DB(reqwest::Error),
    #[error("io error: {}", .0)]
    IO(#[from] std::io::Error),
    #[error("mysql error: {}", .0)]
    MySQL(#[from] sqlx::Error),
}

#[derive(sqlx::FromRow, Debug)]
struct BoardInfo {
    board: String,
    records: i64,
}

#[derive(sqlx::FromRow, Debug)]
struct Post {
    board: String,
    thread_num: u32,
    num: u32,
    title: Option<String>,
    name: Option<String>,
    trip: Option<String>,
    email: Option<String>,
    poster_hash: Option<String>,
    poster_country: Option<String>,
    media_filename: Option<String>,
    media_hash: Option<String>,
    media_w: Option<u16>,
    media_h: Option<u16>,
    timestamp: i64,
    comment: Option<String>,
    deleted: bool,
    sticky: bool,
    spoiler: bool,
    op: bool,
    capcode: Option<String>,
}

impl<'a> Into<super::post::Post<'a>> for &'a Post {
    fn into(self) -> super::post::Post<'a> {
        let upper = {
            let bytes = self.board.as_bytes();
            let bytes = [bytes.get(0).copied().unwrap_or(0), bytes.get(1).copied().unwrap_or(0), bytes.get(2).copied().unwrap_or(0), bytes.get(3).copied().unwrap_or(0)];
            super::post::as_u32_be(&bytes)
        };
        let lower = self.num as u32;
        let tuid = (upper as u64) << 32 | (lower as u64);
        let version = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(n) => n.as_millis() as u64,
            Err(_) => panic!("SystemTime before UNIX EPOCH!"),
        };
        super::post::Post {
            board: self.board.as_str(),
            thread_no: self.thread_num as _,
            post_no: self.num as _,
            subject: self.title.as_ref().map(|x| &**x),
            username: self.name.as_ref().map(|x| &**x),
            tripcode: self.trip.as_ref().map(|x| &**x),
            email: self.email.as_ref().map(|x| &**x),
            unique_id: self.poster_hash.as_ref().map(|x| &**x),
            since4_pass: None,
            country: self.poster_country.as_ref().map(|x| &**x),
            filename: self.media_filename.as_ref().map(|x| &**x),
            image_hash: self.media_hash.as_ref().map(|x| &**x),
            image_width: self.media_w.unwrap_or(0) as _,
            image_height: self.media_h.unwrap_or(0) as  _,
            ts: self.timestamp as _,
            comment: self.comment.as_ref().map(|x| &**x),
            deleted: if self.deleted { 1 } else { 0 },
            ghost: 0,
            sticky: if self.sticky { 1 } else { 0 },
            spoiler: if self.spoiler { 1 } else { 0 },
            op: if self.op { 1 } else { 0 },
            capcode: self.capcode.as_ref()
            .map(|c| {
                c.chars()
                    .filter(char::is_ascii)
                    .next()
                    .map(|c| c as u64)
            })
            .flatten(),
            tuid,
            version,
        }
    }
}

struct LnxReIndex {
    client: reqwest::Client,
    upload_url: url::Url,
    commit_url: url::Url,
    mysql: sqlx::MySqlPool,
    tables: Vec<String>,
    write_streams: usize,
}

fn lookup_query<T: AsRef<str>>(board: T) -> String {
    format!(
        r"SELECT
    '{}' AS board,
    `thread_num`,
    `num`,
    `title`,
    `name`,
    `trip`,
    `email`,
    `poster_hash`,
    `poster_country`,
    `media_filename`,
    `media_hash`,
    `media_w`,
    `media_h`,
    COALESCE(UNIX_TIMESTAMP(`unix_timestamp`), `timestamp`) AS `timestamp`,
    `comment`,
    `deleted`,
    `sticky`,
    `spoiler`,
    `op`,
    `capcode`
FROM
    `{}`",
        board.as_ref(),
        board.as_ref()
    )
}

impl LnxReIndex {
    async fn new<U: AsRef<str>, V: AsRef<str>, T: IntoIterator<Item = U>>(
        upload_url: url::Url,
        commit_url: url::Url,
        authentication_key: V,
        request_timeout: Option<std::time::Duration>,
        source_url: url::Url,
        tables: T,
        write_streams: usize,
    ) -> Result<Self, Error> {

        let mut headers = reqwest::header::HeaderMap::new();
        if !authentication_key.as_ref().is_empty() {
            headers.insert(
                "Authorization",
                reqwest::header::HeaderValue::from_str(&format!(
                    "Bearer {}",
                    authentication_key.as_ref()
                ))
                .unwrap(),
            );
        }
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(request_timeout.unwrap_or(std::time::Duration::from_secs(u64::MAX)))
            .build()
            .unwrap();

        info!(
            "Connecting to MySQL at {}...",
            source_url.host_str().unwrap()
        );
        let mysql_pool = sqlx::MySqlPool::connect(&source_url.to_string()).await?;

        Ok(Self {
            client,
            upload_url,
            commit_url,
            mysql: mysql_pool,
            tables: tables
                .into_iter()
                .map(|x| String::from(x.as_ref()))
                .collect(),
            write_streams,
        })
    }

    pub async fn build(self, commit_interval: Option<std::time::Duration>) -> Result<(), Error> {
        let rwlock = Arc::new(tokio::sync::RwLock::new(()));
        let boards = self.tables;
        let mysql_pool = self.mysql;
        let write_streams = self.write_streams;

        if let Some(commit_interval) = commit_interval {
            let rwlock = rwlock.clone();
            let client = self.client.clone();
            let commit_url = self.commit_url.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::delay_for(commit_interval).await;
                    let t = rwlock.write().await;
                    let r = client
                        .post(commit_url.clone())
                        .send()
                        .and_then(|resp| futures::future::ready(resp.error_for_status()))
                        .await;
                    drop(t);
                    if let Err(err) = r {
                        log::warn!("Failed to commit lnx search index: {}", err);
                    }
                }
            });
        }
        let m = Arc::new(MultiProgress::new());
        let sty = ProgressStyle::default_bar()
        .template("{spinner:.green} {msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>7}/{len:7} ({per_sec}, {eta})")
        .progress_chars("#>-");
        let joined = AtomicBool::new(false);
        info!("Querying table information...");
        let boards = stream::iter(boards)
            .then(|board| {
                mysql_pool.acquire().map(move |conn| match conn {
                    Ok(c) => Ok((board, c)),
                    Err(err) => Err(Error::from(err)),
                })
            })
            .and_then(|(board, mut conn)| async move {
                info!("\t{}...", board);
                let info = sqlx::query_as::<_, BoardInfo>(&format!(
                    "SELECT '{}' AS board, COUNT(1) AS records FROM `{}`",
                    board, board
                ))
                .fetch_one(&mut conn)
                .await?;
                Ok(info)
            })
            .map_ok(|info| {
                let pb = m.add(ProgressBar::new(info.records as u64));
                pb.set_style(sty.clone());
                pb.set_message(info.board.as_str());
                pb.set_position(0);
                (info, pb)
            })
            .try_collect::<Vec<_>>()
            .await?;

        info!("Starting transfer...");
        let client = self.client.clone();
        let upload_url = self.upload_url.clone();
        let rows = stream::iter(boards)
            .then(|(info, pb)| {
                mysql_pool.acquire().map(move |conn| match conn {
                    Ok(c) => Ok((info, c, pb)),
                    Err(err) => Err(Error::from(err)),
                })
            })
            .and_then(|(info, mut conn, pb)| {
                if !joined.fetch_or(true, Ordering::AcqRel) {
                    let m = m.clone();
                    tokio::spawn(tokio::task::spawn_blocking(move || {
                        // do some compute-heavy work or call synchronous code
                        m.join()
                    }));
                }
                let lookup = lookup_query(&info.board);
                let client = client.clone();
                let upload_url = upload_url.clone();
                let rwlock = rwlock.clone();
                async move {
                    sqlx::query_as::<_, Post>(&lookup)
                        .fetch(&mut conn)
                        .chunks(1280)
                        .map(|posts| {
                            posts
                                .into_iter()
                                .collect::<Result<Vec<Post>, _>>()
                                .map_err(|err| Error::from(err))
                        })
                        .map_ok(|posts| {
                            rwlock.read().map(move |permit| (permit, posts)).then(|(permit, posts)| {
                                let rows = posts.len();
                                let delete_posts = {
                                    let posts = posts.iter().map(|p| p.into()).collect::<Vec<super::post::Post>>();
                                    let field = posts.iter().map(|x| x.tuid).collect::<Vec<_>>();
                                    super::post::DeletePost::new(field)
                                };
                                client
                                    .delete(upload_url.clone())
                                    .json(&delete_posts)
                                    .send()
                                    .and_then(|resp| futures::future::ready(resp.error_for_status()))
                                    .map_ok(move |_| posts)
                                    .and_then(|posts| {
                                        let posts = posts.iter().map(|p| p.into()).collect::<Vec<super::post::Post>>();
                                        client
                                            .post(upload_url.clone())
                                            .json(&posts)
                                            .send()
                                            .and_then(|resp| futures::future::ready(resp.error_for_status()))
                                    })
                                    .map_ok(move |_| rows)
                                    .map_err(|err| Error::DB(err))
                                    .inspect(move |_| drop(permit))
                            })
                        })
                        .try_buffer_unordered(write_streams)
                        .try_fold(0, |acc, rows| {
                            pb.inc(rows as u64);
                            futures::future::ready(Ok(acc + rows))
                        })
                        .inspect(|r| {
                            if r.is_ok() {
                                pb.finish_with_message(&info.board);
                            } else {
                                pb.finish_at_current_pos();
                            }
                        })
                        .await
                }
            })
            .try_fold(0, |acc, rows| futures::future::ready(Ok(acc + rows)))
            .await?;

        info!("Finished. Modified {} rows.", rows);
        Ok(())
    }
}

pub fn reindex<'a>(matches: &ArgMatches<'a>) -> i32 {
    info!("Running lnx-search re-indexer");

    let lnx_url: url::Url = match matches.value_of("lnx").unwrap().parse() {
        Ok(c) => c,
        Err(err) => {
            error!("Failed to parse postgres uri: {}", err);
            return 1;
        }
    };
    let lnx_index = match matches.value_of("index") {
        Some(c) => String::from(c),
        None => {
            error!("Invalid index.");
            return 1;
        }
    };
    let lnx_key = match matches.value_of("authentication-key") {
        Some(c) => String::from(c),
        None => String::from("")
    };
    let mysql_url: url::Url = match matches.value_of("mysql").unwrap().parse() {
        Ok(c) => c,
        Err(err) => {
            error!("Failed to parse mysql uri: {}", err);
            return 1;
        }
    };
    let commit_interval: usize = match matches.value_of("commit-interval").unwrap().parse() {
        Ok(c) => c,
        Err(err) => {
            error!("Failed to parse commiut url uri: {}", err);
            return 1;
        }
    };
    let commit_interval = match commit_interval {
        0 => None,
        c => Some(std::time::Duration::from_secs(c as _ ))
    };
    let request_timeout: usize = match matches.value_of("request-timeout").unwrap().parse() {
        Ok(c) => c,
        Err(err) => {
            error!("Failed to parse commiut url uri: {}", err);
            return 1;
        }
    };
    let request_timeout = match request_timeout {
        0 => None,
        c => Some(std::time::Duration::from_secs(c as _ ))
    };

    let write_streams: usize = match matches.value_of("write-streams").unwrap().parse() {
        Ok(0) => {
            error!("Invalid number 0 for write-streams");
            return 1;
        }
        Ok(c) => c,
        Err(err) => {
            error!("Failed to parse write-streams: {}", err);
            return 1;
        }
    };

    let boards = matches
        .values_of("boards")
        .unwrap()
        .map(|x| String::from(x))
        .collect::<Vec<_>>();

    info!("Importing Boards: \"{}\"", boards.join("\",\""));

    let mut runtime = tokio::runtime::Builder::new()
        .threaded_scheduler()
        .enable_all()
        .thread_name("torako")
        .build()
        .unwrap();

    let mut upload_url = lnx_url.clone();
    upload_url.set_path(&format!("/indexes/{}/documents", &lnx_index));
    upload_url.set_query(Some("wait=true"));

    let mut commit_url = lnx_url.clone();
    commit_url.set_path(&format!("/indexes/{}/commit", &lnx_index));

    let r = LnxReIndex::new(upload_url, commit_url, lnx_key, request_timeout, mysql_url, boards, write_streams);
    let r = runtime.block_on(r.and_then(|r| r.build(commit_interval)));

    match r {
        Ok(_) => 0,
        Err(err) => {
            error!("Reindexing failed: {}", err);
            1
        }
    }
}
