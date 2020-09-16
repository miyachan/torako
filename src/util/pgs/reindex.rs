use std::borrow::Cow;
use std::iter::IntoIterator;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use clap::ArgMatches;
use futures::prelude::*;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{error, info};
use thiserror::Error;
use tokio_postgres::types::ToSql;

use crate::storage::search_pg::PLACEHOLDERS;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid database pool size")]
    InvalidPoolSize,
    #[error("invalid database URL provided: {}", .0)]
    InvalidDatabase(tokio_postgres::Error),
    #[error("pg database connection error: {}", .0)]
    Pool(#[from] deadpool_postgres::PoolError),
    #[error("pg database error: {}", .0)]
    DB(#[from] tokio_postgres::Error),
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

struct PGSReIndex {
    pg: deadpool_postgres::Pool,
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
    `{}`
ORDER BY `num` ASC",
        board.as_ref(),
        board.as_ref()
    )
}

impl PGSReIndex {
    async fn new<U: AsRef<str>, T: IntoIterator<Item = U>>(
        mut pg_url: url::Url,
        source_url: url::Url,
        tables: T,
        write_streams: usize,
    ) -> Result<Self, Error> {
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

        let pg_url = {
            let x = pg_url.clone();
            let pairs = x.query_pairs().filter(|x| x.0 != "pool_size");
            pg_url.query_pairs_mut().clear().extend_pairs(pairs);
            pg_url
        };

        let config = match tokio_postgres::Config::from_str(&pg_url.to_string()) {
            Ok(c) => c,
            Err(err) => return Err(Error::InvalidDatabase(err)),
        };
        let manager = deadpool_postgres::Manager::new(config, tokio_postgres::NoTls);
        let pg_pool = deadpool_postgres::Pool::new(manager, pool_size);
        info!(
            "Connecting to postgres at {}...",
            pg_url.host_str().unwrap()
        );
        drop(pg_pool.get().await?);

        info!(
            "Connecting to MySQL at {}...",
            source_url.host_str().unwrap()
        );
        let mysql_pool = sqlx::MySqlPool::connect(&source_url.to_string()).await?;
        // drop(mysql_pool.get_conn().await?);

        Ok(Self {
            pg: pg_pool,
            mysql: mysql_pool,
            tables: tables
                .into_iter()
                .map(|x| String::from(x.as_ref()))
                .collect(),
            write_streams,
        })
    }

    pub async fn build(self) -> Result<(), Error> {
        let boards = self.tables;
        let mysql_pool = self.mysql;
        let pg_pool = Arc::new(self.pg);
        let write_streams = self.write_streams;
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
                let pg_pool = pg_pool.clone();
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
                                            ",",
                                            PLACEHOLDERS[z + 21], // capcode
                                            ")",
                                        ]
                                        .join(""),
                                    )
                                }))
                                .chain(std::iter::once(Cow::Borrowed(" ON CONFLICT DO NOTHING")))
                                .collect::<String>();

                            let params = posts
                                .into_iter()
                                .map(|post| {
                                    let values: Box<[Box<dyn ToSql>]> = Box::new([
                                        Box::new(post.board),
                                        Box::new(post.thread_num as i64),
                                        Box::new(post.num as i64),
                                        Box::new(post.title),
                                        Box::new(post.name),
                                        Box::new(post.trip),
                                        Box::new(post.email),
                                        Box::new(post.poster_hash),
                                        Box::new(None::<i32>),
                                        Box::new(post.poster_country),
                                        Box::new(post.media_filename),
                                        Box::new(post.media_hash),
                                        Box::new(post.media_w.map(|x| x as i32)),
                                        Box::new(post.media_h.map(|x| x as i32)),
                                        Box::new(post.timestamp),
                                        Box::new(post.comment),
                                        Box::new(post.deleted),
                                        Box::new(false),
                                        Box::new(post.sticky),
                                        Box::new(post.spoiler),
                                        Box::new(post.op),
                                        Box::new(
                                            post.capcode
                                                .map(|c| c.chars().next().map(|c| c as i32))
                                                .flatten(),
                                        ),
                                    ]);
                                    values.into_vec()
                                })
                                .flatten()
                                .collect::<Vec<Box<dyn ToSql>>>();

                            pg_pool.get().map_err(|err| Error::from(err)).and_then(
                                move |pg_conn| async move {
                                    pg_conn
                                        .execute_raw(
                                            stmt.as_str(),
                                            params.iter().map(|x| x.as_ref()),
                                        )
                                        .map_ok(|written| (written, rows))
                                        .map_err(|err| Error::from(err))
                                        .await
                                },
                            )
                        })
                        .try_buffer_unordered(write_streams)
                        .try_fold(0, |acc, (written, rows)| {
                            pb.inc(rows as u64);
                            futures::future::ready(Ok(acc + written))
                        })
                        .map_ok(|rows| {
                            pb.finish_with_message(&info.board);
                            rows
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
    info!("Running pg-search re-indexer");

    let postgres_url: url::Url = match matches.value_of("postgres").unwrap().parse() {
        Ok(c) => c,
        Err(err) => {
            error!("Failed to parse postgres uri: {}", err);
            return 1;
        }
    };
    let mysql_url: url::Url = match matches.value_of("mysql").unwrap().parse() {
        Ok(c) => c,
        Err(err) => {
            error!("Failed to parse postgres uri: {}", err);
            return 1;
        }
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

    let r = PGSReIndex::new(postgres_url, mysql_url, boards, write_streams);
    let r = runtime.block_on(r.and_then(|r| r.build()));

    match r {
        Ok(_) => 0,
        Err(err) => {
            error!("Reindexing failed: {}", err);
            1
        }
    }
}
