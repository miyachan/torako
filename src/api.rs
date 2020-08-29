use std::net::SocketAddr;
use std::sync::{atomic::Ordering, Arc};
use std::time::{Duration, Instant, SystemTime};

use clap::crate_version;
use futures::prelude::*;
use log::{error, info};
use rustc_hash::FxHashMap;
use serde::Serialize;
use warp::{http::StatusCode, Filter};

#[derive(Serialize, Default)]
struct BoardMetrics {
    posts: u64,
    deleted: u64,
    warmed_up: bool,
    last_modified: i64,
    cloudflare_blocked: u64,
}

impl std::ops::Add for BoardMetrics {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            posts: self.posts + other.posts,
            deleted: self.deleted + other.deleted,
            last_modified: self.last_modified.max(other.last_modified),
            warmed_up: self.warmed_up && other.warmed_up,
            cloudflare_blocked: self.cloudflare_blocked + other.cloudflare_blocked,
        }
    }
}

impl From<&crate::imageboard::Metrics> for BoardMetrics {
    fn from(metrics: &crate::imageboard::Metrics) -> Self {
        Self {
            posts: metrics.posts.load(Ordering::Relaxed),
            deleted: metrics.deleted.load(Ordering::Relaxed),
            last_modified: metrics.last_modified.load(Ordering::Relaxed),
            warmed_up: metrics.warmed_up.load(Ordering::Relaxed),
            cloudflare_blocked: metrics.cloudflare_blocked.load(Ordering::Relaxed),
        }
    }
}

#[derive(Serialize)]
struct Info {
    name: String,
    #[serde(with = "humantime_serde")]
    uptime: Duration,
    #[serde(with = "humantime_serde")]
    started_at: Option<SystemTime>,
    version: String,
    boards: FxHashMap<String, BoardMetrics>,
    all_boards: BoardMetrics,
    storage: FxHashMap<&'static str, Box<dyn erased_serde::Serialize>>,
    ok: bool,
}

#[derive(Serialize)]
struct ErrorMessage {
    code: u16,
    message: String,
}

fn with_boards(
    db: Arc<Vec<Arc<crate::imageboard::Metrics>>>,
) -> impl Filter<
    Extract = (Arc<Vec<Arc<crate::imageboard::Metrics>>>,),
    Error = std::convert::Infallible,
> + Clone {
    warp::any().map(move || db.clone())
}

fn with_storage(
    db: Arc<Vec<Box<dyn crate::storage::MetricsProvider>>>,
) -> impl Filter<
    Extract = (Arc<Vec<Box<dyn crate::storage::MetricsProvider>>>,),
    Error = std::convert::Infallible,
> + Clone {
    warp::any().map(move || db.clone())
}

fn info(
    start_time: Instant,
    system_start: SystemTime,
    board: Arc<Vec<Arc<crate::imageboard::Metrics>>>,
    storage: Arc<Vec<Box<dyn crate::storage::MetricsProvider>>>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path::end()
        .and(warp::get())
        .and(with_boards(board))
        .and(with_storage(storage))
        .map(
            move |board: Arc<Vec<Arc<crate::imageboard::Metrics>>>,
                  storage: Arc<Vec<Box<dyn crate::storage::MetricsProvider>>>| {
                let info = Info {
                    name: "torako".into(),
                    uptime: start_time.elapsed(),
                    started_at: Some(system_start),
                    version: crate_version!().into(),
                    boards: board
                        .iter()
                        .map(|b| (b.board.clone(), b.as_ref().into()))
                        .collect(),
                    all_boards: board
                        .iter()
                        .fold(BoardMetrics::default(), |acc, x| acc + x.as_ref().into()),
                    storage: storage.iter().map(|s| (s.name(), s.metrics())).collect(),
                    ok: true,
                };
                Ok(warp::reply::json(&info))
            },
        )
}

async fn handle_rejection(
    err: warp::Rejection,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    let code;
    let message;

    if err.is_not_found() {
        code = StatusCode::NOT_FOUND;
        message = "NOT_FOUND".to_string();
    } else if let Some(_) = err.find::<warp::filters::body::BodyDeserializeError>() {
        message = "BAD_REQUEST".to_string();
        code = StatusCode::BAD_REQUEST;
    } else if let Some(_) = err.find::<warp::reject::MethodNotAllowed>() {
        code = StatusCode::METHOD_NOT_ALLOWED;
        message = "METHOD_NOT_ALLOWED".to_string();
    } else {
        error!("unhandled rejection: {:?}", err);
        code = StatusCode::INTERNAL_SERVER_ERROR;
        message = "UNHANDLED_REJECTION".to_string();
    }

    let json = warp::reply::json(&ErrorMessage {
        code: code.as_u16(),
        message: message.into(),
    });

    Ok(warp::reply::with_status(json, code))
}

pub fn serve(
    addr: SocketAddr,
    board_metrics: Vec<Arc<crate::imageboard::Metrics>>,
    storage_metrics: Vec<Box<dyn crate::storage::MetricsProvider>>,
) -> impl Future<Output = ()> {
    let routes = info(
        Instant::now(),
        SystemTime::now(),
        Arc::new(board_metrics),
        Arc::new(storage_metrics),
    );

    info!("Starting API server on: {}", addr);
    warp::serve(routes.recover(handle_rejection)).run(addr)
}
