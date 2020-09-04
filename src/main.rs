#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
use jemallocator::Jemalloc;

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use std::env;
use std::panic;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::{crate_version, App, Arg, ArgMatches};
use futures::prelude::*;
use log::{error, info, warn};
use pretty_env_logger;

mod api;
mod config;
mod feed;
mod imageboard;
mod storage;

pub use feed::FeedSinkExt;

async fn run_async(config: config::Config) -> i32 {
    println!("Torako: Imageboard Archiver");
    println!("\tVersion: {}", crate_version!());
    println!("\tRepo: https://github.com/miyachan/torako");
    let http_client = {
        let mut b = reqwest::Client::builder();
        if let Some(timeout) = config.request_timeout {
            b = b.timeout(timeout);
        }
        if config.request_only_proxy && config.request_proxy.is_empty() {
            error!("Configuration error: request_only_proxy is true but not proxies were provided");
            return 1;
        }
        if !config.request_proxy.is_empty() {
            let rr = AtomicUsize::new(0);
            let proxies = std::iter::once(None)
                .filter(|_| !config.request_only_proxy)
                .chain(config.request_proxy.iter().cloned().map(|p| Some(p)))
                .collect::<Vec<_>>();
            b = b.proxy(reqwest::Proxy::custom(move |_| {
                let r = rr.fetch_add(1, Ordering::AcqRel);
                proxies.get(r % proxies.len()).unwrap().clone()
            }));
        }
        b.build().unwrap()
    };
    let rate_limiter = config.rate_limit.map(|limit| {
        let quota = governor::Quota::per_second(limit);
        Arc::new(governor::RateLimiter::direct(quota))
    });
    let concurrency_limiter = config.thread_concurrency.map(|limit| {
        let sem = tokio::sync::Semaphore::new(limit.get());
        Arc::new(sem)
    });

    let boards = config
        .boards
        .boards
        .iter()
        .map(|(name, board)| {
            info!("Archiving board '{}'", name);
            imageboard::BoardStream::new(
                http_client.clone(),
                board.tls.or(config.boards.tls).unwrap_or(true),
                board
                    .host
                    .as_ref()
                    .cloned()
                    .or(config.boards.host.as_ref().cloned())
                    .unwrap_or(String::from("a.4cdn.org")),
                name,
                board
                    .refresh_rate
                    .or(config.boards.refresh_rate)
                    .unwrap_or(Duration::from_secs(10)),
                rate_limiter.clone(),
                concurrency_limiter.clone(),
                board
                    .deleted_page_threshold
                    .or(config.boards.deleted_page_threshold)
                    .unwrap_or(8),
            )
        })
        .collect::<Vec<_>>();
    if boards.len() == 0 {
        error!("No boards were configured for archiving!");
        return 1;
    }
    if config.backend.asagi.is_none() {
        error!("No valid storage backend was configured.");
        error!("Note: Currently only the Asagi backend is supported");
        return 1;
    }
    let asagi = {
        let asagi_conf = config.backend.asagi.as_ref().unwrap();
        config
            .boards
            .boards
            .iter()
            .map(|(name, board)| {
                (
                    name.clone(),
                    board
                        .download_thumbs
                        .or(config.boards.download_thumbs)
                        .unwrap_or(true),
                    board
                        .download_media
                        .or(config.boards.download_media)
                        .unwrap_or(true),
                )
            })
            .fold(
                storage::asagi::AsagiBuilder::from(asagi_conf),
                |acc, (name, thumbs, media)| acc.with_board(name, thumbs, media),
            )
            .with_http_client(http_client.clone())
            .build()
            .await
    };

    let mut asagi = match asagi {
        Ok(a) => a,
        Err(err) => {
            error!("Failed to initialize asagi storage backend: {}", err);
            return 1;
        }
    };
    let running = AtomicBool::new(true);
    let (end_stream, stream_ender) = tokio::sync::oneshot::channel();
    let mut end_stream = Some(end_stream);
    ctrlc::set_handler(move || {
        match running.swap(false, Ordering::SeqCst) {
            true => {
                if let Some(end_stream) = end_stream.take() {
                    let _ = end_stream.send(());
                }
                warn!("Received SIGTERM/SIGINT signal. Torako will try to exit cleanly (by waiting for all current posts/images to finish downloading).");
                warn!("Sending a second signal will forcefully stop Torako.");
            },
            false => {
                error!("Exiting torako...");
                std::process::exit(0);
            }
        }
    }).expect("Error setting Ctrl-C handler");

    let board_metrics = boards.iter().map(|x| x.metrics()).collect();
    let storage_metrics: Vec<Box<dyn crate::storage::MetricsProvider>> =
        vec![Box::new(asagi.metrics_provider())];

    if let Some(addr) = config.api_addr {
        tokio::spawn(async move {
            api::serve(addr, board_metrics, storage_metrics).await;
        });
    }

    info!("Initialization complete.");
    info!("Starting archiving process...");
    let boards_stream = futures::stream::select_all(boards).map(|x| Some(x));
    let mut boards_stream =
        futures::stream::select(boards_stream, stream_ender.map(|_| None).into_stream())
            .take_while(|x| future::ready(x.is_some()))
            .map(|x| Ok(x.unwrap()));
    let res = asagi.feed_all(&mut boards_stream);
    match res.await {
        Ok(_) => match asagi.close().await {
            Ok(_) => {
                info!("Goodbye.");
                0
            }
            Err(err) => {
                error!("An error occured shutting down: {}", err);
                1
            }
        },
        Err(err) => {
            error!("Torako failed: {}", err);
            1
        }
    }
}

fn run<'a>(matches: ArgMatches<'a>) -> i32 {
    let config: config::Config = {
        let config_str = match std::fs::read_to_string(matches.value_of("config").unwrap()) {
            Ok(s) => s,
            Err(err) => {
                error!(
                    "Failed to read configuration @ {}: {}",
                    matches.value_of("config").unwrap(),
                    err
                );
                return 1;
            }
        };
        match toml::from_str(&config_str) {
            Ok(c) => c,
            Err(err) => {
                error!(
                    "Failed to parse configuration file @ {}: {}",
                    matches.value_of("config").unwrap(),
                    err
                );
                return 1;
            }
        }
    };

    let mut runtime = tokio::runtime::Builder::new()
        .threaded_scheduler()
        .enable_all()
        .thread_name("torako")
        .build()
        .unwrap();

    let r = runtime.block_on(run_async(config));

    r
}

fn main() {
    let orig_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        std::process::exit(1);
    }));

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "torako=info")
    }
    pretty_env_logger::init_timed();

    let matches = App::new("Torako")
        .author("github.com/miyachan")
        .about("Torako: Imageboard archiver backend.")
        .version(crate_version!())
        .arg(
            Arg::with_name("config")
                .long("config")
                .short("c")
                .help("Path to configuration file")
                .takes_value(true)
                .value_name("FILE")
                .default_value("./Torako.toml")
                .env("CONFIG"),
        )
        .get_matches();

    match run(matches) {
        0 => return,
        i => std::process::exit(i),
    };
}
