#![type_length_limit = "5136020"]

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

use clap::{crate_version, App, AppSettings, Arg, ArgMatches, SubCommand};
use futures::prelude::*;
use log::{error, info, warn};
use pretty_env_logger;
use thiserror::Error;

mod api;
mod config;
mod feed;
mod imageboard;
mod storage;
mod util;

pub use feed::FeedSinkExt;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Asagi: {}", .0)]
    Asagi(#[from] storage::asagi::Error),
    #[error("PG Search: {}", .0)]
    PG(#[from] storage::search_pg::Error),
}

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
                board
                    .url_media_filename
                    .or(config.boards.url_media_filename)
                    .unwrap_or(false),
            )
        })
        .collect::<Vec<_>>();
    if boards.len() == 0 {
        error!("No boards were configured for archiving!");
        return 1;
    }

    let asagi = match config.backend.asagi.as_ref() {
        Some(asagi_conf) if !asagi_conf.disabled => {
            let asagi = {
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
                        |acc, (name, thumbs, media)| {
                            acc.with_board(
                                &name,
                                thumbs,
                                media,
                                asagi_conf
                                    .boards
                                    .get(&name)
                                    .and_then(|x| x.media_storage.clone()),
                            )
                        },
                    )
                    .with_http_client(http_client.clone())
                    .build()
                    .await
            };

            match asagi {
                Ok(a) => Some(a),
                Err(err) => {
                    error!("Failed to initialize asagi storage backend: {}", err);
                    return 1;
                }
            }
        }
        _ => None,
    };

    let search = match config.backend.asagi_pg_search.as_ref() {
        Some(conf) if !conf.disabled => {
            let builder = storage::search_pg::SearchBuilder::from(conf);
            match builder.build().await {
                Ok(a) => Some(a),
                Err(err) => {
                    error!(
                        "Failed to initialize asagi pg storage search backend: {}",
                        err
                    );
                    return 1;
                }
            }
        }
        _ => None,
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
    let mut storage_metrics: Vec<Box<dyn crate::storage::MetricsProvider>> = vec![];
    if let Some(asagi) = asagi.as_ref() {
        storage_metrics.push(Box::new(asagi.metrics_provider()));
    }
    if let Some(search) = search.as_ref() {
        storage_metrics.push(Box::new(search.metrics_provider()));
    }

    if let Some(addr) = config.api_addr {
        let addr_interface = config.api_addr_interface;
        tokio::spawn(async move {
            api::serve(addr, addr_interface, board_metrics, storage_metrics).await;
        });
    }

    info!("Initialization complete.");
    info!("Starting archiving process...");
    let boards_stream = futures::stream::select_all(boards).map(|x| Some(x));
    let mut boards_stream =
        futures::stream::select(boards_stream, stream_ender.map(|_| None).into_stream())
            .take_while(|x| future::ready(x.is_some()))
            .map(|x| Ok(x.unwrap()));
    // let res = asagi.feed_all(&mut boards_stream);
    let mut asagi = asagi.map(|asagi| asagi.sink_map_err(|err| Error::from(err)));
    let mut search = search.map(|search| search.sink_map_err(|err| Error::from(err)));
    let null = config
        .backend
        .null
        .and_then(|n| match n.disabled {
            true => None,
            false => Some(()),
        })
        .map(|_| futures::sink::drain().sink_map_err(|_| unreachable!()));

    let res = match (asagi.as_mut(), search.as_mut()) {
        (Some(asagi), None) => asagi.feed_all(&mut boards_stream).await,
        (None, Some(search)) => search.feed_all(&mut boards_stream).await,
        (Some(asagi), Some(search)) => {
            let mut asagi = asagi.fanout(search);
            asagi.feed_all(&mut boards_stream).await
        }
        _ if null.is_some() => null.unwrap().feed_all(&mut boards_stream).await,
        _ => {
            error!("No valid storage backend was configured.");
            return 1;
        }
    };
    match res {
        Ok(_) => {
            let asagi_close = match asagi.as_mut() {
                Some(asagi) => futures::future::Either::Left(asagi.close()),
                None => futures::future::Either::Right(futures::future::ready(Ok(()))),
            };
            let search_close = match search.as_mut() {
                Some(search) => futures::future::Either::Left(search.close()),
                None => futures::future::Either::Right(futures::future::ready(Ok(()))),
            };
            let close = futures::future::join(asagi_close, search_close)
                .map(|(a, b)| a.and(b))
                .await;
            match close {
                Ok(_) => {
                    info!("Goodbye.");
                    0
                }
                Err(err) => {
                    error!("An error occured shutting down: {}", err);
                    1
                }
            }
        }
        Err(err) => {
            error!("Torako failed: {}", err);
            1
        }
    }
}

fn run<'a>(matches: ArgMatches<'a>) -> i32 {
    match matches.subcommand() {
        #[cfg(feature = "pgs-reindex")]
        ("pgs-reindex", Some(sub)) => return util::pgs::reindex(sub),
        ("boo", Some(sub)) => return util::boo(sub),
        _ => (),
    };

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

    if env::var("TORAKO_LOG").is_err() {
        env::set_var("TORAKO_LOG", "torako=info")
    }
    pretty_env_logger::try_init_timed_custom_env("TORAKO_LOG").unwrap();

    let pgs_reindex: Option<App<'_, '_>> = {
        #[cfg(feature = "pgs-reindex")]
        {
            let num_cpus: &'static str = Box::leak(num_cpus::get().to_string().into_boxed_str());
            Some(
                SubCommand::with_name("pgs-reindex")
                    .about("Reindex a postgres search database from MySQL Asagi")
                    .arg(
                        Arg::with_name("postgres")
                            .long("postgres")
                            .value_name("POSTGRES URL")
                            .takes_value(true)
                            .required(true),
                    )
                    .arg(
                        Arg::with_name("mysql")
                            .long("mysql")
                            .value_name("MYSQL URL")
                            .takes_value(true)
                            .required(true),
                    )
                    .arg(
                        Arg::with_name("write-streams")
                            .long("write-streams")
                            .value_name("COUNT")
                            .default_value(num_cpus)
                            .takes_value(true)
                            .required(true),
                    )
                    .arg(Arg::with_name("boards").multiple(true).required(true)),
            )
        }

        #[cfg(not(feature = "pgs-reindex"))]
        {
            None
        }
    };

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
        .subcommand(SubCommand::with_name("boo").setting(AppSettings::Hidden))
        .subcommands(pgs_reindex.into_iter())
        .get_matches();

    match run(matches) {
        0 => return,
        i => std::process::exit(i),
    };
}
