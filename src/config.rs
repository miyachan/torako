use std::net::SocketAddr;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use rustc_hash::FxHashMap;
use serde::Deserialize;

#[serde(default)]
#[derive(Debug, Deserialize, Clone, Default)]
pub struct Config {
    pub api_addr: Option<SocketAddr>,
    pub rate_limit: Option<NonZeroU32>,
    pub thread_concurrency: Option<NonZeroUsize>,
    #[serde(with = "humantime_serde")]
    pub request_timeout: Option<Duration>,
    #[serde(default)]
    pub request_proxy: Vec<url::Url>,
    #[serde(default)]
    pub request_only_proxy: bool,
    pub boards: Board,
    pub backend: Backend,
}

#[serde(default)]
#[derive(Debug, Deserialize, Clone, Default)]
pub struct Board {
    pub tls: Option<bool>,
    pub host: Option<String>,
    #[serde(with = "humantime_serde")]
    pub refresh_rate: Option<Duration>,
    pub deleted_page_threshold: Option<usize>,
    pub download_thumbs: Option<bool>,
    pub download_media: Option<bool>,

    #[serde(flatten)]
    pub boards: FxHashMap<String, Board>,
}

#[serde(default)]
#[derive(Debug, Deserialize, Clone, Default)]
pub struct Backend {
    pub asagi: Option<Asagi>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Asagi {
    #[serde(default)]
    pub thumbs: Option<bool>,
    #[serde(default)]
    pub media: Option<bool>,
    #[serde(default)]
    pub media_url: Option<url::Url>,
    #[serde(default)]
    pub thumb_url: Option<url::Url>,
    #[serde(default)]
    pub media_path: Option<PathBuf>,
    #[serde(default)]
    pub tmp_dir: Option<PathBuf>,
    #[serde(default)]
    pub old_dir_structure: Option<bool>,
    #[serde(default)]
    pub web_unix_group: Option<String>,
    #[serde(default)]
    pub persist_error_is_fatal: Option<bool>,
    #[serde(default)]
    pub inflight_posts: Option<NonZeroUsize>,
    #[serde(default)]
    pub concurrent_downloads: Option<NonZeroUsize>,
    #[serde(default)]
    pub media_backpressure: Option<bool>,
    pub database: AsagiDatabase,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AsagiDatabase {
    pub url: url::Url,
    #[serde(default)]
    pub charset: Option<String>,
    #[serde(default)]
    pub use_triggers: Option<bool>,
    #[serde(default)]
    pub compute_stats: Option<bool>,
    #[serde(default)]
    pub truncate_fields: Option<bool>,
    #[serde(default)]
    pub sql_set_utc: Option<bool>,
}
