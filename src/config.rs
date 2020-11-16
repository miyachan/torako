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
    pub api_addr_interface: Option<String>,
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
    pub url_media_filename: Option<bool>,

    #[serde(flatten)]
    pub boards: FxHashMap<String, Board>,
}

#[serde(default)]
#[derive(Debug, Deserialize, Clone, Default)]
pub struct Backend {
    pub asagi: Option<Asagi>,
    pub asagi_pg_search: Option<AsagiSearch>,
    pub null: Option<NullBackend>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Asagi {
    #[serde(default)]
    pub disabled: bool,
    #[serde(default)]
    pub thumbs: Option<bool>,
    #[serde(default)]
    pub media: Option<bool>,
    #[serde(default)]
    pub media_url: Option<url::Url>,
    #[serde(default)]
    pub thumb_url: Option<url::Url>,
    #[serde(default, alias = "persist_error_is_fatal")]
    pub fail_on_save_error: Option<bool>,
    #[serde(default)]
    pub retries_on_save_error: Option<usize>,
    #[serde(default)]
    pub inflight_posts: Option<NonZeroUsize>,
    #[serde(default)]
    pub concurrent_downloads: Option<NonZeroUsize>,
    #[serde(default)]
    pub media_backpressure: Option<bool>,
    #[serde(default)]
    pub media_storage: Option<AsagiStorage>,
    pub database: AsagiDatabase,
    #[serde(default)]
    pub old_dir_structure: Option<bool>,
    #[serde(default)]
    pub sha_dir_structure: Option<bool>,

    // Options kept for backwards compatibility
    #[serde(default)]
    pub media_path: Option<PathBuf>,
    #[serde(default)]
    pub tmp_dir: Option<PathBuf>,
    #[serde(default)]
    pub web_unix_group: Option<String>,
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
    #[serde(default)]
    pub mysql_engine: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AsagiStorage {
    #[serde(default)]
    pub filesystem: Option<AsagiFilesystemStorage>,
    #[serde(default)]
    pub s3: Option<AsagiS3Storage>,
    #[serde(default)]
    pub b2: Option<AsagiB2Storage>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AsagiFilesystemStorage {
    #[serde(default)]
    pub disabled: bool,
    pub media_path: PathBuf,
    #[serde(default)]
    pub tmp_dir: Option<PathBuf>,
    #[serde(default)]
    pub web_unix_group: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AsagiS3Storage {
    #[serde(default)]
    pub disabled: bool,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    pub endpoint: Option<reqwest::Url>,
    pub bucket: String,
    pub acl: Option<String>,
    #[serde(default)]
    pub check_exists: Option<bool>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AsagiB2Storage {
    #[serde(default)]
    pub disabled: bool,
    pub application_key_id: String,
    pub application_key: String,
    pub bucket_id: String,
    #[serde(default)]
    pub check_exists: Option<bool>,
    #[serde(default)]
    pub bloom: Option<AsagiB2StorageBloom>,
}

#[serde(default)]
#[derive(Debug, Deserialize, Clone, Default)]
pub struct AsagiB2StorageBloom {
    pub disabled: Option<bool>,
    pub file_key: Option<String>,
    pub initial_bit_count: Option<NonZeroUsize>,
    pub false_positive_rate: Option<f64>,
    #[serde(with = "humantime_serde")]
    pub upload_frequency: Option<Duration>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AsagiSearch {
    #[serde(default)]
    pub disabled: bool,
    pub database_url: url::Url,
    #[serde(default)]
    pub inflight_posts: Option<NonZeroUsize>,
    #[serde(default)]
    pub fail_on_save_error: Option<bool>,
    #[serde(default)]
    pub retries_on_save_error: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NullBackend {
    #[serde(default)]
    pub disabled: bool,
}
