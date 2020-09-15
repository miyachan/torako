use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::prelude::*;
use log::{debug, warn};
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use probabilistic_collections::bloom::ScalableBloomFilter;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWrite;
use tokio::sync::mpsc::UnboundedSender;
use void::Void;

use super::Error;

const URLENCODE_FRAGMENT: &AsciiSet = &NON_ALPHANUMERIC.remove(b'/').remove(b'.');

struct File {
    sender: Option<UnboundedSender<Result<Bytes, Void>>>,
    upload: Pin<
        Box<dyn Future<Output = Result<Result<(), reqwest::Error>, tokio::task::JoinError>> + Send>,
    >,
}

struct B2Bloom(Mutex<ScalableBloomFilter<String>>);

impl B2Bloom {
    pub fn contains<T: AsRef<str>>(&self, key: T) -> bool {
        let b = self.0.lock().unwrap();
        b.contains(key.as_ref())
    }

    pub fn insert<T: AsRef<str>>(&self, key: T) {
        let mut b = self.0.lock().unwrap();
        b.insert(key.as_ref())
    }

    pub fn serialize(&self) -> Vec<u8> {
        let b = self.0.lock().unwrap();
        bincode::serialize(&*b).unwrap()
    }
}

pub struct Backblaze {
    client: reqwest::Client,
    bucket_id: String,
    bucket_name: String,
    key_id: String,
    key: String,
    authorization_token: Arc<AtomicPtr<String>>,
    download_url: reqwest::Url,
    api_url: reqwest::Url,
    check_exists: bool,
    bloom: Option<Arc<B2Bloom>>,
    bloom_dirty: Arc<AtomicBool>,
    quit_upload: Option<tokio::sync::oneshot::Sender<()>>,
}

#[derive(Debug, Deserialize, Clone)]
struct B2AuthorizeAccount {
    #[serde(rename = "accountId")]
    account_id: String,
    #[serde(rename = "authorizationToken")]
    authorization_token: String,
    allowed: B2AuthorizeAccountAllowed,
    #[serde(rename = "downloadUrl")]
    download_url: reqwest::Url,
    #[serde(rename = "apiUrl")]
    api_url: reqwest::Url,
}

#[derive(Debug, Deserialize, Clone)]
struct B2AuthorizeAccountAllowed {
    #[serde(rename = "bucketId")]
    bucket_id: Option<String>,
    capabilities: Vec<String>,
    #[serde(rename = "namePrefix")]
    name_prefix: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
struct B2ListBucketsRequest {
    #[serde(rename = "accountId")]
    account_id: String,
    #[serde(rename = "bucketId")]
    bucket_id: String,
}

#[derive(Debug, Deserialize, Clone)]
struct B2ListBuckets {
    buckets: Vec<B2ListBucket>,
}

#[derive(Debug, Deserialize, Clone)]
struct B2ListBucket {
    #[serde(rename = "bucketId")]
    bucket_id: String,
    #[serde(rename = "bucketName")]
    bucket_name: String,
}

#[derive(Debug, Serialize, Clone)]
struct B2UploadUrlRequest {
    #[serde(rename = "bucketId")]
    bucket_id: String,
}

#[derive(Debug, Deserialize, Clone)]
struct B2UploadUrl {
    #[serde(rename = "bucketId")]
    bucket_id: String,
    #[serde(rename = "authorizationToken")]
    authorization_token: String,
    #[serde(rename = "uploadUrl")]
    upload_url: reqwest::Url,
}

#[warn(dead_code)]
#[derive(Debug, Deserialize, Clone)]
struct B2Error {
    code: String,
    message: String,
    status: usize,
}

impl Backblaze {
    fn set_auth_token(&self, token: String) {
        let new_token = Box::into_raw(Box::new(token));
        let old_ptr = self.authorization_token.swap(new_token, Ordering::AcqRel);
        let old = unsafe { Box::from_raw(old_ptr) };
        drop(old);
    }

    fn get_auth_token(&self) -> &'static str {
        let ptr = self.authorization_token.load(Ordering::Acquire);
        return unsafe { (*ptr).as_ref() };
    }

    async fn renew_token(&self) -> Result<(reqwest::Url, reqwest::Url, String), Error> {
        let token = self
            .client
            .get("https://api.backblazeb2.com/b2api/v2/b2_authorize_account")
            .basic_auth(self.key_id.as_str(), Some(self.key.as_str()))
            .send()
            .and_then(|resp| async { resp.error_for_status() })
            .and_then(|resp| resp.json::<B2AuthorizeAccount>())
            .map_err(|err| match err.status() {
                Some(reqwest::StatusCode::UNAUTHORIZED) => Error::B2Config("Unauthorized"),
                _ => Error::B2(err),
            })
            .await?;
        if let Some(bucket) = token.allowed.bucket_id {
            if bucket != self.bucket_id {
                return Err(Error::B2Config(
                    "The configured key id is not allowed to access the configured bucket.",
                ));
            }
        }
        if !token.allowed.capabilities.iter().any(|e| e == "readFiles") {
            return Err(Error::B2Config(
                "The configured key id is not allowed to read files.",
            ));
        }
        if !token.allowed.capabilities.iter().any(|e| e == "writeFiles") {
            return Err(Error::B2Config(
                "The configured key id is not allowed to write files.",
            ));
        }

        self.set_auth_token(token.authorization_token);

        Ok((token.download_url, token.api_url, token.account_id))
    }

    pub async fn new<T: AsRef<str>, U: AsRef<str>, V: AsRef<str>>(
        client: reqwest::Client,
        bucket_id: T,
        key_id: U,
        key: V,
        check_exists: bool,
        bloom_config: Option<crate::config::AsagiB2StorageBloom>,
    ) -> Result<Self, Error> {
        let empty = Box::into_raw(Box::new(String::from("")));
        let mut backblaze = Self {
            client,
            bucket_id: String::from(bucket_id.as_ref()),
            bucket_name: String::from(""),
            key_id: String::from(key_id.as_ref()),
            key: String::from(key.as_ref()),
            authorization_token: Arc::new(AtomicPtr::new(empty)),
            download_url: reqwest::Url::parse("http://localhost/").unwrap(),
            api_url: reqwest::Url::parse("http://localhost/").unwrap(),
            check_exists,
            bloom: None,
            bloom_dirty: Arc::new(AtomicBool::new(false)),
            quit_upload: None,
        };
        let (download_url, api_url, account_id) = backblaze.renew_token().await?;
        backblaze.download_url = download_url;
        backblaze.api_url = api_url;

        {
            let mut lookup_url = backblaze.api_url.clone();
            lookup_url.set_path("/b2api/v2/b2_list_buckets");
            let r = backblaze
                .client
                .post(lookup_url)
                .json(&B2ListBucketsRequest {
                    account_id,
                    bucket_id: backblaze.bucket_id.clone(),
                })
                .header(reqwest::header::AUTHORIZATION, backblaze.get_auth_token())
                .send()
                .and_then(|resp| async { resp.error_for_status() })
                .and_then(|resp| resp.json::<B2ListBuckets>())
                .map_err(|err| match err.status() {
                    Some(reqwest::StatusCode::UNAUTHORIZED) => Error::B2Unauthorized,
                    _ => Error::B2(err),
                })
                .await?;
            match r
                .buckets
                .iter()
                .find(|x| x.bucket_id == backblaze.bucket_id)
            {
                Some(bucket) => {
                    backblaze.bucket_name = bucket.bucket_name.clone();
                }
                None => return Err(Error::B2Config("bucket not found")),
            };
        }

        if let Some(config) = bloom_config {
            if !config.disabled.unwrap_or(false) {
                let file_key = match config.file_key {
                    Some(k) => k.as_str().to_owned(),
                    None => String::from("torako.bloom"),
                };
                let mut file_url = backblaze.download_url.clone();
                file_url.set_path(&format!("/file/{}/{}", backblaze.bucket_name, file_key,));

                let initial_bit_count = config
                    .initial_bit_count
                    .map(|x| x.get())
                    .unwrap_or(100000000);
                let false_positive_rate = config.false_positive_rate.unwrap_or(0.05);

                let bloom = backblaze
                    .client
                    .get(file_url)
                    .header(reqwest::header::AUTHORIZATION, backblaze.get_auth_token())
                    .send()
                    .and_then(|resp| async { resp.error_for_status() })
                    .then(|resp| async {
                        match resp {
                            Ok(resp) => {
                                let full = resp.bytes().await.map_err(|_| Error::Bloom)?;
                                match bincode::deserialize(full.as_ref()) {
                                    Ok(b) => {
                                        debug!("Successfully loaded bloom filter.");
                                        Ok(b)
                                    }
                                    Err(err) => {
                                        warn!("Bloomfilter seems to be corrupted, starting over...: {}", err);
                                        Ok(ScalableBloomFilter::new(
                                            initial_bit_count,
                                            false_positive_rate,
                                            2.0,
                                            0.5
                                        ))
                                    }
                                }
                            }
                            Err(err) => match err.status() {
                                Some(reqwest::StatusCode::NOT_FOUND) => {
                                    Ok(ScalableBloomFilter::new(
                                        initial_bit_count,
                                        false_positive_rate,
                                        2.0,
                                        0.5
                                    ))
                                },
                                Some(reqwest::StatusCode::UNAUTHORIZED) => Err(Error::B2Unauthorized),
                                _ => Err(Error::B2(err)),
                            },
                        }
                    })
                    .await?;

                let bloom = Arc::new(B2Bloom(Mutex::new(bloom)));
                backblaze.bloom = Some(bloom.clone());
                let save_freqency = config.upload_frequency.unwrap_or(Duration::from_secs(300));
                if save_freqency.as_nanos() == 0 {
                    panic!("Save frequency must be non zero.");
                }

                let (rx, tx) = tokio::sync::oneshot::channel();

                let uploader =
                    futures::stream::select(
                        tokio::time::interval_at(
                            tokio::time::Instant::now() + save_freqency,
                            save_freqency,
                        )
                        .map(|x| Some(x)),
                        tx.map(|_| None).into_stream(),
                    )
                    .take_while(|x| futures::future::ready(x.is_some()))
                    .zip(stream::repeat((
                        bloom,
                        file_key,
                        backblaze.authorization_token.clone(),
                        backblaze.bloom_dirty.clone(),
                        backblaze.api_url.clone(),
                        backblaze.client.clone(),
                        backblaze.bucket_id.clone(),
                    )))
                    .then(
                        move |(
                            _,
                            (bloom, file_key, auth_key, dirty, api_url, client, bucket_id),
                        )| async move {
                            if !dirty.load(Ordering::Relaxed) {
                                return;
                            }
                            let auth_key = {
                                let ptr = auth_key.load(Ordering::Acquire);
                                unsafe { &(*ptr) }
                            }
                            .clone();

                            debug!("Uploading bloom filter");
                            let bloom = bloom.serialize();
                            let mut upload_url = api_url.clone();
                            upload_url.set_path("/b2api/v2/b2_get_upload_url");
                            let r = client
                                .post(upload_url)
                                .json(&B2UploadUrlRequest {
                                    bucket_id: bucket_id,
                                })
                                .header(reqwest::header::AUTHORIZATION, auth_key)
                                .send()
                                .and_then(|resp| async { resp.error_for_status() })
                                .and_then(|resp| resp.json::<B2UploadUrl>())
                                .await;
                            let r = match r {
                                Ok(r) => r,
                                Err(err) => match err.status() {
                                    Some(reqwest::StatusCode::UNAUTHORIZED) => return,
                                    _ => {
                                        warn!("Failed to start upload bloom filter: {}", err);
                                        return;
                                    }
                                },
                            };

                            let upload_req = client
                                .post(r.upload_url)
                                .header(reqwest::header::AUTHORIZATION, r.authorization_token)
                                .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                                .header(reqwest::header::CONTENT_LENGTH, bloom.len())
                                .header(
                                    "X-Bz-File-Name",
                                    utf8_percent_encode(&file_key, URLENCODE_FRAGMENT).to_string(),
                                )
                                .header("X-Bz-Content-Sha1", "do_not_verify")
                                .body(bloom)
                                .send()
                                .and_then(|resp| async { resp.error_for_status() })
                                .await;
                            match upload_req {
                                Ok(_) => dirty.store(false, Ordering::Relaxed),
                                Err(err) => warn!("Failed to upload bloom filter: {}", err),
                            };
                            return;
                        },
                    )
                    .for_each(|_| futures::future::ready(()));

                tokio::spawn(uploader);
                backblaze.quit_upload = Some(rx);
            }
        }

        Ok(backblaze)
    }
}

impl Drop for Backblaze {
    fn drop(&mut self) {
        if let Some(q) = self.quit_upload.take() {
            let _ = q.send(());
        }
    }
}

impl Backblaze {
    pub async fn exists<T: AsRef<Path>>(&self, filepath: T) -> Result<bool, Error> {
        if !self.check_exists {
            return Ok(false);
        }
        if !self
            .bloom
            .as_ref()
            .map(|b| b.contains(filepath.as_ref().to_string_lossy()))
            .unwrap_or(true)
        {
            return Ok(false);
        }
        loop {
            // maybe use cloudflare URL if provided?
            let mut file_url = self.download_url.clone();
            file_url.set_path(&format!(
                "/file/{}/{}",
                self.bucket_name,
                utf8_percent_encode(
                    filepath.as_ref().to_string_lossy().as_ref(),
                    URLENCODE_FRAGMENT
                )
            ));
            let r = self
                .client
                .head(file_url)
                .header(reqwest::header::AUTHORIZATION, self.get_auth_token())
                .send()
                .and_then(|resp| async { resp.error_for_status() })
                .await;
            break match r {
                Ok(_) => Ok(true),
                Err(err) => match err.status() {
                    Some(reqwest::StatusCode::NOT_FOUND) => Ok(false),
                    Some(reqwest::StatusCode::UNAUTHORIZED) => {
                        self.renew_token().await?;
                        continue;
                    }
                    _ => Err(Error::B2(err)),
                },
            };
        }
    }

    pub async fn open<T: AsRef<Path>>(
        &self,
        filepath: T,
        size: usize,
    ) -> Result<impl AsyncWrite + Send, Error> {
        let r = loop {
            let mut upload_url = self.api_url.clone();
            upload_url.set_path("/b2api/v2/b2_get_upload_url");
            let r = self
                .client
                .post(upload_url)
                .json(&B2UploadUrlRequest {
                    bucket_id: self.bucket_id.clone(),
                })
                .header(reqwest::header::AUTHORIZATION, self.get_auth_token())
                .send()
                .and_then(|resp| async { resp.error_for_status() })
                .and_then(|resp| resp.json::<B2UploadUrl>())
                .await;
            break match r {
                Ok(r) => r,
                Err(err) => match err.status() {
                    Some(reqwest::StatusCode::UNAUTHORIZED) => {
                        self.renew_token().await?;
                        continue;
                    }
                    _ => return Err(Error::B2(err)),
                },
            };
        };

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let b2bloom = self.bloom.clone();
        let (filekey, dirty) = match b2bloom {
            Some(_) => (
                Some(String::from(filepath.as_ref().to_string_lossy())),
                Some(self.bloom_dirty.clone()),
            ),
            None => (None, None),
        };

        let upload_req = self
            .client
            .post(r.upload_url)
            .body(reqwest::Body::wrap_stream(rx))
            .header(reqwest::header::AUTHORIZATION, r.authorization_token)
            .header(
                reqwest::header::CONTENT_TYPE,
                mime_guess::from_path(filepath.as_ref())
                    .first_or_octet_stream()
                    .to_string(),
            )
            .header(reqwest::header::CONTENT_LENGTH, size)
            .header(
                "X-Bz-File-Name",
                utf8_percent_encode(
                    filepath.as_ref().to_string_lossy().as_ref(),
                    URLENCODE_FRAGMENT,
                )
                .to_string(),
            )
            .header("X-Bz-Content-Sha1", "do_not_verify")
            .header("X-Bz-Info-b2-cache-control", "public%2C+max-age=31104000")
            .send()
            .and_then(|resp| async { resp.error_for_status() })
            .map_ok(move |_| {
                if let Some(b) = b2bloom {
                    b.insert(filekey.unwrap());
                    dirty.unwrap().store(true, Ordering::Relaxed);
                }
            });

        Ok(File {
            sender: Some(tx),
            upload: tokio::spawn(upload_req).boxed(),
        })
    }
}

impl AsyncWrite for File {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let len = buf.len();
        let x = self
            .sender
            .as_ref()
            .unwrap()
            .send(Ok(Bytes::copy_from_slice(buf)));
        match x {
            Ok(_) => Poll::Ready(Ok(len)),
            Err(_) => {
                //reqwest was dropped somehow
                match Pin::new(&mut self.upload).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(join_handle) => match join_handle {
                        Ok(r) => match r {
                            Ok(_) => Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::Other,
                                Error::RequestEOF,
                            ))),
                            Err(err) => Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::Other,
                                Error::from(err),
                            ))),
                        },
                        // maybe panic?
                        Err(err) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err))),
                    },
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(c) = self.sender.take() {
            drop(c)
        }
        match Pin::new(&mut self.upload).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(join_handle) => match join_handle {
                Ok(r) => match r {
                    Ok(_) => Poll::Ready(Ok(())),
                    Err(err) => {
                        Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, Error::from(err))))
                    }
                },
                // maybe panic?
                Err(err) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err))),
            },
        }
    }
}
