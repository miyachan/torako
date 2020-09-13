use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWrite;
use tokio::sync::mpsc::UnboundedSender;
use void::Void;

use super::Error;

struct File {
    sender: Option<UnboundedSender<Result<Bytes, Void>>>,
    upload: Pin<
        Box<dyn Future<Output = Result<Result<(), reqwest::Error>, tokio::task::JoinError>> + Send>,
    >,
}

pub struct Backblaze {
    client: reqwest::Client,
    bucket_id: String,
    bucket_name: String,
    key_id: String,
    key: String,
    authorization_token: AtomicPtr<String>,
    download_url: reqwest::Url,
    api_url: reqwest::Url,
    check_exists: bool,
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
    ) -> Result<Self, Error> {
        let empty = Box::into_raw(Box::new(String::from("")));
        let mut backblaze = Self {
            client,
            bucket_id: String::from(bucket_id.as_ref()),
            bucket_name: String::from(""),
            key_id: String::from(key_id.as_ref()),
            key: String::from(key.as_ref()),
            authorization_token: AtomicPtr::new(empty),
            download_url: reqwest::Url::parse("http://localhost/").unwrap(),
            api_url: reqwest::Url::parse("http://localhost/").unwrap(),
            check_exists,
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

        Ok(backblaze)
    }
}

impl Backblaze {
    pub async fn exists<T: AsRef<Path>>(&self, filepath: T) -> Result<bool, Error> {
        if !self.check_exists {
            return Ok(false);
        }
        loop {
            // maybe use cloudflare URL if provided?
            let mut file_url = self.download_url.clone();
            file_url.set_path(&format!(
                "/file/{}/{}",
                self.bucket_name,
                filepath.as_ref().to_string_lossy()
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
                filepath.as_ref().to_string_lossy().to_string(),
            )
            .header("X-Bz-Content-Sha1", "do_not_verify")
            .header("X-Bz-Info-b2-cache-control", "public%2C+max-age=31104000")
            .send()
            .and_then(|resp| async { resp.error_for_status() })
            .map_ok(|_| ());

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
