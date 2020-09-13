use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::prelude::*;
use log::error;
use rusoto_core::{credential, HttpClient, Region, RusotoError};
use rusoto_s3::{S3Client, S3 as S3Trait};
use tokio::io::AsyncWrite;
use tokio::sync::mpsc::UnboundedSender;

use super::Error;

struct File {
    sender: Option<UnboundedSender<Result<Bytes, std::io::Error>>>,
    upload: Pin<
        Box<
            dyn Future<
                    Output = Result<
                        Result<rusoto_s3::PutObjectOutput, RusotoError<rusoto_s3::PutObjectError>>,
                        tokio::task::JoinError,
                    >,
                > + Send,
        >,
    >,
}

pub struct S3 {
    s3_client: Arc<S3Client>,
    acl: Option<String>,
    bucket: String,
    check_exists: bool,
}

impl S3 {
    pub async fn new<
        T: AsRef<str>,
        U: AsRef<str>,
        V: AsRef<str>,
        W: AsRef<str>,
        X: AsRef<str>,
        Y: AsRef<str>,
    >(
        access_key: T,
        secret_access_key: U,
        bucket: V,
        region: W,
        endpoint: Option<X>,
        acl: Option<Y>,
        check_exists: bool,
    ) -> Result<Self, Error> {
        let provider = credential::StaticProvider::new_minimal(
            access_key.as_ref().into(),
            secret_access_key.as_ref().into(),
        );
        let region = match endpoint {
            Some(endpoint) => Region::Custom {
                name: region.as_ref().into(),
                endpoint: endpoint.as_ref().trim_end_matches('/').to_string(),
            },
            None => match region.as_ref().parse::<Region>() {
                Ok(r) => r,
                Err(_) => return Err(Error::S3("Invalid Region".into())),
            },
        };

        let s3 = Self {
            s3_client: Arc::new(S3Client::new_with(
                HttpClient::new().unwrap(),
                provider,
                region,
            )),
            bucket: String::from(bucket.as_ref()),
            acl: acl.map(|x| String::from(x.as_ref())),
            check_exists,
        };

        Ok(s3)
    }
}

impl S3 {
    pub async fn exists<T: AsRef<Path>>(&self, filepath: T) -> Result<bool, Error> {
        if !self.check_exists {
            return Ok(false);
        }
        let head_object_req = rusoto_s3::HeadObjectRequest {
            bucket: self.bucket.clone(),
            key: filepath.as_ref().to_string_lossy().to_string(),
            ..Default::default()
        };
        match self.s3_client.head_object(head_object_req).await {
            Ok(_) => Ok(true),
            Err(RusotoError::Service(_)) => Ok(false),
            Err(err) => {
                if let RusotoError::Unknown(ref resp) = err {
                    // Rusoto doesn't seem to play nice with Backblaze here...
                    if resp.status == reqwest::StatusCode::NOT_FOUND {
                        return Ok(false);
                    }
                    error!(
                        "Received an unknown error from S3. Check endpoint? Status: {} Headers: {:?} Body: {}",
                        resp.status, resp.headers, resp.body.len()
                    );
                }
                Err(Error::Rusoto(Box::new(err)))
            }
        }
    }

    pub async fn open<T: AsRef<Path>>(
        &self,
        filepath: T,
        size: usize,
    ) -> Result<impl AsyncWrite + Send, Error> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let put_object = rusoto_s3::PutObjectRequest {
            acl: self.acl.clone(),
            bucket: self.bucket.clone(),
            key: filepath.as_ref().to_string_lossy().to_string(),
            body: Some(rusoto_s3::StreamingBody::new(rx)),
            cache_control: Some("public, max-age=31104000".into()),
            content_length: Some(size as i64),
            content_type: Some(
                mime_guess::from_path(filepath.as_ref())
                    .first_or_octet_stream()
                    .to_string(),
            ),
            ..Default::default()
        };
        let client = self.s3_client.clone();

        Ok(File {
            sender: Some(tx),
            upload: tokio::spawn(async move { client.put_object(put_object).await }).boxed(),
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
                                Error::Rusoto(Box::new(err)),
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
                    Err(err) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        Error::Rusoto(Box::new(err)),
                    ))),
                },
                // Maybe panic?
                Err(err) => Poll::Ready(Err(io::Error::new(io::ErrorKind::Other, err))),
            },
        }
    }
}
