use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

#[cfg(not(target_family = "windows"))]
use std::os::unix::fs::PermissionsExt;

use futures::prelude::*;
use rand::distributions::Alphanumeric;
use rand::Rng;
use tokio::io::AsyncWrite;

use super::Error;

enum FileState {
    Open,
    Rename(Pin<Box<dyn Future<Output = io::Result<()>> + Send>>),
    Permission(Pin<Box<dyn Future<Output = io::Result<()>> + Send>>),
}

impl Default for FileState {
    fn default() -> Self {
        FileState::Open
    }
}

struct File {
    filename: PathBuf,
    tempname: PathBuf,
    inner: tokio::fs::File,
    state: FileState,

    #[cfg(target_family = "windows")]
    #[allow(dead_code)]
    media_group: (),
    #[cfg(not(target_family = "windows"))]
    media_group: Option<users::Group>,
}

pub struct FileSystem {
    media_path: PathBuf,
    tmp_dir: PathBuf,

    #[cfg(target_family = "windows")]
    #[allow(dead_code)]
    media_group: (),
    #[cfg(not(target_family = "windows"))]
    media_group: Option<users::Group>,

    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    io_ring: rio::Rio,

    #[cfg(not(all(feature = "io-uring", target_os = "linux")))]
    #[allow(dead_code)]
    io_ring: (),
}

impl FileSystem {
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    fn io_ring() -> rio::Rio {
        rio::new().expect("create uring")
    }

    #[cfg(not(all(feature = "io-uring", target_os = "linux")))]
    fn io_ring() -> () {
        ()
    }

    #[cfg(target_family = "windows")]
    fn group<T: AsRef<OsStr>>(_group: Option<T>) -> Result<(), Error> {
        Ok(())
    }

    #[cfg(not(target_family = "windows"))]
    fn group<T: AsRef<OsStr>>(group: Option<T>) -> Result<Option<users::Group>, Error> {
        group
            .as_ref()
            .map(|g| users::get_group_by_name(g).ok_or(Error::InvalidUserGroup))
            .transpose()
    }

    pub async fn new<T: AsRef<Path>, U: AsRef<Path>, V: AsRef<OsStr>>(
        media_path: T,
        tmp_dir: U,
        media_group: Option<V>,
    ) -> Result<Self, Error> {
        let tmp_dir = tmp_dir.as_ref();

        match tokio::fs::metadata(&tmp_dir).await {
            Ok(m) => {
                if !m.is_dir() {
                    return Err(Error::InvalidTempDir(tmp_dir.to_owned()));
                }
            }
            Err(_) => tokio::fs::create_dir_all(&tmp_dir).await?,
        };

        Ok(Self {
            io_ring: Self::io_ring(),
            media_path: PathBuf::from(media_path.as_ref()),
            tmp_dir: PathBuf::from(tmp_dir),
            media_group: Self::group(media_group)?,
        })
    }
}

impl FileSystem {
    pub async fn exists<T: AsRef<Path>>(&self, filepath: T) -> Result<bool, Error> {
        Ok(tokio::fs::metadata(filepath).await.is_ok())
    }

    pub async fn open<T: AsRef<Path>>(
        &self,
        filepath: T,
        _size: usize,
    ) -> Result<impl AsyncWrite + Send, Error> {
        let filename = self.media_path.join(filepath.as_ref());
        let subdir = filename.parent().unwrap();

        if tokio::fs::metadata(&subdir).await.is_err() {
            let parent = subdir.parent().unwrap();
            let has_parent = parent.exists();
            if let Err(err) = tokio::fs::create_dir_all(&subdir).await {
                return Err(Error::from(err));
            }
            #[cfg(target_family = "windows")]
            {
                // get rid of warning on windows build
                let _has_parent = has_parent;
            }
            #[cfg(not(target_family = "windows"))]
            if let Some(group) = &self.media_group {
                if !has_parent {
                    tokio::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o755))
                        .await?;
                    let _ = nix::unistd::chown(
                        parent,
                        None,
                        Some(nix::unistd::Gid::from_raw(group.gid())),
                    );
                }
                tokio::fs::set_permissions(&subdir, std::fs::Permissions::from_mode(0o755)).await?;
                let _ =
                    nix::unistd::chown(subdir, None, Some(nix::unistd::Gid::from_raw(group.gid())));
            }
        }

        let tempname = self.tmp_dir.join(
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(16)
                .collect::<String>(),
        );

        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tempname)
            .await?;
        Ok(File {
            filename,
            tempname,
            inner: file,
            state: FileState::default(),
            media_group: self.media_group.clone(),
        })
    }
}

#[cfg(not(all(feature = "io-uring", target_os = "linux")))]
impl AsyncWrite for File {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            match &mut self.state {
                FileState::Open => match Pin::new(&mut self.inner).poll_shutdown(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(r) => match r {
                        Ok(_) => {
                            self.state = FileState::Rename(
                                tokio::fs::rename(self.tempname.clone(), self.filename.clone())
                                    .boxed(),
                            );
                            continue;
                        }
                        Err(err) => return Poll::Ready(Err(err)),
                    },
                },
                FileState::Rename(fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(r) => match r {
                        Ok(_) => {
                            #[cfg(not(target_family = "windows"))]
                            if let Some(_) = &self.media_group {
                                self.state = FileState::Permission(
                                    tokio::fs::set_permissions(
                                        self.filename.clone(),
                                        std::fs::Permissions::from_mode(0o644),
                                    )
                                    .boxed(),
                                );
                                continue;
                            }
                            return Poll::Ready(Ok(()));
                        }
                        Err(err) => return Poll::Ready(Err(err)),
                    },
                },
                FileState::Permission(fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(r) => match r {
                        Ok(_) => {
                            #[cfg(not(target_family = "windows"))]
                            if let Some(group) = &self.media_group {
                                let _ = nix::unistd::chown(
                                    &self.filename,
                                    None,
                                    Some(nix::unistd::Gid::from_raw(group.gid())),
                                );
                            }

                            return Poll::Ready(Ok(()));
                        }
                        Err(err) => return Poll::Ready(Err(err)),
                    },
                },
            }
        }
    }
}

#[cfg(all(feature = "io-uring", target_os = "linux"))]
impl AsyncWrite for File {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        unimplemented!()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        unimplemented!()
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        unimplemented!()
    }
}
