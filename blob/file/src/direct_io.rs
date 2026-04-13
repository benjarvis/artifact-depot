// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! O_DIRECT aligned-buffer I/O for the file blob store.
//!
//! Provides [`AlignedBuf`] (a page-aligned allocation), [`AlignedBufPool`]
//! (a bounded recycling pool), [`DirectReader`] (an [`AsyncRead`] impl that
//! reads via `pread` into aligned buffers), and [`DirectWriter`] (a
//! [`BlobWriter`] impl that accumulates and writes aligned chunks).

use std::alloc::{self, Layout};
use std::fs::File;
use std::future::Future;
use std::io;
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};
use tokio::task::JoinHandle;

use depot_core::error;
use depot_core::store::blob::BlobWriter;

// ---------------------------------------------------------------------------
// AlignedBuf
// ---------------------------------------------------------------------------

const ALIGN: usize = 4096;

/// A heap allocation whose pointer is aligned to [`ALIGN`] bytes.
pub struct AlignedBuf {
    ptr: *mut u8,
    layout: Layout,
}

impl AlignedBuf {
    /// Allocate `size` bytes with page alignment.  Contents are uninitialized.
    ///
    /// Returns `None` if `size` is zero or the layout is invalid.
    pub fn new(size: usize) -> Option<Self> {
        if size == 0 {
            return None;
        }
        let layout = Layout::from_size_align(size, ALIGN).ok()?;
        // SAFETY: layout has non-zero size.
        let ptr = unsafe { alloc::alloc(layout) };
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        Some(Self { ptr, layout })
    }
}

impl Deref for AlignedBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        // SAFETY: ptr is valid for layout.size() bytes.
        unsafe { std::slice::from_raw_parts(self.ptr, self.layout.size()) }
    }
}

impl DerefMut for AlignedBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid and exclusively owned.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.layout.size()) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: ptr was allocated with this layout.
        unsafe { alloc::dealloc(self.ptr, self.layout) };
    }
}

// SAFETY: The raw pointer is exclusively owned — no shared mutable state.
unsafe impl Send for AlignedBuf {}

// ---------------------------------------------------------------------------
// AlignedBufPool
// ---------------------------------------------------------------------------

/// A bounded, thread-safe pool of [`AlignedBuf`]s.
pub struct AlignedBufPool {
    bufs: Mutex<Vec<AlignedBuf>>,
    buf_size: usize,
    max_pooled: usize,
}

impl AlignedBufPool {
    pub fn new(buf_size: usize, max_pooled: usize) -> Arc<Self> {
        Arc::new(Self {
            bufs: Mutex::new(Vec::with_capacity(max_pooled)),
            buf_size,
            max_pooled,
        })
    }

    /// Get a buffer (from the pool or freshly allocated).
    pub fn get(&self) -> io::Result<AlignedBuf> {
        if let Ok(mut pool) = self.bufs.lock() {
            if let Some(buf) = pool.pop() {
                return Ok(buf);
            }
        }
        AlignedBuf::new(self.buf_size)
            .ok_or_else(|| io::Error::other("failed to allocate aligned buf"))
    }

    /// Return a buffer to the pool (dropped if pool is full or lock poisoned).
    pub fn put(&self, buf: AlignedBuf) {
        if let Ok(mut pool) = self.bufs.lock() {
            if pool.len() < self.max_pooled {
                pool.push(buf);
            }
        }
        // else: buf is dropped, memory freed
    }
}

// ---------------------------------------------------------------------------
// File open helpers
// ---------------------------------------------------------------------------

/// Open a file for reading with `O_DIRECT`.
pub fn open_direct_read(path: &Path) -> io::Result<File> {
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
}

/// Open (create/truncate) a file for writing with `O_DIRECT`.
pub fn open_direct_write(path: &Path) -> io::Result<File> {
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
}

/// Probe whether the filesystem at `root` supports `O_DIRECT`.
pub fn probe_direct_io(root: &Path) -> bool {
    let probe = root.join(".directio_probe");
    let ok = open_direct_write(&probe).is_ok();
    let _ = std::fs::remove_file(&probe);
    ok
}

// ---------------------------------------------------------------------------
// Round-up helper
// ---------------------------------------------------------------------------

/// Round `n` up to the next multiple of [`ALIGN`].
fn round_up(n: usize) -> usize {
    (n + ALIGN - 1) & !(ALIGN - 1)
}

// ---------------------------------------------------------------------------
// DirectReader
// ---------------------------------------------------------------------------

enum ReaderState {
    /// Buffer data available at `buf[buf_pos..buf_len]`.
    Buffered,
    /// A blocking read is in flight.
    Reading(JoinHandle<io::Result<(AlignedBuf, usize)>>),
    /// All data has been served.
    Done,
}

/// An [`AsyncRead`] that reads from an `O_DIRECT` file descriptor using
/// aligned buffers and `pread`, bridged to async via `spawn_blocking`.
pub struct DirectReader {
    file: Arc<File>,
    file_size: u64,
    file_offset: u64,
    buf: AlignedBuf,
    buf_pos: usize,
    buf_len: usize,
    pool: Arc<AlignedBufPool>,
    io_size: usize,
    state: ReaderState,
}

impl DirectReader {
    pub fn new(
        file: File,
        file_size: u64,
        pool: Arc<AlignedBufPool>,
        io_size: usize,
        buf: AlignedBuf,
    ) -> Self {
        Self {
            file: Arc::new(file),
            file_size,
            file_offset: 0,
            buf,
            buf_pos: 0,
            buf_len: 0,
            pool,
            io_size,
            state: if file_size == 0 {
                ReaderState::Done
            } else {
                ReaderState::Buffered // will immediately trigger a read
            },
        }
    }
}

impl AsyncRead for DirectReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        dst: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();

        loop {
            match &mut me.state {
                ReaderState::Done => return Poll::Ready(Ok(())),

                ReaderState::Buffered if me.buf_pos < me.buf_len => {
                    let avail = me
                        .buf
                        .get(me.buf_pos..me.buf_len)
                        .ok_or_else(|| io::Error::other("buf slice OOB"))?;
                    let n = avail.len().min(dst.remaining());
                    let slice = avail
                        .get(..n)
                        .ok_or_else(|| io::Error::other("buf slice OOB"))?;
                    dst.put_slice(slice);
                    me.buf_pos += n;
                    return Poll::Ready(Ok(()));
                }

                ReaderState::Buffered => {
                    // Buffer exhausted — need more data.
                    if me.file_offset >= me.file_size {
                        me.state = ReaderState::Done;
                        return Poll::Ready(Ok(()));
                    }
                    let remaining = (me.file_size - me.file_offset) as usize;
                    // Read size: up to io_size, but rounded up to ALIGN for O_DIRECT.
                    let read_size = round_up(remaining.min(me.io_size));

                    let file = Arc::clone(&me.file);
                    let mut buf = me.pool.get()?;
                    let offset = me.file_offset;

                    let handle = tokio::task::spawn_blocking(move || {
                        let dest = buf
                            .get_mut(..read_size)
                            .ok_or_else(|| io::Error::other("buf too small"))?;
                        let n = file.read_at(dest, offset)?;
                        Ok((buf, n))
                    });
                    me.state = ReaderState::Reading(handle);
                    // fall through to poll the handle
                }

                ReaderState::Reading(handle) => {
                    let result = std::task::ready!(Pin::new(handle).poll(cx));
                    match result {
                        Ok(Ok((buf, n))) => {
                            // Return old buffer to pool, install new one.
                            let old = std::mem::replace(&mut me.buf, buf);
                            me.pool.put(old);
                            let valid = n.min((me.file_size - me.file_offset) as usize);
                            me.buf_pos = 0;
                            me.buf_len = valid;
                            me.file_offset += valid as u64;
                            me.state = ReaderState::Buffered;
                            // loop back to serve from buffer
                        }
                        Ok(Err(e)) => {
                            me.state = ReaderState::Done;
                            return Poll::Ready(Err(e));
                        }
                        Err(join_err) => {
                            me.state = ReaderState::Done;
                            return Poll::Ready(Err(io::Error::other(join_err)));
                        }
                    }
                }
            }
        }
    }
}

// SAFETY: All fields are Send.  The JoinHandle and Arc<File> are Send.
unsafe impl Send for DirectReader {}
impl Unpin for DirectReader {}

// ---------------------------------------------------------------------------
// DirectWriter
// ---------------------------------------------------------------------------

/// A [`BlobWriter`] that writes to an `O_DIRECT` file, accumulating data in
/// an aligned buffer and flushing full chunks with `pwrite`.
pub struct DirectWriter {
    file: File,
    path: PathBuf,
    pool: Arc<AlignedBufPool>,
    buf: AlignedBuf,
    buf_len: usize,
    io_size: usize,
    file_offset: u64,
    total_size: u64,
    sync: bool,
}

impl DirectWriter {
    pub fn new(
        file: File,
        path: PathBuf,
        pool: Arc<AlignedBufPool>,
        io_size: usize,
        sync: bool,
        buf: AlignedBuf,
    ) -> Self {
        Self {
            file,
            path,
            pool,
            buf,
            buf_len: 0,
            io_size,
            file_offset: 0,
            total_size: 0,
            sync,
        }
    }

    /// Flush the full buffer (exactly `io_size` bytes) to disk.
    async fn flush_full(&mut self) -> error::Result<()> {
        debug_assert!(self.buf_len == self.io_size);
        let file = self.file.try_clone()?;
        let buf = std::mem::replace(&mut self.buf, self.pool.get()?);
        let offset = self.file_offset;
        let io_size = self.io_size;

        tokio::task::spawn_blocking(move || {
            let src = buf
                .get(..io_size)
                .ok_or_else(|| io::Error::other("flush buf slice OOB"))?;
            file.write_all_at(src, offset)?;
            Ok::<_, io::Error>(())
        })
        .await
        .map_err(|e| error::DepotError::Internal(e.to_string()))??;

        self.file_offset += self.io_size as u64;
        self.buf_len = 0;
        Ok(())
    }
}

#[async_trait::async_trait]
impl BlobWriter for DirectWriter {
    async fn write_chunk(&mut self, data: &[u8]) -> error::Result<()> {
        self.total_size += data.len() as u64;
        let mut pos = 0;

        while pos < data.len() {
            let space = self.io_size - self.buf_len;
            let n = (data.len() - pos).min(space);
            let src = data
                .get(pos..pos + n)
                .ok_or_else(|| error::DepotError::Internal("write_chunk slice OOB".to_string()))?;
            let dst = self
                .buf
                .get_mut(self.buf_len..self.buf_len + n)
                .ok_or_else(|| {
                    error::DepotError::Internal("write_chunk buf slice OOB".to_string())
                })?;
            dst.copy_from_slice(src);
            self.buf_len += n;
            pos += n;

            if self.buf_len == self.io_size {
                self.flush_full().await?;
            }
        }
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> error::Result<u64> {
        let total = self.total_size;

        if self.buf_len > 0 {
            let write_size = round_up(self.buf_len);
            // Zero only the pad region between data and the aligned write boundary.
            if let Some(pad) = self.buf.get_mut(self.buf_len..write_size) {
                pad.fill(0);
            }
            let file = self.file.try_clone()?;
            let buf = std::mem::replace(&mut self.buf, self.pool.get()?);
            let offset = self.file_offset;

            tokio::task::spawn_blocking(move || {
                let src = buf
                    .get(..write_size)
                    .ok_or_else(|| io::Error::other("finish buf slice OOB"))?;
                file.write_all_at(src, offset)?;
                Ok::<_, io::Error>(())
            })
            .await
            .map_err(|e| error::DepotError::Internal(e.to_string()))??;
        }

        // Truncate to exact size (removes zero-padding from last block).
        self.file.set_len(total)?;

        if self.sync {
            let file = self.file.try_clone()?;
            tokio::task::spawn_blocking(move || {
                file.sync_all()?;
                Ok::<_, io::Error>(())
            })
            .await
            .map_err(|e| error::DepotError::Internal(e.to_string()))??;
        }

        // Return buffer to pool.
        let buf = std::mem::replace(&mut self.buf, self.pool.get()?);
        self.pool.put(buf);

        Ok(total)
    }

    async fn abort(self: Box<Self>) -> error::Result<()> {
        // Return buffer to pool.
        self.pool.put(self.buf);
        let _ = std::fs::remove_file(&self.path);
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn aligned_buf_basics() {
        let buf = AlignedBuf::new(4096).unwrap();
        assert_eq!(buf.len(), 4096);
        assert_eq!(buf.as_ptr() as usize % ALIGN, 0);
    }

    #[test]
    fn aligned_buf_large() {
        let buf = AlignedBuf::new(1048576).unwrap();
        assert_eq!(buf.len(), 1048576);
        assert_eq!(buf.as_ptr() as usize % ALIGN, 0);
    }

    #[test]
    fn aligned_buf_zero_returns_none() {
        assert!(AlignedBuf::new(0).is_none());
    }

    #[test]
    fn pool_get_put() {
        let pool = AlignedBufPool::new(4096, 4);
        let b1 = pool.get().unwrap();
        let b2 = pool.get().unwrap();
        pool.put(b1);
        pool.put(b2);
        // Should recycle.
        let _b3 = pool.get().unwrap();
        let _b4 = pool.get().unwrap();
    }

    #[test]
    fn pool_bounded() {
        let pool = AlignedBufPool::new(4096, 2);
        let b1 = pool.get().unwrap();
        let b2 = pool.get().unwrap();
        let b3 = pool.get().unwrap();
        pool.put(b1);
        pool.put(b2);
        pool.put(b3); // exceeds max — should be dropped, not panic
    }

    #[test]
    fn round_up_works() {
        assert_eq!(round_up(0), 0);
        assert_eq!(round_up(1), 4096);
        assert_eq!(round_up(4096), 4096);
        assert_eq!(round_up(4097), 8192);
    }
}
