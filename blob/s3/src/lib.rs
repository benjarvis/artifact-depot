// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! S3-backed blob store implementation.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::missing_panics_doc
    )
)]

use std::path::PathBuf;

use bytes::Bytes;
use futures::stream::StreamExt;
use http_body::Frame;
use http_body_util::StreamBody;
use tokio::task::JoinHandle;
use tracing::warn;

use depot_core::error::{self, DepotError, Retryability};
use depot_core::store::blob::{BlobReader, BlobScanStream, BlobStore, BlobWriter, BrowseEntry};

/// Classify an AWS SDK error for retryability.
fn s3_sdk_retryability<E>(e: &aws_sdk_s3::error::SdkError<E>) -> Retryability {
    use aws_sdk_s3::error::SdkError;
    match e {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) => Retryability::Transient,
        SdkError::ServiceError(se) => {
            let status = se.raw().status().as_u16();
            if matches!(status, 429 | 500 | 502 | 503) {
                Retryability::Transient
            } else {
                Retryability::Permanent
            }
        }
        _ => Retryability::Permanent,
    }
}

/// Convert an AWS SDK error into a `DepotError` with appropriate retryability.
fn convert_s3_err<E>(e: aws_sdk_s3::error::SdkError<E>) -> DepotError
where
    E: std::error::Error + Send + Sync + 'static,
{
    let r = s3_sdk_retryability(&e);
    DepotError::Storage(Box::new(e), r)
}

pub struct S3BlobStore {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}

impl S3BlobStore {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        bucket: String,
        endpoint: Option<String>,
        region: String,
        prefix: Option<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
        max_retries: u32,
        connect_timeout_secs: u64,
        read_timeout_secs: u64,
        retry_mode: &str,
    ) -> error::Result<Self> {
        let region_provider = aws_sdk_s3::config::Region::new(region);

        let mut timeout_builder = aws_config::timeout::TimeoutConfig::builder()
            .connect_timeout(std::time::Duration::from_secs(connect_timeout_secs));
        // Only set the read timeout if explicitly configured (non-zero).
        // S3 operations range from tiny metadata GETs to multi-GB streaming
        // uploads, so a fixed read timeout that works for both is impractical.
        // The default of 0 means "no read timeout" (SDK default).
        if read_timeout_secs > 0 {
            timeout_builder =
                timeout_builder.read_timeout(std::time::Duration::from_secs(read_timeout_secs));
        }
        let timeout_config = timeout_builder.build();

        let retry_config = aws_config::retry::RetryConfig::standard()
            .with_max_attempts(max_retries + 1)
            .with_retry_mode(match retry_mode {
                "adaptive" => aws_config::retry::RetryMode::Adaptive,
                _ => aws_config::retry::RetryMode::Standard,
            });

        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region_provider)
            .timeout_config(timeout_config)
            .retry_config(retry_config);

        if let (Some(ak), Some(sk)) = (access_key, secret_key) {
            let creds = aws_credential_types::Credentials::new(ak, sk, None, None, "static");
            config_loader = config_loader.credentials_provider(creds);
        }

        let sdk_config = config_loader.load().await;

        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config)
            // Use WhenRequired to avoid buffering the entire body for
            // checksum computation on streaming PutObject calls.
            .request_checksum_calculation(
                aws_sdk_s3::config::RequestChecksumCalculation::WhenRequired,
            );

        if let Some(ep) = endpoint {
            // Path-style is required for MinIO and most S3-compatible
            // stores, but must not be used with AWS S3 (virtual-hosted
            // style is mandatory in newer regions).
            s3_config_builder = s3_config_builder.endpoint_url(ep).force_path_style(true);
        }

        let client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

        let prefix = match prefix {
            Some(p) => {
                let trimmed = p.trim_matches('/');
                if trimmed.is_empty() {
                    String::new()
                } else {
                    format!("{trimmed}/")
                }
            }
            None => String::new(),
        };

        let store = Self {
            client,
            bucket,
            prefix,
        };
        // Best-effort, non-blocking: spawn lifecycle setup so it doesn't delay
        // store creation or hang if the S3-compatible store lacks lifecycle
        // support (e.g. MinIO, s3s in tests).
        {
            let client = store.client.clone();
            let bucket = store.bucket.clone();
            tokio::spawn(async move {
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    Self::ensure_multipart_lifecycle_rule_inner(&client, &bucket),
                )
                .await;
            });
        }
        Ok(store)
    }

    fn s3_key(&self, blob_id: &str) -> String {
        const DIR_DEPTH: usize = 2;
        let mut key = format!("{}blobs/", self.prefix);
        for i in 0..DIR_DEPTH {
            let start = i * 2;
            if let Some(prefix) = blob_id.get(start..start + 2) {
                key.push_str(prefix);
                key.push('/');
            }
        }
        key.push_str(blob_id);
        key
    }

    /// Best-effort: ensure the bucket has a lifecycle rule that auto-aborts
    /// incomplete multipart uploads after 1 day.  This prevents leaked uploads
    /// when the process crashes before `abort()` or `finish()` runs.
    async fn ensure_multipart_lifecycle_rule_inner(client: &aws_sdk_s3::Client, bucket: &str) {
        use aws_sdk_s3::types::{
            AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, ExpirationStatus,
            LifecycleRule, LifecycleRuleFilter,
        };

        const RULE_ID: &str = "depot-abort-incomplete-multipart";

        // Read existing rules (may be empty).
        let existing = match client
            .get_bucket_lifecycle_configuration()
            .bucket(bucket)
            .send()
            .await
        {
            Ok(resp) => resp.rules.unwrap_or_default(),
            // NoSuchLifecycleConfiguration is expected when the bucket has none.
            Err(_) => Vec::new(),
        };

        // If there is already any rule with AbortIncompleteMultipartUpload, skip.
        let dominated = existing
            .iter()
            .any(|r| r.abort_incomplete_multipart_upload().is_some());
        if dominated {
            return;
        }

        let rule = match LifecycleRule::builder()
            .id(RULE_ID)
            .status(ExpirationStatus::Enabled)
            .filter(LifecycleRuleFilter::builder().prefix(String::new()).build())
            .abort_incomplete_multipart_upload(
                AbortIncompleteMultipartUpload::builder()
                    .days_after_initiation(1)
                    .build(),
            )
            .build()
        {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "failed to build lifecycle rule");
                return;
            }
        };

        let mut rules = existing;
        rules.push(rule);

        let config = match BucketLifecycleConfiguration::builder()
            .set_rules(Some(rules))
            .build()
        {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "failed to build lifecycle configuration");
                return;
            }
        };

        if let Err(e) = client
            .put_bucket_lifecycle_configuration()
            .bucket(bucket)
            .lifecycle_configuration(config)
            .send()
            .await
        {
            warn!(
                bucket = %bucket,
                error = %e,
                "failed to set AbortIncompleteMultipartUpload lifecycle rule; \
                 orphaned multipart uploads may accumulate if the process crashes"
            );
        }
    }
}

#[async_trait::async_trait]
impl BlobStore for S3BlobStore {
    async fn put(&self, blob_id: &str, data: &[u8]) -> error::Result<u64> {
        let key = self.s3_key(blob_id);
        let len = data.len() as u64;
        let body = aws_sdk_s3::primitives::ByteStream::from(data.to_vec());

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body)
            .send()
            .await
            .map_err(convert_s3_err)?;

        Ok(len)
    }

    async fn get(&self, blob_id: &str) -> error::Result<Option<Vec<u8>>> {
        let key = self.s3_key(blob_id);

        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await;

        match result {
            Ok(output) => {
                let bytes = output
                    .body
                    .collect()
                    .await
                    .map_err(|e| DepotError::Storage(Box::new(e), Retryability::Permanent))?;
                Ok(Some(bytes.to_vec()))
            }
            Err(e) => {
                if is_not_found(&e) {
                    Ok(None)
                } else {
                    Err(convert_s3_err(e))
                }
            }
        }
    }

    async fn open_read(&self, blob_id: &str) -> error::Result<Option<(BlobReader, u64)>> {
        let key = self.s3_key(blob_id);

        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await;

        match result {
            Ok(output) => {
                let content_length = output.content_length.unwrap_or(0) as u64;
                let reader = output.body.into_async_read();
                Ok(Some((Box::new(reader), content_length)))
            }
            Err(e) => {
                if is_not_found(&e) {
                    Ok(None)
                } else {
                    Err(convert_s3_err(e))
                }
            }
        }
    }

    async fn exists(&self, blob_id: &str) -> error::Result<bool> {
        let key = self.s3_key(blob_id);

        let result = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(e) => {
                if is_not_found_head(&e) {
                    Ok(false)
                } else {
                    Err(convert_s3_err(e))
                }
            }
        }
    }

    async fn delete(&self, blob_id: &str) -> error::Result<()> {
        let key = self.s3_key(blob_id);
        match self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            // S3-compatible stores may return NoSuchKey; real S3 returns 204
            // regardless. Treat both as success since delete is idempotent.
            Err(e) if is_not_found_delete(&e) => Ok(()),
            Err(e) => Err(convert_s3_err(e)),
        }
    }

    fn blob_path(&self, _blob_id: &str) -> Option<PathBuf> {
        None
    }

    async fn create_writer(
        &self,
        blob_id: &str,
        content_length_hint: Option<u64>,
    ) -> error::Result<Box<dyn BlobWriter>> {
        let key = self.s3_key(blob_id);

        // Use single PutObject when we know the size and it fits within the S3
        // single-object limit (5 GB).  Otherwise fall back to multipart.
        let strategy = if let Some(len) = content_length_hint {
            if len <= S3_SINGLE_PUT_MAX {
                let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, std::io::Error>>(
                    CHANNEL_CAPACITY,
                );

                // Build a streaming body from the channel receiver.  The bounded
                // channel provides natural backpressure: when S3 upload falls
                // behind, the sender (write_chunk) blocks, which in turn stops
                // the HTTP socket read.
                let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
                let sized_body = SizedBody {
                    inner: StreamBody::new(stream),
                    remaining: len,
                };
                let body = aws_sdk_s3::primitives::ByteStream::from_body_1_x(sized_body);

                let client = self.client.clone();
                let bucket = self.bucket.clone();
                let k = key.clone();
                let put_task = tokio::spawn(async move {
                    client
                        .put_object()
                        .bucket(&bucket)
                        .key(&k)
                        .content_length(len as i64)
                        .body(body)
                        .send()
                        .await
                        .map(|_| ())
                        .map_err(|e| DepotError::Storage(Box::new(e), Retryability::Transient))
                });

                S3WriteStrategy::SinglePut {
                    sender: Some(tx),
                    put_task: Some(put_task),
                }
            } else {
                S3WriteStrategy::new_multipart()
            }
        } else {
            S3WriteStrategy::new_multipart()
        };

        Ok(Box::new(S3BlobWriter {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            key,
            strategy,
            total_size: 0,
            consumed: false,
        }))
    }

    async fn scan_all_blobs(&self) -> error::Result<BlobScanStream<'_>> {
        // Linear ListObjectsV2 walk under `{prefix}blobs/`. Callers that need
        // parallelism should use `scan_blobs_sharded` to fan out across
        // the 256 hex-byte top-level prefixes instead.
        let blob_prefix = if self.prefix.is_empty() {
            "blobs/".to_string()
        } else {
            format!("{}blobs/", self.prefix)
        };
        Ok(self.list_prefix_stream(blob_prefix))
    }

    fn scan_shard_count(&self) -> u16 {
        // Blob keys are `{prefix}blobs/XX/YY/<blob_id>`; the top-level hex
        // byte shards cleanly into 256 disjoint S3 list prefixes.
        256
    }

    async fn scan_blobs_sharded(&self, shard: u16) -> error::Result<BlobScanStream<'_>> {
        debug_assert!(
            shard < 256,
            "S3BlobStore shard {shard} out of range (0..256)"
        );
        let blob_prefix = if self.prefix.is_empty() {
            format!("blobs/{shard:02x}/")
        } else {
            format!("{}blobs/{shard:02x}/", self.prefix)
        };
        Ok(self.list_prefix_stream(blob_prefix))
    }

    async fn health_check(&self) -> error::Result<()> {
        let check_id = format!("_depot_health_{}", uuid::Uuid::new_v4().simple());
        let test_data = b"depot-health-check";

        self.put(&check_id, test_data)
            .await
            .map_err(|e| DepotError::Internal(format!("health_check put: {e}")))?;

        let inner = self.health_check_inner(&check_id, test_data).await;
        if inner.is_err() {
            let _ = self.delete(&check_id).await;
        }
        inner
    }

    async fn browse_prefix(&self, prefix: &str) -> error::Result<Vec<BrowseEntry>> {
        let normalized = prefix.trim_start_matches('/');
        let full_prefix = if self.prefix.is_empty() {
            format!("blobs/{normalized}")
        } else {
            format!("{}blobs/{normalized}", self.prefix)
        };

        let resp = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&full_prefix)
            .delimiter("/")
            .send()
            .await
            .map_err(convert_s3_err)?;

        let mut result = Vec::new();

        // Common prefixes -> subdirectories
        for cp in resp.common_prefixes() {
            if let Some(pfx) = cp.prefix() {
                let name = pfx
                    .strip_prefix(&full_prefix)
                    .unwrap_or(pfx)
                    .trim_end_matches('/');
                if !name.is_empty() {
                    result.push(BrowseEntry {
                        name: name.to_string(),
                        is_dir: true,
                        size: None,
                    });
                }
            }
        }

        // Objects -> files (blobs)
        for obj in resp.contents() {
            if let Some(key) = obj.key() {
                let name = key.strip_prefix(&full_prefix).unwrap_or(key);
                if !name.is_empty() && !name.contains('/') {
                    result.push(BrowseEntry {
                        name: name.to_string(),
                        is_dir: false,
                        size: Some(obj.size.unwrap_or(0) as u64),
                    });
                }
            }
        }

        result.sort_by(|a, b| b.is_dir.cmp(&a.is_dir).then_with(|| a.name.cmp(&b.name)));
        Ok(result)
    }
}

impl S3BlobStore {
    /// Stream every object under `blob_prefix` via paginated ListObjectsV2.
    /// Emits `(blob_id, size)` pairs; ignores entries whose key lacks a
    /// trailing `/`-separated blob id.
    fn list_prefix_stream<'a>(&self, blob_prefix: String) -> BlobScanStream<'a> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();

        let (tx, rx) = tokio::sync::mpsc::channel::<error::Result<(String, u64)>>(1024);

        tokio::spawn(async move {
            let mut continuation_token: Option<String> = None;
            loop {
                let mut req = client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&blob_prefix);
                if let Some(token) = &continuation_token {
                    req = req.continuation_token(token);
                }
                let resp = match req.send().await {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!(
                            bucket = %bucket,
                            prefix = %blob_prefix,
                            error = %e,
                            "S3 ListObjectsV2 failed during scan"
                        );
                        return;
                    }
                };

                for obj in resp.contents() {
                    let Some(key) = obj.key() else { continue };
                    let blob_id = key.rsplit('/').next().unwrap_or("");
                    if blob_id.is_empty() {
                        continue;
                    }
                    let size = obj.size.unwrap_or(0) as u64;
                    if tx.send(Ok((blob_id.to_string(), size))).await.is_err() {
                        return;
                    }
                }

                match resp.next_continuation_token() {
                    Some(token) => continuation_token = Some(token.to_string()),
                    None => break,
                }
            }
        });

        tokio_stream::wrappers::ReceiverStream::new(rx).boxed()
    }

    async fn health_check_inner(&self, check_id: &str, test_data: &[u8]) -> error::Result<()> {
        match self.get(check_id).await {
            Ok(Some(data)) if data == test_data => {}
            Ok(Some(_)) => {
                return Err(DepotError::Internal(
                    "health_check get: returned unexpected data".into(),
                ))
            }
            Ok(None) => {
                return Err(DepotError::Internal(
                    "health_check get: blob not found after successful put".into(),
                ))
            }
            Err(e) => return Err(DepotError::Internal(format!("health_check get: {e}"))),
        }

        let (d1, d2) = match (check_id.get(..2), check_id.get(2..4)) {
            (Some(a), Some(b)) => (a, b),
            _ => {
                return Err(DepotError::Internal(
                    "health_check list: blob id too short".into(),
                ))
            }
        };
        let dir_path = format!("{d1}/{d2}/");
        let entries = self
            .browse_prefix(&dir_path)
            .await
            .map_err(|e| DepotError::Internal(format!("health_check list: {e}")))?;
        if !entries.iter().any(|e| e.name == check_id) {
            return Err(DepotError::Internal(format!(
                "health_check list: blob not found in directory listing ({} entries)",
                entries.len()
            )));
        }

        self.delete(check_id)
            .await
            .map_err(|e| DepotError::Internal(format!("health_check delete: {e}")))?;

        match self.exists(check_id).await {
            Ok(false) => Ok(()),
            Ok(true) => Err(DepotError::Internal(
                "health_check delete verify: blob still exists".into(),
            )),
            Err(e) => Err(DepotError::Internal(format!(
                "health_check delete verify: {e}"
            ))),
        }
    }
}

/// A thin wrapper around `StreamBody` that reports a known content length in
/// its `size_hint()`.  The AWS SDK uses this hint to set `Content-Length` and
/// to satisfy checksum interceptor requirements for sized request bodies.
struct SizedBody<S> {
    inner: StreamBody<S>,
    remaining: u64,
}

impl<S, E> http_body::Body for SizedBody<S>
where
    S: futures::Stream<Item = Result<Frame<Bytes>, E>> + Send + Unpin,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
{
    type Data = Bytes;
    type Error = E;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        let poll = std::pin::Pin::new(&mut this.inner).poll_frame(cx);
        if let std::task::Poll::Ready(Some(Ok(ref frame))) = poll {
            if let Some(data) = frame.data_ref() {
                this.remaining = this.remaining.saturating_sub(data.len() as u64);
            }
        }
        poll
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.remaining)
    }
}

/// Maximum object size for a single PutObject (5 GB).
const S3_SINGLE_PUT_MAX: u64 = 5 * 1024 * 1024 * 1024;

/// Minimum part size for S3 multipart upload (5 MB).
const S3_MIN_PART_SIZE: usize = 5 * 1024 * 1024;

/// Bounded channel capacity for the single-put streaming path.
/// Each slot holds one `write_chunk` worth of data; a small capacity ensures
/// backpressure propagates to the HTTP socket reader quickly.
const CHANNEL_CAPACITY: usize = 4;

enum S3WriteStrategy {
    /// Stream directly into a single PutObject via a channel-backed body.
    SinglePut {
        sender: Option<tokio::sync::mpsc::Sender<Result<Frame<Bytes>, std::io::Error>>>,
        put_task: Option<JoinHandle<error::Result<()>>>,
    },
    /// Classic multipart upload for unknown-size or >5 GB blobs.
    Multipart {
        upload_id: Option<String>,
        parts: Vec<aws_sdk_s3::types::CompletedPart>,
        buffer: Vec<u8>,
        part_number: i32,
    },
}

impl S3WriteStrategy {
    fn new_multipart() -> Self {
        Self::Multipart {
            upload_id: None,
            parts: Vec::new(),
            buffer: Vec::new(),
            part_number: 1,
        }
    }
}

/// S3 streaming blob writer.
///
/// When the total size is known and ≤ 5 GB the writer streams data through a
/// channel directly into a single `PutObject` call.  Otherwise it falls back
/// to the classic multipart upload path (buffer → 5 MB parts → complete).
struct S3BlobWriter {
    client: aws_sdk_s3::Client,
    bucket: String,
    key: String,
    strategy: S3WriteStrategy,
    total_size: u64,
    /// Set to `true` after `finish()` or `abort()` so `Drop` is a no-op.
    consumed: bool,
}

#[async_trait::async_trait]
impl BlobWriter for S3BlobWriter {
    async fn write_chunk(&mut self, data: &[u8]) -> error::Result<()> {
        self.total_size += data.len() as u64;

        match &mut self.strategy {
            S3WriteStrategy::SinglePut { sender, put_task } => {
                let tx = match sender.as_ref() {
                    Some(tx) => tx,
                    None => {
                        return Err(DepotError::Storage(
                            Box::new(std::io::Error::other(
                                "write_chunk called after finish/abort",
                            )),
                            Retryability::Permanent,
                        ));
                    }
                };
                if tx
                    .send(Ok(Frame::data(Bytes::copy_from_slice(data))))
                    .await
                    .is_err()
                {
                    // The PutObject background task has terminated.  Retrieve
                    // the real S3 error from it instead of returning a generic
                    // "terminated unexpectedly" message.
                    sender.take();
                    if let Some(task) = put_task.take() {
                        match task.await {
                            Ok(Err(e)) => return Err(e),
                            Err(e) => {
                                return Err(DepotError::Storage(
                                    Box::new(e),
                                    Retryability::Transient,
                                ));
                            }
                            Ok(Ok(())) => {}
                        }
                    }
                    return Err(DepotError::Storage(
                        Box::new(std::io::Error::other(
                            "S3 PutObject task terminated unexpectedly",
                        )),
                        Retryability::Transient,
                    ));
                }
            }
            S3WriteStrategy::Multipart { ref mut buffer, .. } => {
                buffer.extend_from_slice(data);
            }
        }
        // Flush multipart parts outside the match to avoid double-borrow.
        while matches!(&self.strategy, S3WriteStrategy::Multipart { buffer, .. } if buffer.len() >= S3_MIN_PART_SIZE)
        {
            self.flush_part().await?;
        }
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> error::Result<u64> {
        self.consumed = true;
        let total = self.total_size;

        match &mut self.strategy {
            S3WriteStrategy::SinglePut { sender, put_task } => {
                // Drop sender to signal EOF to the PutObject body stream.
                sender.take();
                let task = match put_task.take() {
                    Some(t) => t,
                    None => {
                        return Err(DepotError::Storage(
                            Box::new(std::io::Error::other("finish called twice")),
                            Retryability::Permanent,
                        ));
                    }
                };
                task.await
                    .map_err(|e| DepotError::Storage(Box::new(e), Retryability::Transient))??;
            }
            S3WriteStrategy::Multipart {
                upload_id, buffer, ..
            } => {
                if upload_id.is_none() {
                    // Small blob — never started multipart. Single PutObject.
                    let body = aws_sdk_s3::primitives::ByteStream::from(std::mem::take(buffer));
                    self.client
                        .put_object()
                        .bucket(&self.bucket)
                        .key(&self.key)
                        .body(body)
                        .send()
                        .await
                        .map_err(convert_s3_err)?;
                } else {
                    // Flush remaining buffer as final part, then complete.
                    if !buffer.is_empty() {
                        self.flush_part().await?;
                    }
                    if let S3WriteStrategy::Multipart {
                        upload_id, parts, ..
                    } = &self.strategy
                    {
                        let uid = upload_id.as_ref().ok_or_else(|| {
                            DepotError::Storage(
                                Box::new(std::io::Error::other("missing S3 upload_id")),
                                Retryability::Permanent,
                            )
                        })?;
                        let completed = aws_sdk_s3::types::CompletedMultipartUpload::builder()
                            .set_parts(Some(parts.clone()))
                            .build();
                        self.client
                            .complete_multipart_upload()
                            .bucket(&self.bucket)
                            .key(&self.key)
                            .upload_id(uid)
                            .multipart_upload(completed)
                            .send()
                            .await
                            .map_err(convert_s3_err)?;
                    }
                }
            }
        }
        Ok(total)
    }

    async fn abort(mut self: Box<Self>) -> error::Result<()> {
        self.consumed = true;

        match &mut self.strategy {
            S3WriteStrategy::SinglePut { sender, put_task } => {
                // Drop sender so the PutObject fails with an incomplete body.
                sender.take();
                if let Some(task) = put_task.take() {
                    let _ = task.await;
                }
            }
            S3WriteStrategy::Multipart { upload_id, .. } => {
                if let Some(ref uid) = upload_id {
                    let _ = self
                        .client
                        .abort_multipart_upload()
                        .bucket(&self.bucket)
                        .key(&self.key)
                        .upload_id(uid)
                        .send()
                        .await;
                }
            }
        }
        Ok(())
    }
}

impl Drop for S3BlobWriter {
    fn drop(&mut self) {
        if self.consumed {
            return;
        }
        match &mut self.strategy {
            S3WriteStrategy::SinglePut { sender, .. } => {
                // Drop sender — the background PutObject will fail on its own.
                sender.take();
            }
            S3WriteStrategy::Multipart { upload_id, .. } => {
                if let Some(uid) = upload_id.take() {
                    let client = self.client.clone();
                    let bucket = self.bucket.clone();
                    let key = self.key.clone();
                    // Best-effort background abort.
                    tokio::spawn(async move {
                        let _ = client
                            .abort_multipart_upload()
                            .bucket(&bucket)
                            .key(&key)
                            .upload_id(&uid)
                            .send()
                            .await;
                    });
                }
            }
        }
    }
}

impl S3BlobWriter {
    async fn flush_part(&mut self) -> error::Result<()> {
        let (upload_id, buffer, parts, part_number) = match &mut self.strategy {
            S3WriteStrategy::Multipart {
                upload_id,
                buffer,
                parts,
                part_number,
            } => (upload_id, buffer, parts, part_number),
            _ => {
                return Err(DepotError::Storage(
                    Box::new(std::io::Error::other(
                        "flush_part called on SinglePut strategy",
                    )),
                    Retryability::Permanent,
                ));
            }
        };

        // Start multipart upload on first part flush.
        if upload_id.is_none() {
            let resp = self
                .client
                .create_multipart_upload()
                .bucket(&self.bucket)
                .key(&self.key)
                .send()
                .await
                .map_err(convert_s3_err)?;
            *upload_id = resp.upload_id;
        }

        let part_data = std::mem::take(buffer);
        let body = aws_sdk_s3::primitives::ByteStream::from(part_data);
        let uid = upload_id.as_ref().ok_or_else(|| {
            DepotError::Storage(
                Box::new(std::io::Error::other("missing S3 upload_id")),
                Retryability::Permanent,
            )
        })?;

        let resp = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(uid)
            .part_number(*part_number)
            .body(body)
            .send()
            .await
            .map_err(convert_s3_err)?;

        parts.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(*part_number)
                .set_e_tag(resp.e_tag)
                .build(),
        );
        *part_number += 1;
        Ok(())
    }
}

/// Check if an S3 `GetObject` error is a "not found" error.
fn is_not_found(
    err: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
) -> bool {
    matches!(err, aws_sdk_s3::error::SdkError::ServiceError(e) if e.err().is_no_such_key())
}

/// Check if an S3 `DeleteObject` error is a "not found" error.
fn is_not_found_delete(
    err: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::delete_object::DeleteObjectError>,
) -> bool {
    matches!(err, aws_sdk_s3::error::SdkError::ServiceError(e)
        if e.raw().status().as_u16() == 404)
}

/// Check if an S3 HeadObject error is a "not found" error.
fn is_not_found_head(
    err: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::head_object::HeadObjectError>,
) -> bool {
    matches!(err, aws_sdk_s3::error::SdkError::ServiceError(e) if e.err().is_not_found())
}

#[cfg(test)]
mod tests {
    use super::*;

    use hyper_util::rt::TokioIo;
    use s3s::service::S3ServiceBuilder;
    use tokio::net::TcpListener;

    const BUCKET: &str = "test-bucket";

    /// Start an in-process S3 server backed by s3s-fs and return an `S3BlobStore`
    /// pointing at it. Callers must hold the returned `TempDir`s alive for the
    /// duration of the test.
    async fn make_store() -> (S3BlobStore, tempfile::TempDir) {
        // s3s-fs root — each bucket is a subdirectory.
        let s3_root = tempfile::tempdir().expect("create s3 root");
        std::fs::create_dir_all(s3_root.path().join(BUCKET)).expect("create bucket dir");

        let fs = s3s_fs::FileSystem::new(s3_root.path()).expect("s3s_fs::FileSystem::new");
        let service = {
            let mut b = S3ServiceBuilder::new(fs);
            b.set_auth(s3s::auth::SimpleAuth::from_single("test", "test"));
            b.build()
        };

        // Bind to a random port.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        // Spawn hyper server in background.
        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(c) => c,
                    Err(_) => break,
                };
                let svc = service.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection(io, svc)
                    .await;
                });
            }
        });

        let endpoint = format!("http://127.0.0.1:{}", addr.port());

        let store = S3BlobStore::new(
            BUCKET.to_string(),
            Some(endpoint),
            "us-east-1".to_string(),
            None,
            Some("test".to_string()),
            Some("test".to_string()),
            3,
            3,
            30,
            "standard",
        )
        .await
        .expect("S3BlobStore::new");

        (store, s3_root)
    }

    fn test_blob_id() -> String {
        "a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_put_get_roundtrip() {
        let (store, _s3) = make_store().await;
        let hash = test_blob_id();
        let written = store.put(&hash, b"test data").await.expect("put");
        assert_eq!(written, 9);
        let data = store.get(&hash).await.expect("get").expect("some");
        assert_eq!(data, b"test data");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_get_missing_returns_none() {
        let (store, _s3) = make_store().await;
        let result = store.get(&test_blob_id()).await.expect("get");
        assert!(result.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_open_read_success() {
        let (store, _s3) = make_store().await;
        let hash = test_blob_id();
        store.put(&hash, b"test data").await.expect("put");
        let (mut reader, size) = store
            .open_read(&hash)
            .await
            .expect("open_read")
            .expect("some");
        assert_eq!(size, 9);
        let mut buf = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buf)
            .await
            .expect("read");
        assert_eq!(buf, b"test data");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_open_read_missing_returns_none() {
        let (store, _s3) = make_store().await;
        let result = store.open_read(&test_blob_id()).await.expect("open_read");
        assert!(result.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_exists() {
        let (store, _s3) = make_store().await;
        let hash = test_blob_id();
        assert!(!store.exists(&hash).await.expect("exists before"));
        store.put(&hash, b"test data").await.expect("put");
        assert!(store.exists(&hash).await.expect("exists after"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_delete_existing() {
        let (store, _s3) = make_store().await;
        let hash = test_blob_id();
        store.put(&hash, b"test data").await.expect("put");
        store.delete(&hash).await.expect("delete");
        assert!(!store.exists(&hash).await.expect("exists after delete"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_delete_missing() {
        let (store, _s3) = make_store().await;
        store.delete(&test_blob_id()).await.expect("delete");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_blob_path_returns_none() {
        let (store, _s3) = make_store().await;
        assert!(store.blob_path(&test_blob_id()).is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_writer_single_put_known_size() {
        let (store, _s3) = make_store().await;
        let hash = test_blob_id();
        let data = b"hello single-put";
        let mut writer = store
            .create_writer(&hash, Some(data.len() as u64))
            .await
            .expect("create_writer");
        writer.write_chunk(data).await.expect("write_chunk");
        let size = writer.finish().await.expect("finish");
        assert_eq!(size, data.len() as u64);
        let got = store.get(&hash).await.expect("get").expect("some");
        assert_eq!(got, data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_writer_multipart_unknown_size() {
        let (store, _s3) = make_store().await;
        let hash = test_blob_id();
        // Write > 5 MB to trigger multipart path.
        let chunk = vec![0xABu8; 1024 * 1024]; // 1 MB
        let mut writer = store
            .create_writer(&hash, None)
            .await
            .expect("create_writer");
        for _ in 0..6 {
            writer.write_chunk(&chunk).await.expect("write_chunk");
        }
        let size = writer.finish().await.expect("finish");
        assert_eq!(size, 6 * 1024 * 1024);
        assert!(store.exists(&hash).await.expect("exists"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_writer_single_put_larger_data() {
        let (store, _s3) = make_store().await;
        let hash = test_blob_id();
        // Write > 5 MB via single-put path (known size).
        let total = 6 * 1024 * 1024;
        let chunk = vec![0xCDu8; 1024 * 1024];
        let mut writer = store
            .create_writer(&hash, Some(total as u64))
            .await
            .expect("create_writer");
        for _ in 0..6 {
            writer.write_chunk(&chunk).await.expect("write_chunk");
        }
        let size = writer.finish().await.expect("finish");
        assert_eq!(size, total as u64);
        assert!(store.exists(&hash).await.expect("exists"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_writer_abort_single_put() {
        let (store, _s3) = make_store().await;
        let hash = test_blob_id();
        let mut writer = store
            .create_writer(&hash, Some(100))
            .await
            .expect("create_writer");
        writer.write_chunk(b"partial").await.expect("write_chunk");
        writer.abort().await.expect("abort");
        assert!(!store.exists(&hash).await.expect("exists after abort"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_writer_abort_multipart() {
        let (store, _s3) = make_store().await;
        let hash = test_blob_id();
        let mut writer = store
            .create_writer(&hash, None)
            .await
            .expect("create_writer");
        // Write enough to start a multipart upload.
        let chunk = vec![0u8; S3_MIN_PART_SIZE + 1];
        writer.write_chunk(&chunk).await.expect("write_chunk");
        writer.abort().await.expect("abort");
        assert!(!store.exists(&hash).await.expect("exists after abort"));
    }

    #[test]
    fn test_s3_key_with_prefix() {
        let store = S3BlobStore {
            client: {
                let conf = aws_sdk_s3::Config::builder()
                    .behavior_version_latest()
                    .region(aws_sdk_s3::config::Region::new("us-east-1"))
                    .build();
                aws_sdk_s3::Client::from_conf(conf)
            },
            bucket: "b".to_string(),
            prefix: "myprefix/".to_string(),
        };
        assert_eq!(
            store.s3_key("abc123def456"),
            "myprefix/blobs/ab/c1/abc123def456"
        );
    }

    #[test]
    fn test_s3_key_without_prefix() {
        let store = S3BlobStore {
            client: {
                let conf = aws_sdk_s3::Config::builder()
                    .behavior_version_latest()
                    .region(aws_sdk_s3::config::Region::new("us-east-1"))
                    .build();
                aws_sdk_s3::Client::from_conf(conf)
            },
            bucket: "b".to_string(),
            prefix: String::new(),
        };
        assert_eq!(store.s3_key("abc123def456"), "blobs/ab/c1/abc123def456");
    }

    #[test]
    fn test_s3_key_with_nested_prefix() {
        let store = S3BlobStore {
            client: {
                let conf = aws_sdk_s3::Config::builder()
                    .behavior_version_latest()
                    .region(aws_sdk_s3::config::Region::new("us-east-1"))
                    .build();
                aws_sdk_s3::Client::from_conf(conf)
            },
            bucket: "b".to_string(),
            prefix: "env/prod/".to_string(),
        };
        assert_eq!(
            store.s3_key("abc123def456"),
            "env/prod/blobs/ab/c1/abc123def456"
        );
    }

    /// Verify that the constructor normalizes a prefix that is missing
    /// a trailing slash (the most common user mistake).
    #[tokio::test]
    async fn test_prefix_normalization_no_trailing_slash() {
        let store = S3BlobStore::new(
            "b".into(),
            Some("http://localhost:1".into()),
            "us-east-1".into(),
            Some("depot".into()),
            Some("x".into()),
            Some("x".into()),
            3,
            3,
            30,
            "standard",
        )
        .await
        .unwrap();
        assert_eq!(store.prefix, "depot/");
        assert_eq!(
            store.s3_key("abc123def456"),
            "depot/blobs/ab/c1/abc123def456"
        );
    }

    /// Verify that "/" is treated as empty (no prefix).
    #[tokio::test]
    async fn test_prefix_normalization_slash_only() {
        let store = S3BlobStore::new(
            "b".into(),
            Some("http://localhost:1".into()),
            "us-east-1".into(),
            Some("/".into()),
            Some("x".into()),
            Some("x".into()),
            3,
            3,
            30,
            "standard",
        )
        .await
        .unwrap();
        assert_eq!(store.prefix, "");
        assert_eq!(store.s3_key("abc123def456"), "blobs/ab/c1/abc123def456");
    }

    /// Verify that leading slashes are stripped.
    #[tokio::test]
    async fn test_prefix_normalization_leading_slash() {
        let store = S3BlobStore::new(
            "b".into(),
            Some("http://localhost:1".into()),
            "us-east-1".into(),
            Some("/depot/".into()),
            Some("x".into()),
            Some("x".into()),
            3,
            3,
            30,
            "standard",
        )
        .await
        .unwrap();
        assert_eq!(store.prefix, "depot/");
    }

    /// Verify that None prefix results in empty string.
    #[tokio::test]
    async fn test_prefix_normalization_none() {
        let store = S3BlobStore::new(
            "b".into(),
            Some("http://localhost:1".into()),
            "us-east-1".into(),
            None,
            Some("x".into()),
            Some("x".into()),
            3,
            3,
            30,
            "standard",
        )
        .await
        .unwrap();
        assert_eq!(store.prefix, "");
        assert_eq!(store.s3_key("abc123def456"), "blobs/ab/c1/abc123def456");
    }
}
