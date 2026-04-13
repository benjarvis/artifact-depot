// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! DynamoDB-backed KV store implementation.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]

use std::borrow::Cow;

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client;

use depot_core::error::{self, DepotError, Retryability};
use depot_core::store::kv::ScanResult;

fn dynamo_sdk_retryability<E>(e: &aws_sdk_dynamodb::error::SdkError<E>) -> Retryability {
    use aws_sdk_dynamodb::error::SdkError;
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

/// DynamoDB-backed KV store.
///
/// Each logical table maps to a separate DynamoDB table named
/// `"{table_prefix}_{logical_table}"` (e.g. `"depot_artifacts"`).
///
/// Within each DynamoDB table:
///   PK (S) = partition key (or empty string for unsharded tables)
///   SK (B) = sort key bytes (UTF-8 stored as binary)
///   V  (B) = msgpack-encoded value
///
/// Each operation is a direct DynamoDB API call — no buffering, no transactions.
pub struct DynamoKvStore {
    client: Client,
    table_prefix: String,
}

impl DynamoKvStore {
    pub async fn connect(
        table_prefix: &str,
        region: &str,
        endpoint_url: Option<&str>,
        max_retries: u32,
        connect_timeout_secs: u64,
        read_timeout_secs: u64,
        retry_mode: &str,
    ) -> error::Result<Self> {
        use std::time::Duration;

        let timeout_config = aws_config::timeout::TimeoutConfig::builder()
            .connect_timeout(Duration::from_secs(connect_timeout_secs))
            .read_timeout(Duration::from_secs(read_timeout_secs))
            .build();

        let retry_config = aws_config::retry::RetryConfig::standard()
            .with_max_attempts(max_retries + 1)
            .with_retry_mode(match retry_mode {
                "adaptive" => aws_config::retry::RetryMode::Adaptive,
                _ => aws_config::retry::RetryMode::Standard,
            });

        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_string()))
            .timeout_config(timeout_config)
            .retry_config(retry_config);
        if let Some(endpoint) = endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint);
        }
        let sdk_config = config_loader.load().await;
        let client = Client::new(&sdk_config);

        let store = Self {
            client,
            table_prefix: table_prefix.to_string(),
        };

        // Pre-create all known tables.
        for name in depot_core::store::keys::ALL_TABLES {
            store
                .create_table_if_not_exists(&store.dynamo_table(name))
                .await?;
        }

        Ok(store)
    }

    /// Map a logical table name to the DynamoDB table name.
    fn dynamo_table(&self, table: &str) -> String {
        format!("{}_{}", self.table_prefix, table)
    }

    async fn create_table_if_not_exists(&self, dynamo_table: &str) -> error::Result<()> {
        use aws_sdk_dynamodb::types::{
            AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
        };

        let result = self
            .client
            .describe_table()
            .table_name(dynamo_table)
            .send()
            .await;

        match result {
            Ok(_) => return Ok(()),
            Err(e) => {
                let is_not_found = e
                    .as_service_error()
                    .map(|se| se.is_resource_not_found_exception())
                    .unwrap_or(false);
                if !is_not_found {
                    let r = dynamo_sdk_retryability(&e);
                    return Err(DepotError::Storage(Box::new(e.into_service_error()), r));
                }
            }
        }

        let map_build = |e: aws_sdk_dynamodb::error::BuildError| {
            DepotError::Storage(Box::new(e), Retryability::Permanent)
        };

        let create_result = self
            .client
            .create_table()
            .table_name(dynamo_table)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("PK")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(map_build)?,
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("SK")
                    .attribute_type(ScalarAttributeType::B)
                    .build()
                    .map_err(map_build)?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("PK")
                    .key_type(KeyType::Hash)
                    .build()
                    .map_err(map_build)?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("SK")
                    .key_type(KeyType::Range)
                    .build()
                    .map_err(map_build)?,
            )
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await;

        match create_result {
            Ok(_) => {}
            Err(e) => {
                let is_in_use = e
                    .as_service_error()
                    .map(|se| se.is_resource_in_use_exception())
                    .unwrap_or(false);
                if !is_in_use {
                    let r = dynamo_sdk_retryability(&e);
                    return Err(DepotError::Storage(Box::new(e.into_service_error()), r));
                }
            }
        }

        // Wait for the table to become active
        loop {
            let desc = self
                .client
                .describe_table()
                .table_name(dynamo_table)
                .send()
                .await
                .map_err(|e| {
                    let r = dynamo_sdk_retryability(&e);
                    DepotError::Storage(Box::new(e.into_service_error()), r)
                })?;

            if let Some(t) = desc.table() {
                if t.table_status() == Some(&aws_sdk_dynamodb::types::TableStatus::Active) {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        Ok(())
    }
}

/// Extract (sk, value) from a DynamoDB item map.
fn extract_sk_value(
    item: &std::collections::HashMap<String, AttributeValue>,
) -> Option<(String, Vec<u8>)> {
    let sk_blob = item.get("SK")?.as_b().ok()?;
    let v_blob = item.get("V")?.as_b().ok()?;
    let sk = String::from_utf8(sk_blob.as_ref().to_vec()).ok()?;
    Some((sk, v_blob.as_ref().to_vec()))
}

/// Extract value bytes from a DynamoDB item map.
fn extract_value(item: &std::collections::HashMap<String, AttributeValue>) -> Option<Vec<u8>> {
    let v_blob = item.get("V")?.as_b().ok()?;
    Some(v_blob.as_ref().to_vec())
}

/// Extract (value, version) from a DynamoDB item map.
fn extract_value_versioned(
    item: &std::collections::HashMap<String, AttributeValue>,
) -> Option<(Vec<u8>, u64)> {
    let v_blob = item.get("V")?.as_b().ok()?;
    let version = item
        .get("VER")
        .and_then(|v| v.as_n().ok())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0);
    Some((v_blob.as_ref().to_vec(), version))
}

#[async_trait::async_trait]
impl depot_core::store::kv::KvStore for DynamoKvStore {
    async fn get(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<Vec<u8>>> {
        self.client
            .get_item()
            .table_name(self.dynamo_table(table))
            .key("PK", AttributeValue::S(pk.into_owned()))
            .key("SK", AttributeValue::B(Blob::new(sk.as_bytes())))
            .send()
            .await
            .map_err(|e| {
                let r = dynamo_sdk_retryability(&e);
                DepotError::Storage(Box::new(e.into_service_error()), r)
            })
            .map(|r| r.item().and_then(extract_value))
    }

    async fn get_consistent(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<Vec<u8>>> {
        self.client
            .get_item()
            .table_name(self.dynamo_table(table))
            .key("PK", AttributeValue::S(pk.into_owned()))
            .key("SK", AttributeValue::B(Blob::new(sk.as_bytes())))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| {
                let r = dynamo_sdk_retryability(&e);
                DepotError::Storage(Box::new(e.into_service_error()), r)
            })
            .map(|r| r.item().and_then(extract_value))
    }

    async fn get_versioned(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<(Vec<u8>, u64)>> {
        self.client
            .get_item()
            .table_name(self.dynamo_table(table))
            .key("PK", AttributeValue::S(pk.into_owned()))
            .key("SK", AttributeValue::B(Blob::new(sk.as_bytes())))
            .consistent_read(true)
            .send()
            .await
            .map_err(|e| {
                let r = dynamo_sdk_retryability(&e);
                DepotError::Storage(Box::new(e.into_service_error()), r)
            })
            .map(|r| r.item().and_then(extract_value_versioned))
    }

    async fn put(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        value: &[u8],
    ) -> error::Result<()> {
        // Use UpdateItem with ADD to atomically increment VER even on concurrent writes.
        self.client
            .update_item()
            .table_name(self.dynamo_table(table))
            .key("PK", AttributeValue::S(pk.into_owned()))
            .key("SK", AttributeValue::B(Blob::new(sk.as_bytes())))
            .update_expression("SET V = :v ADD VER :one")
            .expression_attribute_values(":v", AttributeValue::B(Blob::new(value)))
            .expression_attribute_values(":one", AttributeValue::N("1".to_string()))
            .send()
            .await
            .map_err(|e| {
                let r = dynamo_sdk_retryability(&e);
                DepotError::Storage(Box::new(e.into_service_error()), r)
            })
            .map(|_| ())
    }

    async fn put_if_version(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        value: &[u8],
        expected_version: Option<u64>,
    ) -> error::Result<bool> {
        let new_version = expected_version.unwrap_or(0) + 1;

        let mut builder = self
            .client
            .put_item()
            .table_name(self.dynamo_table(table))
            .item("PK", AttributeValue::S(pk.into_owned()))
            .item("SK", AttributeValue::B(Blob::new(sk.as_bytes())))
            .item("V", AttributeValue::B(Blob::new(value)))
            .item("VER", AttributeValue::N(new_version.to_string()));

        builder = match expected_version {
            None => builder.condition_expression("attribute_not_exists(SK)"),
            Some(v) => builder
                .condition_expression("VER = :expected_ver")
                .expression_attribute_values(":expected_ver", AttributeValue::N(v.to_string())),
        };

        match builder.send().await {
            Ok(_) => Ok(true),
            Err(e) => {
                let is_condition_failed = e
                    .as_service_error()
                    .map(|se| se.is_conditional_check_failed_exception())
                    .unwrap_or(false);
                if is_condition_failed {
                    Ok(false)
                } else {
                    let r = dynamo_sdk_retryability(&e);
                    Err(DepotError::Storage(Box::new(e.into_service_error()), r))
                }
            }
        }
    }

    async fn delete(&self, table: &str, pk: Cow<'_, str>, sk: Cow<'_, str>) -> error::Result<bool> {
        self.client
            .delete_item()
            .table_name(self.dynamo_table(table))
            .key("PK", AttributeValue::S(pk.into_owned()))
            .key("SK", AttributeValue::B(Blob::new(sk.as_bytes())))
            .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
            .send()
            .await
            .map_err(|e| {
                let r = dynamo_sdk_retryability(&e);
                DepotError::Storage(Box::new(e.into_service_error()), r)
            })
            .map(|r| r.attributes().is_some())
    }

    async fn delete_if_version(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
        expected_version: u64,
    ) -> error::Result<bool> {
        match self
            .client
            .delete_item()
            .table_name(self.dynamo_table(table))
            .key("PK", AttributeValue::S(pk.into_owned()))
            .key("SK", AttributeValue::B(Blob::new(sk.as_bytes())))
            .condition_expression("VER = :expected_ver")
            .expression_attribute_values(
                ":expected_ver",
                AttributeValue::N(expected_version.to_string()),
            )
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let is_condition_failed = e
                    .as_service_error()
                    .map(|se| se.is_conditional_check_failed_exception())
                    .unwrap_or(false);
                if is_condition_failed {
                    Ok(false)
                } else {
                    let r = dynamo_sdk_retryability(&e);
                    Err(DepotError::Storage(Box::new(e.into_service_error()), r))
                }
            }
        }
    }

    async fn delete_returning(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk: Cow<'_, str>,
    ) -> error::Result<Option<Vec<u8>>> {
        self.client
            .delete_item()
            .table_name(self.dynamo_table(table))
            .key("PK", AttributeValue::S(pk.into_owned()))
            .key("SK", AttributeValue::B(Blob::new(sk.as_bytes())))
            .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
            .send()
            .await
            .map_err(|e| {
                let r = dynamo_sdk_retryability(&e);
                DepotError::Storage(Box::new(e.into_service_error()), r)
            })
            .map(|r| r.attributes().and_then(extract_value))
    }

    async fn scan_prefix(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk_prefix: Cow<'_, str>,
        limit: usize,
    ) -> error::Result<ScanResult> {
        self.scan_prefix_inner(table, &pk, &sk_prefix, limit).await
    }

    async fn scan_range(
        &self,
        table: &str,
        pk: Cow<'_, str>,
        sk_start: Cow<'_, str>,
        sk_end: Option<&str>,
        limit: usize,
    ) -> error::Result<ScanResult> {
        self.scan_range_inner(table, &pk, &sk_start, sk_end, limit)
            .await
    }

    async fn put_batch(&self, table: &str, entries: &[(&str, &str, &[u8])]) -> error::Result<()> {
        use futures::stream::{self, StreamExt, TryStreamExt};
        const CONCURRENCY: usize = 25;
        let owned: Vec<(String, String, Vec<u8>)> = entries
            .iter()
            .map(|(pk, sk, value)| (pk.to_string(), sk.to_string(), value.to_vec()))
            .collect();
        stream::iter(owned)
            .map(|(pk, sk, value)| async move {
                self.put(table, Cow::Owned(pk), Cow::Owned(sk), &value)
                    .await
            })
            .buffer_unordered(CONCURRENCY)
            .try_collect::<Vec<()>>()
            .await?;
        Ok(())
    }

    async fn delete_batch(&self, table: &str, keys: &[(&str, &str)]) -> error::Result<Vec<bool>> {
        use futures::stream::{self, StreamExt, TryStreamExt};
        const CONCURRENCY: usize = 25;
        let owned: Vec<(String, String)> = keys
            .iter()
            .map(|(pk, sk)| (pk.to_string(), sk.to_string()))
            .collect();
        stream::iter(owned)
            .map(|(pk, sk)| async move { self.delete(table, Cow::Owned(pk), Cow::Owned(sk)).await })
            .buffered(CONCURRENCY)
            .try_collect()
            .await
    }
}

impl DynamoKvStore {
    async fn scan_prefix_inner(
        &self,
        table: &str,
        pk: &str,
        sk_prefix: &str,
        limit: usize,
    ) -> error::Result<ScanResult> {
        let dynamo_table = self.dynamo_table(table);
        let dynamo_pk = pk.to_string();

        let mut results = Vec::new();
        let mut exclusive_start_key: Option<std::collections::HashMap<String, AttributeValue>> =
            None;

        loop {
            let mut builder = self
                .client
                .query()
                .table_name(&dynamo_table)
                .expression_attribute_values(":pk", AttributeValue::S(dynamo_pk.clone()));

            if sk_prefix.is_empty() {
                builder = builder.key_condition_expression("PK = :pk");
            } else {
                builder = builder
                    .key_condition_expression("PK = :pk AND begins_with(SK, :prefix)")
                    .expression_attribute_values(
                        ":prefix",
                        AttributeValue::B(Blob::new(sk_prefix.as_bytes())),
                    );
            }

            builder = builder.limit((limit - results.len()).min(1000) as i32);

            if let Some(ref start_key) = exclusive_start_key {
                builder = builder.set_exclusive_start_key(Some(start_key.clone()));
            }

            let response = builder.send().await.map_err(|e| {
                let r = dynamo_sdk_retryability(&e);
                DepotError::Storage(Box::new(e.into_service_error()), r)
            })?;

            for item in response.items() {
                if results.len() >= limit {
                    return Ok(ScanResult {
                        items: results,
                        done: false,
                    });
                }
                if let Some(kv) = extract_sk_value(item) {
                    results.push(kv);
                }
            }

            match response.last_evaluated_key() {
                Some(_) if results.len() >= limit => {
                    return Ok(ScanResult {
                        items: results,
                        done: false,
                    });
                }
                Some(lek) => {
                    exclusive_start_key = Some(lek.clone());
                }
                None => break,
            }
        }

        Ok(ScanResult {
            items: results,
            done: true,
        })
    }

    async fn scan_range_inner(
        &self,
        table: &str,
        pk: &str,
        sk_start: &str,
        sk_end: Option<&str>,
        limit: usize,
    ) -> error::Result<ScanResult> {
        let dynamo_table = self.dynamo_table(table);
        let dynamo_pk = pk.to_string();

        // DynamoDB key conditions allow at most ONE condition on the sort key,
        // and reject empty binary values. We use >= on the start bound and
        // apply the upper bound (< sk_end) as a client-side filter.
        let has_start = !sk_start.is_empty();

        let mut results = Vec::new();
        let mut exclusive_start_key: Option<std::collections::HashMap<String, AttributeValue>> =
            None;

        loop {
            let mut builder = self
                .client
                .query()
                .table_name(&dynamo_table)
                .expression_attribute_values(":pk", AttributeValue::S(dynamo_pk.clone()));

            if has_start {
                builder = builder
                    .key_condition_expression("PK = :pk AND SK >= :sk_start")
                    .expression_attribute_values(
                        ":sk_start",
                        AttributeValue::B(Blob::new(sk_start.as_bytes())),
                    );
            } else {
                builder = builder.key_condition_expression("PK = :pk");
            }

            builder = builder.limit((limit - results.len()).min(1000) as i32);

            if let Some(ref start_key) = exclusive_start_key {
                builder = builder.set_exclusive_start_key(Some(start_key.clone()));
            }

            let response = builder.send().await.map_err(|e| {
                let r = dynamo_sdk_retryability(&e);
                DepotError::Storage(Box::new(e.into_service_error()), r)
            })?;

            let mut hit_end = false;
            for item in response.items() {
                if results.len() >= limit {
                    return Ok(ScanResult {
                        items: results,
                        done: false,
                    });
                }
                if let Some(kv) = extract_sk_value(item) {
                    // Client-side upper bound: [sk_start, sk_end).
                    if let Some(end) = sk_end {
                        if kv.0.as_str() >= end {
                            hit_end = true;
                            break;
                        }
                    }
                    results.push(kv);
                }
            }

            if hit_end {
                break;
            }

            match response.last_evaluated_key() {
                Some(_) if results.len() >= limit => {
                    return Ok(ScanResult {
                        items: results,
                        done: false,
                    });
                }
                Some(lek) => {
                    exclusive_start_key = Some(lek.clone());
                }
                None => break,
            }
        }

        Ok(ScanResult {
            items: results,
            done: true,
        })
    }
}
