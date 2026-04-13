// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::time::Duration;

/// A single operation result.
pub struct OpResult {
    pub op_type: String,
    pub duration: Duration,
    pub bytes: u64,
    pub success: bool,
}

/// Collects and computes statistics for benchmark operations.
pub struct StatsCollector {
    results: HashMap<String, Vec<OpRecord>>,
}

struct OpRecord {
    duration: Duration,
    bytes: u64,
    success: bool,
}

/// Summary stats for a single operation type.
pub struct OpStats {
    pub op_type: String,
    pub count: usize,
    pub errors: usize,
    pub p50: Duration,
    pub p90: Duration,
    pub p99: Duration,
    pub ops_per_sec: f64,
    pub mb_per_sec: f64,
    #[allow(dead_code)]
    pub total_duration: Duration,
}

impl Default for StatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl StatsCollector {
    pub fn new() -> Self {
        Self {
            results: HashMap::new(),
        }
    }

    pub fn record(&mut self, result: OpResult) {
        self.results
            .entry(result.op_type.clone())
            .or_default()
            .push(OpRecord {
                duration: result.duration,
                bytes: result.bytes,
                success: result.success,
            });
    }

    pub fn compute(&self, wall_duration: Duration) -> Vec<OpStats> {
        let mut stats = Vec::new();
        let wall_secs = wall_duration.as_secs_f64();

        for (op_type, records) in &self.results {
            let count = records.len();
            let errors = records.iter().filter(|r| !r.success).count();
            let total_bytes: u64 = records.iter().map(|r| r.bytes).sum();

            let mut durations: Vec<Duration> = records
                .iter()
                .filter(|r| r.success)
                .map(|r| r.duration)
                .collect();
            durations.sort();

            let p50 = percentile(&durations, 50);
            let p90 = percentile(&durations, 90);
            let p99 = percentile(&durations, 99);

            let ops_per_sec = if wall_secs > 0.0 {
                count as f64 / wall_secs
            } else {
                0.0
            };
            let mb_per_sec = if wall_secs > 0.0 {
                (total_bytes as f64 / (1024.0 * 1024.0)) / wall_secs
            } else {
                0.0
            };

            stats.push(OpStats {
                op_type: op_type.clone(),
                count,
                errors,
                p50,
                p90,
                p99,
                ops_per_sec,
                mb_per_sec,
                total_duration: wall_duration,
            });
        }

        stats.sort_by(|a, b| a.op_type.cmp(&b.op_type));
        stats
    }
}

fn percentile(sorted: &[Duration], pct: u32) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let index = ((pct as f64 / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    #[test]
    fn default_is_empty() {
        let collector = StatsCollector::default();
        let stats = collector.compute(Duration::from_secs(1));
        assert!(stats.is_empty());
    }

    #[test]
    fn stats_collector_empty() {
        let collector = StatsCollector::new();
        let stats = collector.compute(Duration::from_secs(10));
        assert!(stats.is_empty());
    }

    #[test]
    fn stats_collector_single_op() {
        let mut collector = StatsCollector::new();
        collector.record(OpResult {
            op_type: "upload".into(),
            duration: ms(100),
            bytes: 1024,
            success: true,
        });
        let stats = collector.compute(Duration::from_secs(1));
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].op_type, "upload");
        assert_eq!(stats[0].count, 1);
        assert_eq!(stats[0].errors, 0);
        assert_eq!(stats[0].p50, ms(100));
    }

    #[test]
    fn stats_collector_multiple_ops() {
        let mut collector = StatsCollector::new();
        for i in 0..10 {
            collector.record(OpResult {
                op_type: "upload".into(),
                duration: ms(i * 10),
                bytes: 1000,
                success: true,
            });
        }
        let stats = collector.compute(Duration::from_secs(1));
        assert_eq!(stats[0].count, 10);
        assert_eq!(stats[0].errors, 0);
        assert_eq!(stats[0].p50, ms(50)); // median of 0,10,20,...,90
    }

    #[test]
    fn stats_collector_mixed_op_types() {
        let mut collector = StatsCollector::new();
        collector.record(OpResult {
            op_type: "upload".into(),
            duration: ms(10),
            bytes: 100,
            success: true,
        });
        collector.record(OpResult {
            op_type: "download".into(),
            duration: ms(5),
            bytes: 200,
            success: true,
        });
        collector.record(OpResult {
            op_type: "upload".into(),
            duration: ms(20),
            bytes: 150,
            success: true,
        });
        let stats = collector.compute(Duration::from_secs(1));
        assert_eq!(stats.len(), 2);
        // Sorted by op_type
        assert_eq!(stats[0].op_type, "download");
        assert_eq!(stats[0].count, 1);
        assert_eq!(stats[1].op_type, "upload");
        assert_eq!(stats[1].count, 2);
    }

    #[test]
    fn stats_collector_errors_counted() {
        let mut collector = StatsCollector::new();
        collector.record(OpResult {
            op_type: "upload".into(),
            duration: ms(10),
            bytes: 0,
            success: false,
        });
        collector.record(OpResult {
            op_type: "upload".into(),
            duration: ms(5),
            bytes: 100,
            success: true,
        });
        collector.record(OpResult {
            op_type: "upload".into(),
            duration: ms(15),
            bytes: 0,
            success: false,
        });
        let stats = collector.compute(Duration::from_secs(1));
        assert_eq!(stats[0].count, 3);
        assert_eq!(stats[0].errors, 2);
        // Only successful durations in percentiles
        assert_eq!(stats[0].p50, ms(5));
    }

    #[test]
    fn stats_collector_ops_per_sec() {
        let mut collector = StatsCollector::new();
        for _ in 0..100 {
            collector.record(OpResult {
                op_type: "op".into(),
                duration: ms(1),
                bytes: 1024,
                success: true,
            });
        }
        let stats = collector.compute(Duration::from_secs(10));
        assert!((stats[0].ops_per_sec - 10.0).abs() < 0.01);
    }

    #[test]
    fn stats_collector_mb_per_sec() {
        let mut collector = StatsCollector::new();
        let one_mb = 1024 * 1024;
        for _ in 0..10 {
            collector.record(OpResult {
                op_type: "op".into(),
                duration: ms(1),
                bytes: one_mb,
                success: true,
            });
        }
        let stats = collector.compute(Duration::from_secs(10));
        // 10 MB over 10 seconds = 1 MB/s
        assert!((stats[0].mb_per_sec - 1.0).abs() < 0.01);
    }

    #[test]
    fn stats_collector_zero_duration() {
        let mut collector = StatsCollector::new();
        collector.record(OpResult {
            op_type: "op".into(),
            duration: ms(1),
            bytes: 100,
            success: true,
        });
        let stats = collector.compute(Duration::ZERO);
        assert_eq!(stats[0].ops_per_sec, 0.0);
        assert_eq!(stats[0].mb_per_sec, 0.0);
    }

    #[test]
    fn percentile_empty() {
        assert_eq!(percentile(&[], 50), Duration::ZERO);
    }

    #[test]
    fn percentile_single_element() {
        let sorted = vec![ms(42)];
        assert_eq!(percentile(&sorted, 0), ms(42));
        assert_eq!(percentile(&sorted, 50), ms(42));
        assert_eq!(percentile(&sorted, 99), ms(42));
    }

    #[test]
    fn percentile_known_values() {
        // 10 elements: 0ms, 10ms, 20ms, ..., 90ms
        let sorted: Vec<Duration> = (0..10).map(|i| ms(i * 10)).collect();
        assert_eq!(percentile(&sorted, 50), ms(50)); // index 4.5 rounds to 5
        assert_eq!(percentile(&sorted, 0), ms(0));
    }

    #[test]
    fn percentile_p99_near_max() {
        let sorted: Vec<Duration> = (0..100).map(ms).collect();
        let p99 = percentile(&sorted, 99);
        // p99 of 0..99 should be near 98
        assert!(p99 >= ms(97));
    }

    #[test]
    fn stats_sorted_by_op_type() {
        let mut collector = StatsCollector::new();
        collector.record(OpResult {
            op_type: "zebra".into(),
            duration: ms(1),
            bytes: 0,
            success: true,
        });
        collector.record(OpResult {
            op_type: "alpha".into(),
            duration: ms(1),
            bytes: 0,
            success: true,
        });
        collector.record(OpResult {
            op_type: "middle".into(),
            duration: ms(1),
            bytes: 0,
            success: true,
        });
        let stats = collector.compute(Duration::from_secs(1));
        assert_eq!(stats[0].op_type, "alpha");
        assert_eq!(stats[1].op_type, "middle");
        assert_eq!(stats[2].op_type, "zebra");
    }
}
