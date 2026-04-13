// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use super::stats::OpStats;
use serde::Serialize;

#[derive(Serialize)]
struct JsonReport {
    scenario: String,
    duration_secs: f64,
    concurrency: usize,
    operations: Vec<JsonOpStats>,
}

#[derive(Serialize)]
struct JsonOpStats {
    operation: String,
    count: usize,
    errors: usize,
    p50_ms: f64,
    p90_ms: f64,
    p99_ms: f64,
    ops_per_sec: f64,
    mb_per_sec: f64,
}

pub fn print_table(scenario: &str, duration_secs: u64, concurrency: usize, stats: &[OpStats]) {
    use std::fmt::Write;
    let mut buf = String::new();
    writeln!(buf).unwrap();
    writeln!(
        buf,
        "Scenario: {} ({}s, {} workers)",
        scenario, duration_secs, concurrency
    )
    .unwrap();
    writeln!(
        buf,
        "  {:<16} {:>7} {:>7} {:>8} {:>8} {:>8} {:>8} {:>8}",
        "Operation", "Count", "Errors", "p50", "p90", "p99", "ops/s", "MB/s"
    )
    .unwrap();
    writeln!(buf, "  {}", "-".repeat(82)).unwrap();

    for s in stats {
        writeln!(
            buf,
            "  {:<16} {:>7} {:>7} {:>8} {:>8} {:>8} {:>8.1} {:>8.1}",
            s.op_type,
            s.count,
            s.errors,
            format_duration(s.p50),
            format_duration(s.p90),
            format_duration(s.p99),
            s.ops_per_sec,
            s.mb_per_sec,
        )
        .unwrap();
    }
    tracing::info!("{}", buf);
}

pub fn print_json(scenario: &str, duration_secs: u64, concurrency: usize, stats: &[OpStats]) {
    let report = JsonReport {
        scenario: scenario.to_string(),
        duration_secs: duration_secs as f64,
        concurrency,
        operations: stats
            .iter()
            .map(|s| JsonOpStats {
                operation: s.op_type.clone(),
                count: s.count,
                errors: s.errors,
                p50_ms: s.p50.as_secs_f64() * 1000.0,
                p90_ms: s.p90.as_secs_f64() * 1000.0,
                p99_ms: s.p99.as_secs_f64() * 1000.0,
                ops_per_sec: s.ops_per_sec,
                mb_per_sec: s.mb_per_sec,
            })
            .collect(),
    };
    tracing::info!("{}", serde_json::to_string_pretty(&report).unwrap());
}

pub(crate) fn format_duration(d: std::time::Duration) -> String {
    let us = d.as_micros();
    if us < 1000 {
        format!("{}us", us)
    } else if us < 1_000_000 {
        format!("{:.1}ms", us as f64 / 1000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn format_duration_microseconds() {
        assert_eq!(format_duration(Duration::from_micros(0)), "0us");
        assert_eq!(format_duration(Duration::from_micros(500)), "500us");
        assert_eq!(format_duration(Duration::from_micros(999)), "999us");
    }

    #[test]
    fn format_duration_milliseconds() {
        assert_eq!(format_duration(Duration::from_micros(1000)), "1.0ms");
        assert_eq!(format_duration(Duration::from_micros(1500)), "1.5ms");
        assert_eq!(format_duration(Duration::from_micros(999_999)), "1000.0ms");
    }

    #[test]
    fn format_duration_seconds() {
        assert_eq!(format_duration(Duration::from_secs(1)), "1.00s");
        assert_eq!(format_duration(Duration::from_millis(1500)), "1.50s");
        assert_eq!(format_duration(Duration::from_secs(10)), "10.00s");
    }

    fn make_stats(op_type: &str, count: usize, errors: usize) -> OpStats {
        OpStats {
            op_type: op_type.to_string(),
            count,
            errors,
            p50: Duration::from_millis(2),
            p90: Duration::from_millis(4),
            p99: Duration::from_millis(12),
            ops_per_sec: 50.0,
            mb_per_sec: 51.2,
            total_duration: Duration::from_secs(30),
        }
    }

    #[test]
    fn json_report_structure() {
        let stats = [
            make_stats("raw-upload", 1523, 0),
            make_stats("raw-download", 3044, 2),
        ];
        // Capture JSON output by building the struct directly
        let report = JsonReport {
            scenario: "raw-mixed".to_string(),
            duration_secs: 30.0,
            concurrency: 4,
            operations: stats
                .iter()
                .map(|s| JsonOpStats {
                    operation: s.op_type.clone(),
                    count: s.count,
                    errors: s.errors,
                    p50_ms: s.p50.as_secs_f64() * 1000.0,
                    p90_ms: s.p90.as_secs_f64() * 1000.0,
                    p99_ms: s.p99.as_secs_f64() * 1000.0,
                    ops_per_sec: s.ops_per_sec,
                    mb_per_sec: s.mb_per_sec,
                })
                .collect(),
        };
        let json_str = serde_json::to_string_pretty(&report).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed["scenario"], "raw-mixed");
        assert_eq!(parsed["duration_secs"], 30.0);
        assert_eq!(parsed["concurrency"], 4);

        let ops = parsed["operations"].as_array().unwrap();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0]["operation"], "raw-upload");
        assert_eq!(ops[0]["count"], 1523);
        assert_eq!(ops[0]["errors"], 0);
        assert_eq!(ops[1]["operation"], "raw-download");
        assert_eq!(ops[1]["errors"], 2);

        // Check latency values are in milliseconds
        assert!((ops[0]["p50_ms"].as_f64().unwrap() - 2.0).abs() < 0.01);
        assert!((ops[0]["p90_ms"].as_f64().unwrap() - 4.0).abs() < 0.01);
        assert!((ops[0]["p99_ms"].as_f64().unwrap() - 12.0).abs() < 0.01);
    }

    #[test]
    fn print_table_runs_without_panic() {
        let stats = vec![
            make_stats("raw-upload", 1523, 0),
            make_stats("raw-download", 3044, 2),
        ];
        // Just verify it doesn't panic
        print_table("raw-mixed", 30, 4, &stats);
    }

    #[test]
    fn print_table_empty_stats() {
        print_table("empty", 10, 1, &[]);
    }

    #[test]
    fn print_json_runs_without_panic() {
        let stats = vec![make_stats("raw-upload", 100, 5)];
        print_json("test", 10, 2, &stats);
    }

    #[test]
    fn print_json_empty_stats() {
        print_json("empty", 10, 1, &[]);
    }

    #[test]
    fn json_report_empty_stats() {
        let report = JsonReport {
            scenario: "empty".to_string(),
            duration_secs: 10.0,
            concurrency: 1,
            operations: vec![],
        };
        let json_str = serde_json::to_string(&report).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["operations"].as_array().unwrap().len(), 0);
    }
}
