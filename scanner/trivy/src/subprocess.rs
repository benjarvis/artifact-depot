// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Thin harness around `tokio::process::Command` with a deadline and
//! `kill_on_drop` so cancelled futures don't leak processes.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use tokio::process::Command;
use tokio::time::timeout;
use tracing::debug;

use depot_core::error::{DepotError, Result};

/// Captured output of a finished subprocess.
#[derive(Debug, Clone)]
pub struct SubprocessOutput {
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl SubprocessOutput {
    pub fn success(&self) -> bool {
        matches!(self.exit_code, Some(0))
    }

    pub fn stderr_str(&self) -> String {
        String::from_utf8_lossy(&self.stderr).into_owned()
    }
}

/// Run `binary` with `args` and collect its output. The process is killed
/// when the future is dropped (cancellation) or when `deadline` elapses.
///
/// Trivy writes its JSON report to stdout and progress/errors to stderr.
/// Both are captured fully so the caller can parse the report and include
/// stderr in error diagnostics when things fail.
pub async fn run(binary: &Path, args: &[&str], deadline: Duration) -> Result<SubprocessOutput> {
    let mut cmd = Command::new(binary);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    debug!(binary = %binary.display(), ?args, "spawning trivy subprocess");
    let child = cmd.spawn().map_err(|e| {
        DepotError::Internal(format!("failed to spawn `{}`: {e}", binary.display()))
    })?;

    // `wait_with_output` consumes the child and returns captured stdio.
    let output = match timeout(deadline, child.wait_with_output()).await {
        Ok(res) => res.map_err(|e| DepotError::Internal(format!("trivy io error: {e}")))?,
        Err(_elapsed) => {
            // The child is killed automatically because `kill_on_drop(true)`
            // is set; we dropped the JoinHandle by going through the timeout
            // error branch.
            return Err(DepotError::Internal(format!(
                "trivy scan exceeded {deadline:?}"
            )));
        }
    };

    Ok(SubprocessOutput {
        exit_code: output.status.code(),
        stdout: output.stdout,
        stderr: output.stderr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn which(name: &str) -> Option<PathBuf> {
        std::env::var_os("PATH").and_then(|paths| {
            std::env::split_paths(&paths).find_map(|dir| {
                let candidate = dir.join(name);
                if candidate.is_file() {
                    Some(candidate)
                } else {
                    None
                }
            })
        })
    }

    #[tokio::test]
    async fn captures_stdout() {
        let Some(sh) = which("sh") else {
            // Skip when /bin/sh isn't available (unlikely on Linux).
            return;
        };
        let out = run(&sh, &["-c", "printf hello"], Duration::from_secs(5))
            .await
            .unwrap();
        assert!(out.success());
        assert_eq!(out.stdout, b"hello");
    }

    #[tokio::test]
    async fn times_out_long_sleep() {
        let Some(sh) = which("sh") else {
            return;
        };
        let err = run(&sh, &["-c", "sleep 30"], Duration::from_millis(100))
            .await
            .unwrap_err();
        assert!(matches!(err, DepotError::Internal(_)));
        assert!(err.to_string().contains("exceeded"));
    }

    #[tokio::test]
    async fn nonzero_exit_reported() {
        let Some(sh) = which("sh") else {
            return;
        };
        let out = run(
            &sh,
            &["-c", "echo oops 1>&2; exit 7"],
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert!(!out.success());
        assert_eq!(out.exit_code, Some(7));
        assert!(out.stderr_str().contains("oops"));
    }

    #[tokio::test]
    async fn missing_binary_is_internal_error() {
        let nonexistent = PathBuf::from("/nonexistent/definitely-not-here");
        let err = run(&nonexistent, &[], Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(matches!(err, DepotError::Internal(_)));
    }
}
