// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;
use std::process::Command;
use std::time::SystemTime;

fn main() {
    for path in &[
        "frontend/package.json",
        "frontend/vite.config.ts",
        "frontend/vite.config.test.ts",
        "frontend/tsconfig.json",
        "frontend/index.html",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
    println!("cargo:rerun-if-env-changed=DEPOT_INSTRUMENT_FRONTEND");
    rerun_if_changed_recursive(Path::new("frontend/src"));
    rerun_if_changed_recursive(Path::new("frontend/public"));

    let instrumented = std::env::var("DEPOT_INSTRUMENT_FRONTEND").is_ok();
    if needs_rebuild(instrumented) {
        build_frontend(instrumented);
    }

    let dist = Path::new("frontend/dist");
    if dist.is_dir() {
        rerun_if_changed_recursive(dist);
    }
    println!("cargo:rerun-if-changed=frontend/dist");
}

fn needs_npm_install(frontend_dir: &Path) -> bool {
    let sentinel = frontend_dir.join("node_modules/.package-lock.json");
    if !sentinel.exists() {
        return true;
    }
    let sentinel_time = sentinel
        .metadata()
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let pkg_time = frontend_dir
        .join("package.json")
        .metadata()
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);
    pkg_time > sentinel_time
}

fn needs_rebuild(instrumented: bool) -> bool {
    let dist_marker = Path::new("frontend/dist/index.html");
    if !dist_marker.exists() {
        return true;
    }

    // Rebuild when switching between normal and instrumented modes.
    let mode_marker = Path::new("frontend/dist/.instrumented");
    if instrumented != mode_marker.exists() {
        return true;
    }

    let dist_time = dist_marker
        .metadata()
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);

    let sources = &[
        "frontend/package.json",
        "frontend/vite.config.ts",
        "frontend/vite.config.test.ts",
        "frontend/tsconfig.json",
        "frontend/index.html",
    ];
    for src in sources {
        let path = Path::new(src);
        if path.exists() {
            if let Ok(t) = path.metadata().and_then(|m| m.modified()) {
                if t > dist_time {
                    return true;
                }
            }
        }
    }

    if let Some(t) = newest_mtime(Path::new("frontend/src")) {
        if t > dist_time {
            return true;
        }
    }

    if let Some(t) = newest_mtime(Path::new("frontend/public")) {
        if t > dist_time {
            return true;
        }
    }

    false
}

fn newest_mtime(dir: &Path) -> Option<SystemTime> {
    let mut newest: Option<SystemTime> = None;
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return None,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(t) = newest_mtime(&path) {
                newest = Some(newest.map_or(t, |cur| cur.max(t)));
            }
        } else if let Ok(t) = path.metadata().and_then(|m| m.modified()) {
            newest = Some(newest.map_or(t, |cur| cur.max(t)));
        }
    }
    newest
}

fn build_frontend(instrumented: bool) {
    let frontend_dir = Path::new("frontend");

    if needs_npm_install(frontend_dir) {
        let status = Command::new("npm")
            .arg("install")
            .current_dir(frontend_dir)
            .status();
        match status {
            Ok(s) if s.success() => {}
            Ok(s) => panic!("npm install failed with {s}"),
            Err(e) => panic!("failed to run npm install: {e}"),
        }
    }

    let script = if instrumented { "build:test" } else { "build" };
    let status = Command::new("npm")
        .args(["run", script])
        .current_dir(frontend_dir)
        .status();
    match status {
        Ok(s) if s.success() => {}
        Ok(s) => panic!("npm run {script} failed with {s}"),
        Err(e) => panic!("failed to run npm run {script}: {e}"),
    }

    // Write/remove mode marker so we can detect mode switches.
    let mode_marker = Path::new("frontend/dist/.instrumented");
    if instrumented {
        std::fs::write(mode_marker, "").ok();
    } else if mode_marker.exists() {
        std::fs::remove_file(mode_marker).ok();
    }
}

fn rerun_if_changed_recursive(dir: &Path) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rerun_if_changed_recursive(&path);
        } else {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
}
