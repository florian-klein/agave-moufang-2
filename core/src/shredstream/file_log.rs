//! Dedicated file logger for shredstream debugging.
//!
//! Writes timestamped log lines to a separate file (default:
//! `/var/solana/data/shredstream.log`) so shredstream activity can be
//! inspected without grepping through the main validator log.

use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

static LOGGER: OnceLock<ShredstreamFileLogger> = OnceLock::new();

const DEFAULT_LOG_PATH: &str = "/var/solana/data/shredstream.log";

struct ShredstreamFileLogger {
    writer: Mutex<BufWriter<File>>,
    path: PathBuf,
}

/// Initialise the shredstream file logger.
/// Safe to call multiple times - only the first call takes effect.
pub fn init(path: Option<&Path>) {
    let path = path
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from(DEFAULT_LOG_PATH));

    LOGGER.get_or_init(|| {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap_or_else(|e| {
                panic!(
                    "shredstream: cannot open log file {}: {e}",
                    path.display()
                )
            });
        ShredstreamFileLogger {
            writer: Mutex::new(BufWriter::new(file)),
            path,
        }
    });
}

/// Write a single line to the shredstream log file.
/// No-op if [`init`] was never called.
pub fn write_line(level: &str, msg: &str) {
    let Some(logger) = LOGGER.get() else {
        return;
    };
    let ts = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
    if let Ok(mut w) = logger.writer.lock() {
        let _ = writeln!(w, "[{ts} {level}] {msg}");
        let _ = w.flush();
    }
}

/// Return the path the logger is writing to (for startup messages).
pub fn log_path() -> Option<&'static Path> {
    LOGGER.get().map(|l| l.path.as_path())
}

/// Log to both the standard `log` crate and the shredstream file.
macro_rules! ss_info {
    ($($arg:tt)+) => {{
        log::info!($($arg)+);
        $crate::shredstream::file_log::write_line("INFO", &format!($($arg)+));
    }};
}

macro_rules! ss_warn {
    ($($arg:tt)+) => {{
        log::warn!($($arg)+);
        $crate::shredstream::file_log::write_line("WARN", &format!($($arg)+));
    }};
}

macro_rules! ss_error {
    ($($arg:tt)+) => {{
        log::error!($($arg)+);
        $crate::shredstream::file_log::write_line("ERROR", &format!($($arg)+));
    }};
}

pub(crate) use {ss_error, ss_info, ss_warn};
