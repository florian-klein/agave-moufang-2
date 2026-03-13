//! Shredstream health tracking
//!
//! Provides health status for the shredstream service that can be queried
//! to determine if the connection to Jito's block engine is healthy.

use std::{
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{Duration, Instant},
};

/// Health status for the shredstream service
#[derive(Debug)]
pub struct ShredstreamHealth {
    /// Whether the service is currently connected to the block engine
    connected: AtomicBool,
    /// Timestamp of the last successful heartbeat (as milliseconds since service start)
    last_successful_heartbeat_ms: AtomicU64,
    /// Total number of successful heartbeats
    successful_heartbeat_count: AtomicU64,
    /// Total number of failed heartbeats
    failed_heartbeat_count: AtomicU64,
    /// Number of times the client has been restarted
    client_restart_count: AtomicU64,
    /// When the service started
    service_start: Instant,
}

impl Default for ShredstreamHealth {
    fn default() -> Self {
        Self::new()
    }
}

impl ShredstreamHealth {
    /// Create a new health tracker
    pub fn new() -> Self {
        Self {
            connected: AtomicBool::new(false),
            last_successful_heartbeat_ms: AtomicU64::new(0),
            successful_heartbeat_count: AtomicU64::new(0),
            failed_heartbeat_count: AtomicU64::new(0),
            client_restart_count: AtomicU64::new(0),
            service_start: Instant::now(),
        }
    }

    /// Record that we've connected to the block engine
    pub fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Relaxed);
    }

    /// Record a successful heartbeat
    pub fn record_successful_heartbeat(&self) {
        self.last_successful_heartbeat_ms.store(
            self.service_start.elapsed().as_millis() as u64,
            Ordering::Relaxed,
        );
        self.successful_heartbeat_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed heartbeat
    pub fn record_failed_heartbeat(&self) {
        self.failed_heartbeat_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a client restart
    pub fn record_client_restart(&self) {
        self.client_restart_count.fetch_add(1, Ordering::Relaxed);
        self.connected.store(false, Ordering::Relaxed);
    }

    /// Check if the service is healthy
    ///
    /// The service is considered healthy if:
    /// - It is connected to the block engine
    /// - A heartbeat was successful within the last 2 minutes
    pub fn is_healthy(&self) -> bool {
        if !self.connected.load(Ordering::Relaxed) {
            return false;
        }

        let last_heartbeat_ms = self.last_successful_heartbeat_ms.load(Ordering::Relaxed);
        if last_heartbeat_ms == 0 {
            // No heartbeat yet, but we're connected - give it some time
            return self.service_start.elapsed() < Duration::from_secs(60);
        }

        let elapsed_since_heartbeat = self.service_start.elapsed().as_millis() as u64 - last_heartbeat_ms;
        // Healthy if last heartbeat was within 2 minutes (heartbeat interval is ~40s)
        elapsed_since_heartbeat < 120_000
    }

    /// Get the current health status as a summary
    pub fn get_status(&self) -> ShredstreamHealthStatus {
        ShredstreamHealthStatus {
            connected: self.connected.load(Ordering::Relaxed),
            healthy: self.is_healthy(),
            successful_heartbeat_count: self.successful_heartbeat_count.load(Ordering::Relaxed),
            failed_heartbeat_count: self.failed_heartbeat_count.load(Ordering::Relaxed),
            client_restart_count: self.client_restart_count.load(Ordering::Relaxed),
            seconds_since_last_heartbeat: self.seconds_since_last_heartbeat(),
            uptime_seconds: self.service_start.elapsed().as_secs(),
        }
    }

    /// Get seconds since last successful heartbeat
    pub fn seconds_since_last_heartbeat(&self) -> Option<u64> {
        let last_heartbeat_ms = self.last_successful_heartbeat_ms.load(Ordering::Relaxed);
        if last_heartbeat_ms == 0 {
            return None;
        }
        let elapsed_since_heartbeat = self.service_start.elapsed().as_millis() as u64 - last_heartbeat_ms;
        Some(elapsed_since_heartbeat / 1000)
    }
}

/// Snapshot of shredstream health status
#[derive(Debug, Clone)]
pub struct ShredstreamHealthStatus {
    /// Whether currently connected to block engine
    pub connected: bool,
    /// Whether the service is considered healthy
    pub healthy: bool,
    /// Total successful heartbeats
    pub successful_heartbeat_count: u64,
    /// Total failed heartbeats
    pub failed_heartbeat_count: u64,
    /// Number of client restarts
    pub client_restart_count: u64,
    /// Seconds since last successful heartbeat (None if no heartbeat yet)
    pub seconds_since_last_heartbeat: Option<u64>,
    /// Service uptime in seconds
    pub uptime_seconds: u64,
}

impl std::fmt::Display for ShredstreamHealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Shredstream: {} (connected={}, heartbeats={}/{}, restarts={}, last_hb={}s ago, uptime={}s)",
            if self.healthy { "HEALTHY" } else { "UNHEALTHY" },
            self.connected,
            self.successful_heartbeat_count,
            self.successful_heartbeat_count + self.failed_heartbeat_count,
            self.client_restart_count,
            self.seconds_since_last_heartbeat.map(|s| s.to_string()).unwrap_or_else(|| "never".to_string()),
            self.uptime_seconds,
        )
    }
}
