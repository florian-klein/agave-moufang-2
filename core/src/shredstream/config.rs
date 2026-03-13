use std::{net::IpAddr, path::PathBuf};

/// Configuration for Jito shredstream integration
#[derive(Clone, Debug, Default)]
pub struct ShredstreamConfig {
    /// Whether shredstream is enabled
    pub enabled: bool,
    /// Jito block engine URL (e.g., "https://mainnet.block-engine.jito.wtf")
    pub block_engine_url: Option<String>,
    /// Path to keypair for Jito authentication (defaults to validator identity)
    pub auth_keypair_path: Option<PathBuf>,
    /// Regions to receive shreds from (e.g., ["amsterdam", "frankfurt", "ny", "tokyo"])
    pub regions: Vec<String>,
    /// Public IP address for shredstream (defaults to auto-detect from gossip)
    pub public_ip: Option<IpAddr>,
}

/// First port used for supplementary multi-heartbeat connections.
pub const MULTI_HEARTBEAT_PORT_START: u16 = 20100;

/// Maximum supplementary ports to register.
/// Jito rate-limits to 20 req/min per identity KEY (shared across ALL
/// machines using the same auth keypair). Each port needs a heartbeat
/// every ~40s (TTL/3) = 1.5 req/min. With 3 supplementary ports + 1
/// primary = 6 req/min per machine. Safe for 3 machines (18 req/min).
pub const MULTI_HEARTBEAT_MAX_CONNECTIONS: u16 = 3;

/// Per-port heartbeat interval (seconds). Must be <= TTL/3 to keep
/// registrations alive. Jito's TTL is ~120s, so 40s is the sweet spot.
pub const MULTI_HEARTBEAT_PER_PORT_INTERVAL_SECS: u64 = 40;

/// Returns the number of known unique Jito relay boxes for a given region.
/// This is used by the multi-heartbeat manager to know when all relays have been discovered.
pub fn expected_relay_count(region: &str) -> usize {
    match region {
        "amsterdam" => 11,
        "dublin" => 3,
        "frankfurt" => 10,
        "london" => 6,
        "ny" | "new york" | "new_york" => 8,
        "salt_lake_city" | "salt lake city" | "slc" => 7,
        "singapore" => 5,
        "tokyo" => 6,
        _ => 0,
    }
}

/// Returns the total expected unique relay IPs across all given regions.
pub fn total_expected_relays(regions: &[String]) -> usize {
    regions.iter().map(|r| expected_relay_count(r)).sum()
}

impl ShredstreamConfig {
    /// Create a disabled shredstream config
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Create an enabled shredstream config with the given block engine URL
    pub fn new(block_engine_url: String, regions: Vec<String>) -> Self {
        Self {
            enabled: true,
            block_engine_url: Some(block_engine_url),
            auth_keypair_path: None,
            regions,
            public_ip: None,
        }
    }

    /// Get the auth URL derived from the block engine URL
    /// Jito uses the same URL for both block engine and auth
    pub fn auth_url(&self) -> Option<String> {
        self.block_engine_url.clone()
    }
}
