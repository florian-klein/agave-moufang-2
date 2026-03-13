//! Jito Shredstream Service
//!
//! Main service that orchestrates shredstream integration.
//! Registers the validator's TVU socket with Jito's block engine
//! via gRPC heartbeats. Shreds are then sent by Jito directly to
//! the TVU socket alongside normal turbine shreds.
//!
//! Multi-heartbeat is enabled by default: supplementary heartbeat
//! connections register extra ports (whose sockets are in the TVU
//! pipeline) to discover additional relay sources for maximum coverage.
//!
//! ## Live restart
//!
//! The service can be restarted without restarting the validator by
//! sending "restart" to a local UDP socket (127.0.0.1:20099):
//!
//! ```text
//! echo restart | nc -u 127.0.0.1 20099
//! ```
//!
//! This tears down all heartbeat threads and recreates them from
//! scratch (fresh tokio runtime, fresh gRPC auth). The TVU sockets
//! remain in the pipeline — only the heartbeat registrations restart.
//! Restarts are rate-limited to at most once per 30 seconds.

use {
    super::{
        config::ShredstreamConfig,
        file_log::{self, ss_info, ss_warn},
        health::ShredstreamHealth,
        heartbeat::heartbeat_loop_thread,
        multi_heartbeat, ShredstreamError,
    },
    crossbeam_channel::{bounded, Sender},
    solana_keypair::{read_keypair_file, Keypair},
    std::{
        io::ErrorKind,
        net::{SocketAddr, UdpSocket},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::{Builder, JoinHandle},
        time::{Duration, Instant},
    },
    tokio::runtime::Runtime,
};

/// Port for the local restart listener (localhost only).
/// Send "restart" to 127.0.0.1:20099 to restart shredstream.
const RESTART_LISTENER_PORT: u16 = 20099;

/// Minimum interval between restarts to prevent restart flooding.
const RESTART_COOLDOWN: Duration = Duration::from_secs(30);

/// Config stored for respawning heartbeat threads on restart.
struct HeartbeatConfig {
    block_engine_url: String,
    auth_url: String,
    auth_keypair: Arc<Keypair>,
    regions: Vec<String>,
    tvu_addr: SocketAddr,
    extra_ports: Vec<u16>,
}

/// Running heartbeat threads and their shutdown channels.
struct HeartbeatState {
    heartbeat_handle: JoinHandle<()>,
    multi_hb_handle: Option<JoinHandle<()>>,
    shutdown_sender: Sender<()>,
    multi_hb_shutdown_sender: Option<Sender<()>>,
}

/// Why the poll loop exited.
enum PollResult {
    /// User sent "restart" command.
    Restart,
    /// A heartbeat thread died (panicked or exited unexpectedly).
    ThreadDied,
    /// Global exit flag was set.
    Exit,
}

impl HeartbeatState {
    /// Spawn primary and multi-heartbeat threads.
    fn spawn(
        config: &HeartbeatConfig,
        exit: &Arc<AtomicBool>,
        health: &Arc<ShredstreamHealth>,
    ) -> Result<Self, String> {
        let runtime = Runtime::new()
            .map_err(|e| format!("failed to create tokio runtime: {e}"))?;
        let (shutdown_sender, shutdown_receiver) = bounded(1);

        let heartbeat_handle = heartbeat_loop_thread(
            config.block_engine_url.clone(),
            config.auth_url.clone(),
            config.auth_keypair.clone(),
            config.regions.clone(),
            config.tvu_addr,
            runtime,
            "shredstream".to_string(),
            shutdown_receiver,
            exit.clone(),
            health.clone(),
            None,
        );

        let (multi_hb_handle, multi_hb_shutdown_sender) = if !config.extra_ports.is_empty() {
            let (sender, receiver) = bounded(1);
            let handle = multi_heartbeat::start_multi_heartbeat_manager(
                config.tvu_addr.ip(),
                config.extra_ports.clone(),
                config.block_engine_url.clone(),
                config.auth_url.clone(),
                config.auth_keypair.clone(),
                config.regions.clone(),
                receiver,
                exit.clone(),
            );
            (Some(handle), Some(sender))
        } else {
            (None, None)
        };

        Ok(Self {
            heartbeat_handle,
            multi_hb_handle,
            shutdown_sender,
            multi_hb_shutdown_sender,
        })
    }

    /// Returns true if any managed thread has finished (crashed or exited).
    fn any_thread_dead(&self) -> bool {
        if self.heartbeat_handle.is_finished() {
            return true;
        }
        if let Some(ref h) = self.multi_hb_handle {
            if h.is_finished() {
                return true;
            }
        }
        false
    }

    /// Signal threads to stop and join them.
    fn stop(self) {
        let _ = self.shutdown_sender.send(());
        if let Some(sender) = self.multi_hb_shutdown_sender {
            let _ = sender.send(());
        }
        if let Some(handle) = self.multi_hb_handle {
            let _ = handle.join();
        }
        let _ = self.heartbeat_handle.join();
    }
}

/// Jito Shredstream Service
///
/// Manages the heartbeat connection to Jito's block engine to receive
/// shreds directly on the validator's TVU socket. Supports live restart
/// via a local UDP command socket (127.0.0.1:20099).
pub struct ShredstreamService {
    manager_handle: JoinHandle<()>,
    exit: Arc<AtomicBool>,
    health: Arc<ShredstreamHealth>,
}

impl ShredstreamService {
    /// Create a new ShredstreamService.
    ///
    /// # Arguments
    /// * `config` - Shredstream configuration
    /// * `identity_keypair` - Validator identity keypair (used for auth if no separate keypair specified)
    /// * `tvu_receive_addr` - The public TVU socket address (public_ip:port) to register with Jito
    /// * `extra_ports` - Additional ports whose sockets are already in the TVU pipeline;
    ///   the manager will gradually register them with Jito via heartbeats.
    /// * `exit` - Global exit signal
    pub fn new(
        config: ShredstreamConfig,
        identity_keypair: &Arc<Keypair>,
        tvu_receive_addr: SocketAddr,
        extra_ports: Vec<u16>,
        exit: Arc<AtomicBool>,
    ) -> Result<Self, ShredstreamError> {
        let block_engine_url = config.block_engine_url.ok_or(ShredstreamError::Config(
            "block_engine_url is required".into(),
        ))?;

        let auth_url = block_engine_url.clone();

        let auth_keypair = if let Some(keypair_path) = config.auth_keypair_path {
            let keypair = read_keypair_file(&keypair_path)
                .map_err(|e| ShredstreamError::Config(e.to_string()))?;
            Arc::new(keypair)
        } else {
            identity_keypair.clone()
        };

        let regions = config.regions;
        if regions.is_empty() {
            return Err(ShredstreamError::Config(
                "at least one region must be specified".into(),
            ));
        }

        file_log::init(None);

        ss_info!(
            "Shredstream: starting service for {} with regions {:?}, TVU addr: {}, extra ports: {:?}",
            block_engine_url, regions, tvu_receive_addr, extra_ports
        );

        let health = Arc::new(ShredstreamHealth::new());

        let hb_config = HeartbeatConfig {
            block_engine_url,
            auth_url,
            auth_keypair,
            regions,
            tvu_addr: tvu_receive_addr,
            extra_ports,
        };

        let manager_exit = exit.clone();
        let manager_health = health.clone();

        let manager_handle = Builder::new()
            .name("ssManager".to_string())
            .spawn(move || {
                service_manager(hb_config, manager_exit, manager_health);
            })
            .map_err(|e| ShredstreamError::Config(
                format!("failed to spawn manager thread: {e}")
            ))?;

        Ok(Self {
            manager_handle,
            exit,
            health,
        })
    }

    /// Get the health tracker for this service
    pub fn health(&self) -> &Arc<ShredstreamHealth> {
        &self.health
    }

    /// Check if the shredstream service is healthy
    pub fn is_healthy(&self) -> bool {
        self.health.is_healthy()
    }

    /// Signal the service to stop and wait for it to finish
    pub fn join(self) -> std::thread::Result<()> {
        self.exit.store(true, Ordering::Relaxed);
        self.manager_handle.join()
    }
}

/// Service manager loop: spawns heartbeat threads, monitors them,
/// listens for restart commands on a local UDP socket, and respawns
/// on request or when threads die.
fn service_manager(
    config: HeartbeatConfig,
    exit: Arc<AtomicBool>,
    health: Arc<ShredstreamHealth>,
) {
    // Bind local restart listener (localhost only)
    let restart_socket = match UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], RESTART_LISTENER_PORT))) {
        Ok(s) => {
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            ss_info!(
                "Shredstream: restart listener active on 127.0.0.1:{} (send \"restart\" to restart)",
                RESTART_LISTENER_PORT
            );
            Some(s)
        }
        Err(e) => {
            ss_warn!(
                "Shredstream: failed to bind restart listener on port {}: {} (restart via UDP disabled)",
                RESTART_LISTENER_PORT, e
            );
            None
        }
    };

    let mut last_restart = Instant::now() - RESTART_COOLDOWN; // allow immediate first start

    while !exit.load(Ordering::Relaxed) {
        // Spawn heartbeat threads
        ss_info!("Shredstream: spawning heartbeat threads");
        let state = match HeartbeatState::spawn(&config, &exit, &health) {
            Ok(s) => s,
            Err(e) => {
                ss_warn!("Shredstream: failed to spawn heartbeat threads: {e}, retrying in 10s");
                health.set_connected(false);
                // Sleep in short intervals so we can observe exit
                for _ in 0..50 {
                    if exit.load(Ordering::Relaxed) { break; }
                    std::thread::sleep(Duration::from_millis(200));
                }
                continue;
            }
        };

        // Wait for restart command, thread death, or exit
        let poll_result = poll_for_event(restart_socket.as_ref(), &exit, &state);

        let reason = match &poll_result {
            PollResult::Restart => " (restart requested)",
            PollResult::ThreadDied => " (heartbeat thread died, auto-restarting)",
            PollResult::Exit => "",
        };
        ss_info!("Shredstream: stopping heartbeat threads{}", reason);
        state.stop();

        if matches!(poll_result, PollResult::Restart | PollResult::ThreadDied) {
            // Rate-limit restarts
            let since_last = last_restart.elapsed();
            if since_last < RESTART_COOLDOWN {
                let wait = RESTART_COOLDOWN - since_last;
                ss_info!("Shredstream: restart cooldown, waiting {:?}", wait);
                // Sleep in short intervals so we can observe exit
                let deadline = Instant::now() + wait;
                while Instant::now() < deadline && !exit.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(200));
                }
            }

            if !exit.load(Ordering::Relaxed) {
                // Drain any queued restart commands to prevent cascading restarts
                if let Some(ref socket) = restart_socket {
                    drain_socket(socket);
                }
                last_restart = Instant::now();
                ss_info!("Shredstream: restarting heartbeat threads");
                health.set_connected(false);
            }
        }
    }

    ss_info!("Shredstream: service manager exited");
}

/// Poll for restart command, thread death, or exit signal.
fn poll_for_event(
    socket: Option<&UdpSocket>,
    exit: &Arc<AtomicBool>,
    state: &HeartbeatState,
) -> PollResult {
    let mut buf = [0u8; 64];
    while !exit.load(Ordering::Relaxed) {
        // Check if any heartbeat thread has died
        if state.any_thread_dead() {
            ss_warn!("Shredstream: detected dead heartbeat thread");
            return PollResult::ThreadDied;
        }

        // Check for restart command on UDP socket
        if let Some(socket) = socket {
            match socket.recv_from(&mut buf) {
                Ok((n, src)) => {
                    let cmd = std::str::from_utf8(&buf[..n])
                        .unwrap_or("")
                        .trim();
                    if cmd == "restart" {
                        ss_info!("Shredstream: restart command received from {}", src);
                        return PollResult::Restart;
                    }
                }
                Err(ref e)
                    if e.kind() == ErrorKind::WouldBlock
                        || e.kind() == ErrorKind::TimedOut =>
                {
                    // Normal timeout — loop and recheck
                }
                Err(e) => {
                    ss_warn!("Shredstream: restart listener error: {}", e);
                    // Sleep to avoid spin loop on persistent errors
                    std::thread::sleep(Duration::from_secs(2));
                }
            }
        } else {
            // No restart socket — just poll exit and thread health
            std::thread::sleep(Duration::from_secs(2));
        }
    }
    PollResult::Exit
}

/// Drain any pending data from the socket to prevent cascading restarts.
/// Note: `set_nonblocking` and `set_read_timeout` are independent on Linux
/// (O_NONBLOCK via fcntl vs SO_RCVTIMEO via setsockopt), so we must
/// explicitly clear nonblocking mode after draining.
fn drain_socket(socket: &UdpSocket) {
    let mut buf = [0u8; 64];
    let _ = socket.set_nonblocking(true);
    while socket.recv_from(&mut buf).is_ok() {}
    let _ = socket.set_nonblocking(false);
    let _ = socket.set_read_timeout(Some(Duration::from_secs(2)));
}
