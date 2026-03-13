//! Multi-heartbeat manager for Jito shredstream
//!
//! Registers supplementary ports with Jito's block engine using a SINGLE
//! gRPC connection that round-robins heartbeats across all ports. This
//! avoids the per-identity rate limit (20 req/min) that would be exceeded
//! by N independent connections each with their own auth overhead.
//!
//! The actual UDP sockets for those ports are added directly to the TVU's
//! fetch_sockets before ShredFetchStage is created (in validator.rs), so
//! shreds arrive through the normal pipeline with correct source IPs and
//! full deduplication — no loopback forwarding needed.

use {
    super::{
        config::{
            total_expected_relays, MULTI_HEARTBEAT_MAX_CONNECTIONS,
            MULTI_HEARTBEAT_PER_PORT_INTERVAL_SECS, MULTI_HEARTBEAT_PORT_START,
        },
        file_log::{ss_info, ss_warn},
        heartbeat::get_grpc_client,
        protos::{shared::Socket, shredstream::Heartbeat},
    },
    solana_keypair::Keypair,
    std::{
        net::{IpAddr, SocketAddr, UdpSocket},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::{Builder, JoinHandle},
        time::Duration,
    },
    tokio::runtime::Runtime,
    tonic::Code,
};

/// Create UDP sockets for supplementary multi-heartbeat ports.
/// These sockets are added to the TVU's fetch_sockets so ShredFetchStage
/// processes them directly — preserving source IPs and using native dedup.
pub fn create_multi_heartbeat_sockets(regions: &[String]) -> (Vec<UdpSocket>, Vec<u16>) {
    super::file_log::init(None);
    let expected = total_expected_relays(regions);
    let count = (expected as u16).min(MULTI_HEARTBEAT_MAX_CONNECTIONS);

    if count == 0 {
        return (Vec::new(), Vec::new());
    }

    let mut sockets = Vec::with_capacity(count as usize);
    let mut ports = Vec::with_capacity(count as usize);

    for i in 0..count {
        let port = MULTI_HEARTBEAT_PORT_START + i;
        match UdpSocket::bind(SocketAddr::new(
            IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
            port,
        )) {
            Ok(socket) => {
                let _ = socket.set_nonblocking(true);
                ss_info!("Shredstream multi-heartbeat: bound socket on port {}", port);
                sockets.push(socket);
                ports.push(port);
            }
            Err(e) => {
                ss_info!(
                    "Shredstream multi-heartbeat: failed to bind port {}: {} (skipping)",
                    port, e
                );
            }
        }
    }

    ss_info!(
        "Shredstream multi-heartbeat: created {} sockets on ports {:?} \
         (expecting {} relay IPs for regions {:?})",
        sockets.len(),
        ports,
        expected,
        regions
    );

    (sockets, ports)
}

/// Start the multi-heartbeat manager thread.
///
/// Uses a SINGLE gRPC connection and round-robins heartbeats across all
/// extra ports. This keeps auth overhead to 1 session and heartbeat rate
/// to 1 request per interval regardless of port count.
#[allow(clippy::too_many_arguments)]
pub fn start_multi_heartbeat_manager(
    public_ip: IpAddr,
    extra_ports: Vec<u16>,
    block_engine_url: String,
    auth_url: String,
    auth_keypair: Arc<Keypair>,
    desired_regions: Vec<String>,
    shutdown_receiver: crossbeam_channel::Receiver<()>,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    Builder::new()
        .name("ssMultiHbMgr".to_string())
        .spawn(move || {
            let runtime = Runtime::new()
                .expect("Failed to create tokio runtime for multi-heartbeat");

            let sockets: Vec<Socket> = extra_ports
                .iter()
                .map(|&port| Socket {
                    ip: public_ip.to_string(),
                    port: port as i64,
                })
                .collect();

            ss_info!(
                "Shredstream multi-heartbeat: manager started, {} ports to register",
                sockets.len()
            );

            // Each port must be heartbeated every PER_PORT_INTERVAL to stay
            // registered (must be <= Jito TTL / 3). We round-robin, so the
            // tick interval = per_port_interval / N_ports.
            // E.g. 3 ports at 40s each = 1 heartbeat every ~13s = 4.5 req/min.
            let per_port_interval = Duration::from_secs(MULTI_HEARTBEAT_PER_PORT_INTERVAL_SECS);
            let tick_interval = per_port_interval
                .checked_div(sockets.len() as u32)
                .unwrap_or(per_port_interval);
            let heartbeat_tick = crossbeam_channel::tick(tick_interval);

            // Initial delay to let primary heartbeat settle
            let startup_delay = crossbeam_channel::tick(Duration::from_secs(15));
            crossbeam_channel::select! {
                recv(startup_delay) -> _ => {}
                recv(shutdown_receiver) -> _ => { return; }
            }

            let mut port_idx = 0;
            let mut backoff_until: Option<std::time::Instant> = None;
            let mut reconnect_backoff = Duration::from_secs(5);
            const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(60);

            // Connect loop — reconnects on failure with exponential backoff
            while !exit.load(Ordering::Relaxed) {
                ss_info!(
                    "Shredstream multi-heartbeat: connecting to {}",
                    block_engine_url
                );

                let client_exit = Arc::new(AtomicBool::new(false));
                let client_res = runtime.block_on(get_grpc_client(
                    block_engine_url.clone(),
                    auth_url.clone(),
                    auth_keypair.clone(),
                    "shredstream_multi".to_string(),
                    client_exit.clone(),
                ));

                let (mut client, refresh_handle) = match client_res {
                    Ok(c) => c,
                    Err(e) => {
                        ss_warn!(
                            "Shredstream multi-heartbeat: connect failed: {e}, retrying in {:?}",
                            reconnect_backoff
                        );
                        let retry_tick = crossbeam_channel::tick(reconnect_backoff);
                        reconnect_backoff = (reconnect_backoff * 2).min(MAX_RECONNECT_BACKOFF);
                        crossbeam_channel::select! {
                            recv(retry_tick) -> _ => { continue; }
                            recv(shutdown_receiver) -> _ => { return; }
                        }
                    }
                };

                ss_info!(
                    "Shredstream multi-heartbeat: connected, rotating across {} ports every {:?}",
                    sockets.len(),
                    tick_interval
                );

                let mut consecutive_failures = 0u32;
                const MAX_CONSECUTIVE_FAILURES: u32 = 3;

                // Inner heartbeat loop — round-robin across ports
                while !exit.load(Ordering::Relaxed) {
                    crossbeam_channel::select! {
                        recv(heartbeat_tick) -> _ => {
                            // Respect rate-limit backoff
                            if let Some(until) = backoff_until {
                                if std::time::Instant::now() < until {
                                    continue;
                                }
                                backoff_until = None;
                            }

                            let socket = &sockets[port_idx % sockets.len()];
                            port_idx = port_idx.wrapping_add(1);

                            let result = runtime.block_on(client.send_heartbeat(Heartbeat {
                                socket: Some(socket.clone()),
                                regions: desired_regions.clone(),
                            }));

                            match result {
                                Ok(_) => {
                                    consecutive_failures = 0;
                                    reconnect_backoff = Duration::from_secs(5);
                                }
                                Err(err) if err.code() == Code::ResourceExhausted => {
                                    let wait_ms = err
                                        .metadata()
                                        .get("x-wait-to-retry-ms")
                                        .and_then(|v| v.to_str().ok())
                                        .and_then(|v| v.parse::<u64>().ok())
                                        .unwrap_or(60_000);
                                    ss_warn!(
                                        "Shredstream multi-heartbeat: rate limited, backing off {}s",
                                        wait_ms / 1000
                                    );
                                    backoff_until = Some(
                                        std::time::Instant::now()
                                            + Duration::from_millis(wait_ms),
                                    );
                                }
                                Err(err) => {
                                    consecutive_failures += 1;
                                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                                        ss_warn!(
                                            "Shredstream multi-heartbeat: {} consecutive failures \
                                             (last: {err}), reconnecting in {:?}",
                                            consecutive_failures, reconnect_backoff
                                        );
                                        break; // reconnect with backoff
                                    }
                                    ss_warn!(
                                        "Shredstream multi-heartbeat: heartbeat error: {err} \
                                         ({}/{})", consecutive_failures, MAX_CONSECUTIVE_FAILURES
                                    );
                                }
                            }
                        }

                        recv(shutdown_receiver) -> _ => {
                            refresh_handle.abort();
                            client_exit.store(true, Ordering::Relaxed);
                            ss_info!("Shredstream multi-heartbeat: manager exited");
                            return;
                        }
                    }
                }

                // Abort token refresh task before reconnecting.
                // Dropping a tokio JoinHandle does NOT abort the task.
                refresh_handle.abort();
                client_exit.store(true, Ordering::Relaxed);

                // Back off before reconnecting
                if !exit.load(Ordering::Relaxed) {
                    let wait_tick = crossbeam_channel::tick(reconnect_backoff);
                    crossbeam_channel::select! {
                        recv(wait_tick) -> _ => {}
                        recv(shutdown_receiver) -> _ => { return; }
                    }
                    reconnect_backoff = (reconnect_backoff * 2).min(MAX_RECONNECT_BACKOFF);
                }
            }

            ss_info!("Shredstream multi-heartbeat: manager exited");
        })
        .unwrap()
}
