//! Heartbeat loop for Jito shredstream
//!
//! Sends periodic heartbeats to the Jito block engine to register
//! the validator's TVU socket for receiving shreds.

use {
    super::{
        auth::{create_grpc_channel, ClientInterceptor},
        file_log::{ss_info, ss_warn},
        health::ShredstreamHealth,
        protos::{
            auth::{auth_service_client::AuthServiceClient, Role},
            shared::Socket,
            shredstream::{shredstream_client::ShredstreamClient, Heartbeat},
        },
        ShredstreamError,
    },
    crossbeam_channel::Receiver,
    solana_keypair::Keypair,
    solana_metrics::{datapoint_info, datapoint_warn},
    std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::{Builder, JoinHandle},
        time::Duration,
    },
    tokio::runtime::Runtime,
    tonic::{codegen::InterceptedService, transport::Channel, Code},
};

/// Wrapper around AtomicBool that sets to true on drop.
/// Used to scope the lifetime of the gRPC client to the heartbeat loop.
struct ScopedAtomicBool {
    inner: Arc<AtomicBool>,
}

impl ScopedAtomicBool {
    fn get_inner_clone(&self) -> Arc<AtomicBool> {
        self.inner.clone()
    }
}

impl Default for ScopedAtomicBool {
    fn default() -> Self {
        Self {
            inner: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl Drop for ScopedAtomicBool {
    fn drop(&mut self) {
        self.inner.store(true, Ordering::Relaxed);
    }
}

/// Start the heartbeat loop thread.
/// Sends heartbeats to Jito block engine to register the TVU socket.
#[allow(clippy::too_many_arguments)]
pub fn heartbeat_loop_thread(
    block_engine_url: String,
    auth_url: String,
    auth_keypair: Arc<Keypair>,
    desired_regions: Vec<String>,
    recv_socket: SocketAddr,
    runtime: Runtime,
    service_name: String,
    shutdown_receiver: Receiver<()>,
    exit: Arc<AtomicBool>,
    health: Arc<ShredstreamHealth>,
    initial_interval: Option<Duration>,
) -> JoinHandle<()> {
    Builder::new()
        .name("shredstreamHbeat".to_string())
        .spawn(move || {
            let heartbeat_socket = Socket {
                ip: recv_socket.ip().to_string(),
                port: recv_socket.port() as i64,
            };
            // Primary heartbeat starts at 1s and adjusts from server TTL.
            // Supplementary heartbeats should start at a known-good interval
            // to avoid blowing through Jito's per-identity rate limit (20/min).
            let mut heartbeat_interval = initial_interval.unwrap_or(Duration::from_secs(1));
            let mut heartbeat_tick = crossbeam_channel::tick(heartbeat_interval);
            let metrics_tick = crossbeam_channel::tick(Duration::from_secs(30));
            let mut client_restart_count = 0u64;
            let mut successful_heartbeat_count = 0u64;
            let mut failed_heartbeat_count = 0u64;
            let mut client_restart_count_cumulative = 0u64;
            let mut successful_heartbeat_count_cumulative = 0u64;
            let mut failed_heartbeat_count_cumulative = 0u64;
            let mut reconnect_backoff = Duration::from_secs(5);
            const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(60);

            while !exit.load(Ordering::Relaxed) {
                // Scope gRPC client to heartbeat loop - exits when loop breaks
                let per_con_exit = ScopedAtomicBool::default();
                ss_info!("Shredstream: starting heartbeat client to {}", block_engine_url);

                let shredstream_client_res = runtime.block_on(get_grpc_client(
                    block_engine_url.clone(),
                    auth_url.clone(),
                    auth_keypair.clone(),
                    service_name.clone(),
                    per_con_exit.get_inner_clone(),
                ));

                let (mut shredstream_client, refresh_thread_hdl) = match shredstream_client_res {
                    Ok(c) => c,
                    Err(e) => {
                        ss_warn!(
                            "Shredstream: failed to connect to block engine, retrying in {:?}. Error: {e}",
                            reconnect_backoff
                        );
                        client_restart_count += 1;
                        health.record_client_restart();
                        datapoint_warn!(
                            "shredstream-heartbeat_client_error",
                            "block_engine_url" => block_engine_url.clone(),
                            ("errors", 1, i64),
                            ("error_str", e.to_string(), String),
                        );
                        let backoff_tick = crossbeam_channel::tick(reconnect_backoff);
                        crossbeam_channel::select! {
                            recv(backoff_tick) -> _ => {}
                            recv(shutdown_receiver) -> _ => {}
                        }
                        reconnect_backoff = (reconnect_backoff * 2).min(MAX_RECONNECT_BACKOFF);
                        continue;
                    }
                };

                health.set_connected(true);
                ss_info!(
                    "Shredstream: connected to {}, registering socket {} for regions {:?}",
                    block_engine_url, recv_socket, desired_regions
                );

                let mut consecutive_failures = 0u32;
                const MAX_CONSECUTIVE_FAILURES: u32 = 5;

                // Inner heartbeat send loop
                while !exit.load(Ordering::Relaxed) {
                    crossbeam_channel::select! {
                        // Send heartbeat
                        recv(heartbeat_tick) -> _ => {
                            let heartbeat_result = runtime.block_on(shredstream_client
                                .send_heartbeat(Heartbeat {
                                    socket: Some(heartbeat_socket.clone()),
                                    regions: desired_regions.clone(),
                                }));

                            match heartbeat_result {
                                Ok(hb) => {
                                    // Update interval based on server TTL.
                                    // Clamp to [1s, 120s] to prevent busy-spin (ttl=0)
                                    // or wrap-around from negative values.
                                    let raw_ms = (hb.get_ref().ttl_ms / 3).clamp(1_000, 120_000) as u64;
                                    let new_interval = Duration::from_millis(raw_ms);
                                    if heartbeat_interval != new_interval {
                                        ss_info!("Shredstream: sending heartbeat every {new_interval:?}");
                                        heartbeat_interval = new_interval;
                                        heartbeat_tick = crossbeam_channel::tick(new_interval);
                                    }
                                    successful_heartbeat_count += 1;
                                    consecutive_failures = 0;
                                    reconnect_backoff = Duration::from_secs(5);
                                    health.record_successful_heartbeat();
                                }
                                Err(err) => {
                                    if err.code() == Code::InvalidArgument {
                                        ss_warn!("Shredstream: invalid arguments from server: {err}, reconnecting");
                                        health.set_connected(false);
                                        break;
                                    }
                                    // On rate limit, back off using the server-provided wait time
                                    // (x-wait-to-retry-ms header), or 60s as fallback.
                                    if err.code() == Code::ResourceExhausted {
                                        let wait_ms = err.metadata()
                                            .get("x-wait-to-retry-ms")
                                            .and_then(|v| v.to_str().ok())
                                            .and_then(|v| v.parse::<u64>().ok())
                                            .unwrap_or(60_000);
                                        let backoff = Duration::from_millis(wait_ms);
                                        ss_warn!(
                                            "Shredstream: rate limited on {}, backing off {backoff:?}",
                                            recv_socket
                                        );
                                        heartbeat_tick = crossbeam_channel::tick(backoff);
                                    } else {
                                        ss_warn!("Shredstream: error sending heartbeat: {err}");
                                        consecutive_failures += 1;
                                    }
                                    datapoint_warn!(
                                        "shredstream-heartbeat_send_error",
                                        "block_engine_url" => block_engine_url.clone(),
                                        ("errors", 1, i64),
                                        ("error_str", err.to_string(), String),
                                    );
                                    failed_heartbeat_count += 1;
                                    health.record_failed_heartbeat();

                                    // After too many consecutive failures, break out to
                                    // force a full gRPC reconnect with a fresh channel.
                                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                                        ss_warn!(
                                            "Shredstream: {} consecutive failures, reconnecting gRPC client",
                                            consecutive_failures
                                        );
                                        health.set_connected(false);
                                        break;
                                    }
                                }
                            }
                        }

                        // Periodic metrics
                        recv(metrics_tick) -> _ => {
                            datapoint_info!(
                                "shredstream-heartbeat_stats",
                                "block_engine_url" => block_engine_url.clone(),
                                ("successful_heartbeat_count", successful_heartbeat_count, i64),
                                ("failed_heartbeat_count", failed_heartbeat_count, i64),
                                ("client_restart_count", client_restart_count, i64),
                            );

                            successful_heartbeat_count_cumulative += successful_heartbeat_count;
                            failed_heartbeat_count_cumulative += failed_heartbeat_count;
                            client_restart_count_cumulative += client_restart_count;
                            successful_heartbeat_count = 0;
                            failed_heartbeat_count = 0;
                            client_restart_count = 0;
                        }

                        // Handle shutdown
                        recv(shutdown_receiver) -> _ => {
                            break;
                        }
                    }
                }

                // Abort the token refresh task before reconnecting or exiting.
                // Dropping a tokio JoinHandle does NOT abort the task.
                refresh_thread_hdl.abort();

                // Back off before reconnecting (exponential, capped at 60s).
                // Use select! so shutdown is still responsive during the wait.
                if !exit.load(Ordering::Relaxed) {
                    client_restart_count += 1;
                    health.record_client_restart();
                    ss_info!(
                        "Shredstream: waiting {:?} before reconnecting",
                        reconnect_backoff
                    );
                    let backoff_tick = crossbeam_channel::tick(reconnect_backoff);
                    crossbeam_channel::select! {
                        recv(backoff_tick) -> _ => {}
                        recv(shutdown_receiver) -> _ => {}
                    }
                    reconnect_backoff = (reconnect_backoff * 2).min(MAX_RECONNECT_BACKOFF);
                }
            }

            ss_info!(
                "Shredstream: exiting heartbeat thread, sent {} successful, {} failed heartbeats. \
                 Client restarted {} times.",
                successful_heartbeat_count_cumulative,
                failed_heartbeat_count_cumulative,
                client_restart_count_cumulative
            );
        })
        .unwrap()
}

/// Create an authenticated gRPC client for shredstream
pub async fn get_grpc_client(
    block_engine_url: String,
    auth_url: String,
    auth_keypair: Arc<Keypair>,
    service_name: String,
    exit: Arc<AtomicBool>,
) -> Result<
    (
        ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>,
        tokio::task::JoinHandle<()>,
    ),
    ShredstreamError,
> {
    let auth_channel = create_grpc_channel(auth_url).await?;
    let shredstream_channel = create_grpc_channel(block_engine_url).await?;
    let (client_interceptor, thread_handle) = ClientInterceptor::new(
        AuthServiceClient::new(auth_channel),
        auth_keypair,
        Role::ShredstreamSubscriber,
        service_name,
        exit,
    )
    .await?;
    let shredstream_client =
        ShredstreamClient::with_interceptor(shredstream_channel, client_interceptor);
    Ok((shredstream_client, thread_handle))
}
