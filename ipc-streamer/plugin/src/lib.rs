use agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use fastbloom::AtomicBloomFilter;
use gxhash::GxBuildHasher;
use log::info;
use shm_accounts_shared::{ShmWriter, shm_unlink_if_exists};
use solana_pubkey::Pubkey;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::str::FromStr;
use arc_swap::ArcSwap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const FILTER_FALSE_POS_RATE: f64 = 0.00001;
const FILTER_EXPECTED_ITEMS: usize = 20000;
const SHM_PATH_DEFAULT: &str = "/moufang_accounts";
const SHM_CAPACITY_DEFAULT: usize = 512 * 1024 * 1024;

// ===============================
// Monotonic timestamp (µs)
// ===============================
#[inline(always)]
fn mono_us() -> u64 {
    #[cfg(target_os = "linux")]
    {
        use libc::{CLOCK_MONOTONIC_RAW, clock_gettime, timespec};
        let mut ts = timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        unsafe {
            clock_gettime(CLOCK_MONOTONIC_RAW, &mut ts);
        }
        (ts.tv_sec as u64) * 1_000_000 + (ts.tv_nsec as u64) / 1_000
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}

// ===============================
// SHM Reset helper
// ===============================
fn reset_shm(
    shm_writer: &Arc<ArcSwap<ShmWriter>>,
    shm_path: &str,
    shm_capacity: usize,
) -> Result<(), String> {
    info!("Resetting SHM queue at {}", shm_path);

    shm_unlink_if_exists(shm_path).map_err(|e| format!("Failed to unlink old SHM: {}", e))?;

    let new_writer = ShmWriter::create(shm_path, shm_capacity)
        .map_err(|e| format!("Failed to create new SHM: {}", e))?;

    shm_writer.store(Arc::new(new_writer));

    info!("SHM queue reset complete");
    Ok(())
}

// ===============================
// Filter server
// ===============================
fn run_filter_server(
    port: u16,
    filter: Arc<AtomicBloomFilter<GxBuildHasher>>,
    write_all: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    shm_writer: Arc<ArcSwap<ShmWriter>>,
    shm_path: String,
    shm_capacity: usize,
) {
    let listener =
        TcpListener::bind(("127.0.0.1", port)).expect("bind 127.0.0.1:port for filter server");
    listener
        .set_nonblocking(true)
        .expect("set_nonblocking(true)");
    info!("filter server listening on 127.0.0.1:{port}");

    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((mut stream, _addr)) => {
                if let Err(e) = handle_conn(
                    &mut stream,
                    &filter,
                    &write_all,
                    &shm_writer,
                    &shm_path,
                    shm_capacity,
                ) {
                    let _ = writeln_safely(&mut stream, &format!("ERR {e}\n"));
                }
            }
            Err(e) => {
                if e.kind() != std::io::ErrorKind::WouldBlock {
                    // optional: log
                }
                thread::sleep(Duration::from_millis(25));
            }
        }
    }
}

fn handle_conn(
    stream: &mut TcpStream,
    filter: &Arc<AtomicBloomFilter<GxBuildHasher>>,
    write_all: &Arc<AtomicBool>,
    shm_writer: &Arc<ArcSwap<ShmWriter>>,
    shm_path: &str,
    shm_capacity: usize,
) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(64 * 1024);
    stream.read_to_end(&mut buf)?;
    let body = String::from_utf8_lossy(&buf);
    let trimmed = body.trim();

    // 1. Write All override
    if trimmed == "*" {
        write_all.store(true, Ordering::Relaxed);
        writeln_safely(stream, "OK ALL (writing all account updates)\n")?;
        return Ok(());
    }

    // 2. Clear Filter (New Command)
    // Since we are append-only by default now, we need an explicit way to clear if desired.
    if trimmed.eq_ignore_ascii_case("CLEAR") {
        filter.clear();
        writeln_safely(stream, "OK CLEARED\n")?;
        return Ok(());
    }

    // 3. Reset SHM
    if trimmed.eq_ignore_ascii_case("RESET") {
        match reset_shm(shm_writer, shm_path, shm_capacity) {
            Ok(_) => {
                filter.clear();
                writeln_safely(stream, "OK RESET (SHM queue and Filter reinitialized)\n")?;
            }
            Err(e) => {
                writeln_safely(stream, &format!("ERR Failed to reset: {e}\n"))?;
            }
        }
        return Ok(());
    }

    write_all.store(false, Ordering::Relaxed);

    let res = if trimmed.starts_with('[') {
        parse_json_array(&body, filter)
    } else {
        parse_whitespace_list(&body, filter)
    };

    match res {
        Ok(count) => {
            writeln_safely(stream, &format!("OK ADDED {}\n", count))?;
        }
        Err(e) => {
            writeln_safely(stream, &format!("ERR {e}\n"))?;
        }
    }

    Ok(())
}

fn writeln_safely(stream: &mut TcpStream, s: &str) -> std::io::Result<()> {
    let _ = stream.write_all(s.as_bytes());
    let _ = stream.flush();
    Ok(())
}

fn parse_json_array(
    body: &str,
    filter: &Arc<AtomicBloomFilter<GxBuildHasher>>,
) -> Result<usize, String> {
    let vals: Vec<String> =
        serde_json::from_str(body).map_err(|e| format!("invalid JSON array: {e}"))?;
    parse_keys(vals.into_iter(), filter)
}

fn parse_whitespace_list(
    body: &str,
    filter: &Arc<AtomicBloomFilter<GxBuildHasher>>,
) -> Result<usize, String> {
    let vals = body.split_whitespace().map(|s| s.to_string());
    parse_keys(vals, filter)
}

fn parse_keys<I>(iter: I, filter: &Arc<AtomicBloomFilter<GxBuildHasher>>) -> Result<usize, String>
where
    I: IntoIterator<Item = String>,
{
    let mut count = 0;
    for s in iter {
        if s.is_empty() {
            continue;
        }
        let pk = Pubkey::from_str(&s).map_err(|e| format!("bad pubkey '{s}': {e}"))?;
        filter.insert(pk.as_array());
        count += 1;
    }
    Ok(count)
}

// ===============================
// Plugin
// ===============================
pub struct ShmPlugin {
    filter: Arc<AtomicBloomFilter<GxBuildHasher>>,
    write_all: Arc<AtomicBool>,
    server_handle: Mutex<Option<thread::JoinHandle<()>>>,
    shutdown_flag: Arc<AtomicBool>,
    shm_writer: Arc<ArcSwap<ShmWriter>>,
    shm_path: String,
    shm_capacity: usize,
}

impl std::fmt::Debug for ShmPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShmPlugin").finish()
    }
}

impl ShmPlugin {
    pub fn new() -> Self {
        let shm_path = SHM_PATH_DEFAULT.to_string();
        let shm_capacity = SHM_CAPACITY_DEFAULT;

        let writer = ShmWriter::create(&shm_path, shm_capacity)
            .map_err(|e| GeyserPluginError::Custom(Box::new(e)))
            .unwrap();
        let shm_writer = Arc::new(ArcSwap::from_pointee(writer));

        // Create the single filter instance
        let filter = Arc::new(
            AtomicBloomFilter::with_false_pos(FILTER_FALSE_POS_RATE)
                .hasher(GxBuildHasher::default())
                .expected_items(FILTER_EXPECTED_ITEMS),
        );

        let write_all = Arc::new(AtomicBool::new(false));
        let shutdown_flag = Arc::new(AtomicBool::new(false));

        let filter_clone = Arc::clone(&filter);
        let shutdown_clone = Arc::clone(&shutdown_flag);
        let write_all_clone = Arc::clone(&write_all);
        let shm_writer_clone = Arc::clone(&shm_writer);
        let shm_path_clone = shm_path.clone();

        let thr = thread::Builder::new()
            .name(format!("geyser-filter-5681"))
            .spawn(move || {
                run_filter_server(
                    5681,
                    filter_clone,
                    write_all_clone,
                    shutdown_clone,
                    shm_writer_clone,
                    shm_path_clone,
                    shm_capacity,
                )
            })
            .map_err(|e| GeyserPluginError::Custom(Box::new(e)))
            .unwrap();

        info!("filter server enabled on 127.0.0.1:5681");

        ShmPlugin {
            filter,
            write_all,
            server_handle: Mutex::new(Some(thr)),
            shutdown_flag,
            shm_writer,
            shm_path,
            shm_capacity,

        }
    }

    #[inline(always)]
    pub fn filtered_write(&self, pubkey: &[u8; 32], data: &[u8], trigger_sig: &[u8; 64]) -> bool {
        if self.write_all.load(Ordering::Relaxed) {
            self.enqueue(pubkey, data, trigger_sig);
            return true;
        }

        if self.filter.contains(pubkey) {
            self.enqueue(pubkey, data, trigger_sig);
            return true;
        }

        false
    }

    /// Call after all account updates for a transaction have been written.
    /// Always emits the end marker unconditionally — the reader silently
    /// ignores markers for which it has no buffered state. This avoids a
    /// race condition where concurrent banking threads could consume each
    /// other's `tx_had_filtered_write` flag.
    #[inline(always)]
    pub fn flush_tx_end_marker(&self, trigger_sig: &[u8; 64]) {
        let writer = self.shm_writer.load();
        loop {
            match writer.push_tx_end_marker(trigger_sig) {
                Ok(_) => break,
                Err(e) => {
                    eprintln!("[SHM-PLUGIN] err {:#?}", e);
                    thread::yield_now();
                }
            }
        }
    }

    #[inline(always)]
    fn enqueue(&self, pubkey: &[u8; 32], data: &[u8], trigger_sig: &[u8; 64]) {
        let writer = self.shm_writer.load();
        loop {
            let now = mono_us();
            match writer.push(pubkey, data, now, trigger_sig) {
                Ok(_) => break,
                Err(_) => {
                    thread::yield_now();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::TcpStream;
    use std::thread;
    use std::time::Duration;

    fn send_command(port: u16, command: &str) -> String {
        let mut stream =
            TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect");
        stream
            .write_all(command.as_bytes())
            .expect("Failed to write");
        stream
            .shutdown(std::net::Shutdown::Write)
            .expect("Failed to shutdown write");

        let mut response = String::new();
        std::io::Read::read_to_string(&mut stream, &mut response).expect("Failed to read response");
        response
    }

    #[test]
    fn test_reset_shm() {
        // Clean up any existing shared memory
        let _ = shm_unlink_if_exists("/test_reset_shm");

        // Create a plugin with a test SHM path
        let shm_path = "/test_reset_shm";
        let shm_capacity = 1024 * 1024;

        let writer = ShmWriter::create(shm_path, shm_capacity).unwrap();
        let shm_writer = Arc::new(ArcSwap::from_pointee(writer));

        // Write some data to the initial writer
        {
            let writer = shm_writer.load();
            let pk = [1u8; 32];
            let data = b"initial data";
            let _ = writer.push(&pk, data, mono_us(), 0);
        }

        // Reset the SHM
        let result = reset_shm(&shm_writer, shm_path, shm_capacity);
        assert!(result.is_ok(), "Reset should succeed: {:?}", result);

        // Verify we can still write after reset
        {
            let writer = shm_writer.load();
            let pk = [2u8; 32];
            let data = b"post-reset data";
            let result = writer.push(&pk, data, mono_us(), 0);
            assert!(result.is_ok(), "Should be able to write after reset");
        }

        // Clean up
        let _ = shm_unlink_if_exists(shm_path);
    }

    #[test]
    fn test_concurrent_writes_during_reset() {
        let _ = shm_unlink_if_exists("/test_concurrent_reset");

        let shm_path = "/test_concurrent_reset";
        let shm_capacity = 4 * 1024 * 1024;

        let writer = ShmWriter::create(shm_path, shm_capacity).unwrap();
        let shm_writer = Arc::new(ArcSwap::from_pointee(writer));

        let num_writers = 4;
        let writes_per_thread = 100;
        let mut handles = vec![];

        // Start writer threads
        for i in 0..num_writers {
            let shm_writer_clone = Arc::clone(&shm_writer);
            let handle = thread::spawn(move || {
                for j in 0..writes_per_thread {
                    let writer = shm_writer_clone.load();

                    let pk = [(i * 100 + j) as u8; 32];
                    let data = format!("thread-{}-write-{}", i, j);

                    loop {
                        match writer.push(&pk, data.as_bytes(), mono_us(), 0) {
                            Ok(_) => break,
                            Err(_) => thread::yield_now(),
                        }
                    }

                    // Small delay to simulate realistic workload
                    thread::sleep(Duration::from_micros(10));
                }
            });
            handles.push(handle);
        }

        // Perform reset while writers are active
        thread::sleep(Duration::from_millis(50));
        let result = reset_shm(&shm_writer, shm_path, shm_capacity);
        assert!(
            result.is_ok(),
            "Reset should succeed even with concurrent writes"
        );

        // Wait for all writers to complete
        for handle in handles {
            handle.join().expect("Writer thread should complete");
        }

        // Verify we can still write after all operations
        {
            let writer = shm_writer.load();
            let pk = [255u8; 32];
            let data = b"final verification";
            let result = writer.push(&pk, data, mono_us(), 0);
            assert!(
                result.is_ok(),
                "Should be able to write after concurrent reset"
            );
        }

        let _ = shm_unlink_if_exists(shm_path);
    }

    #[test]
    fn test_reset_command_via_server() {
        let _ = shm_unlink_if_exists("/test_server_reset");

        let shm_path = "/test_server_reset".to_string();
        let shm_capacity = 1024 * 1024;
        let port = 5682; // Different port to avoid conflicts

        let writer = ShmWriter::create(&shm_path, shm_capacity).unwrap();
        let shm_writer = Arc::new(ArcSwap::from_pointee(writer));

        let filter = Arc::new(
            AtomicBloomFilter::with_false_pos(0.00001)
                .hasher(GxBuildHasher::default())
                .expected_items(20000),
        );
        let write_all = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));

        let filter_clone = Arc::clone(&filter);
        let write_all_clone = Arc::clone(&write_all);
        let shutdown_clone = Arc::clone(&shutdown);
        let shm_writer_clone = Arc::clone(&shm_writer);
        let shm_path_clone = shm_path.clone();

        // Start server
        let _server_handle = thread::spawn(move || {
            run_filter_server(
                port,
                filter_clone,
                write_all_clone,
                shutdown_clone,
                shm_writer_clone,
                shm_path_clone,
                shm_capacity,
            );
        });

        // Give server time to start
        thread::sleep(Duration::from_millis(100));

        // Send RESET command
        let response = send_command(port, "RESET");
        assert!(
            response.contains("OK RESET"),
            "Should get OK RESET response, got: {}",
            response
        );

        // Verify we can still write after reset
        {
            let writer = shm_writer.load();
            let pk = [42u8; 32];
            let data = b"post-server-reset";
            let result = writer.push(&pk, data, mono_us(), 0);
            assert!(result.is_ok(), "Should be able to write after server reset");
        }

        // Shutdown server
        shutdown.store(true, Ordering::Relaxed);
        thread::sleep(Duration::from_millis(100));

        let _ = shm_unlink_if_exists(&shm_path);
    }

    #[test]
    fn test_filtered_write_survives_reset() {
        let _ = shm_unlink_if_exists("/test_filtered_write_reset");

        let shm_path = "/test_filtered_write_reset".to_string();
        let shm_capacity = 2 * 1024 * 1024;

        let writer = ShmWriter::create(&shm_path, shm_capacity).unwrap();
        let shm_writer = Arc::new(ArcSwap::from_pointee(writer));

        let filter = Arc::new(
            AtomicBloomFilter::with_false_pos(0.00001)
                .hasher(GxBuildHasher::default())
                .expected_items(20000),
        );

        let plugin = ShmPlugin {
            filter: Arc::clone(&filter),
            write_all: Arc::new(AtomicBool::new(false)),
            server_handle: Mutex::new(None),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            shm_writer: Arc::clone(&shm_writer),
            shm_path: shm_path.clone(),
            shm_capacity,

        };

        // Add a key to the filter
        let test_pk = [7u8; 32];
        plugin.filter.insert(&test_pk);

        // Write before reset
        plugin.filtered_write(&test_pk, b"before reset", &[0u8; 64]);

        // Perform reset
        let result = reset_shm(&shm_writer, &shm_path, shm_capacity);
        assert!(result.is_ok(), "Reset should succeed");

        // Write after reset - should still work
        plugin.filtered_write(&test_pk, b"after reset", &[0u8; 64]);

        // Verify the filter still works
        assert!(
            plugin.filter.contains(&test_pk),
            "Filter should still contain the key"
        );

        let _ = shm_unlink_if_exists(&shm_path);
    }

    #[test]
    #[ignore] // Run with: cargo test bench_push_latency -- --ignored --nocapture
    fn bench_push_latency() {
        let _ = shm_unlink_if_exists("/bench_push_latency");

        let shm_path = "/bench_push_latency";
        let shm_capacity = 512 * 1024 * 1024;

        let writer = ShmWriter::create(shm_path, shm_capacity).unwrap();
        let shm_writer = Arc::new(ArcSwap::from_pointee(writer));

        let filter = Arc::new(
            AtomicBloomFilter::with_false_pos(0.00001)
                .hasher(GxBuildHasher::default())
                .expected_items(20000),
        );

        let plugin = ShmPlugin {
            filter: Arc::clone(&filter),
            write_all: Arc::new(AtomicBool::new(false)),
            server_handle: Mutex::new(None),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            shm_writer: Arc::clone(&shm_writer),
            shm_path: shm_path.to_string(),
            shm_capacity,

        };

        // Add test keys to filter
        let test_pks: Vec<[u8; 32]> = (0..1000)
            .map(|i| {
                let mut pk = [0u8; 32];
                pk[0] = (i & 0xFF) as u8;
                pk[1] = ((i >> 8) & 0xFF) as u8;
                plugin.filter.insert(&pk);
                pk
            })
            .collect();

        // Verify filter is working
        assert!(
            plugin.filter.contains(&test_pks[0]),
            "Filter should contain test keys"
        );
        println!("Filter verification passed - keys are in filter");

        // Warmup
        for i in 0..1000 {
            let data = format!("warmup data {}", i);
            plugin.filtered_write(&test_pks[i % test_pks.len()], data.as_bytes(), &[0u8; 64]);
        }

        // Benchmark: measure individual push operations
        const NUM_ITERATIONS: usize = 100_000;
        let mut latencies = Vec::with_capacity(NUM_ITERATIONS);

        for i in 0..NUM_ITERATIONS {
            let pk = &test_pks[i % test_pks.len()];
            let data = format!("benchmark data {}", i);

            let start = std::time::Instant::now();
            plugin.filtered_write(pk, data.as_bytes(), &[0u8; 64]);
            let elapsed = start.elapsed().as_nanos() as u64;

            latencies.push(elapsed);
        }

        // Calculate statistics
        latencies.sort_unstable();
        let min = latencies[0];
        let max = latencies[NUM_ITERATIONS - 1];
        let median = latencies[NUM_ITERATIONS / 2];
        let p95 = latencies[(NUM_ITERATIONS as f64 * 0.95) as usize];
        let p99 = latencies[(NUM_ITERATIONS as f64 * 0.99) as usize];
        let avg: u64 = latencies.iter().sum::<u64>() / NUM_ITERATIONS as u64;

        println!("\n=== Push Latency Benchmark Results ===");
        println!("Total operations: {}", NUM_ITERATIONS);
        println!("Min latency:      {} ns", min);
        println!("Avg latency:      {} ns", avg);
        println!("Median latency:   {} ns", median);
        println!("P95 latency:      {} ns", p95);
        println!("P99 latency:      {} ns", p99);
        println!("Max latency:      {} ns", max);
        println!("======================================\n");

        let _ = shm_unlink_if_exists(shm_path);
    }

    #[test]
    fn test_reset_clears_filter() {
        let _ = shm_unlink_if_exists("/test_reset_clears_filter");

        let shm_path = "/test_reset_clears_filter".to_string();
        let shm_capacity = 1024 * 1024;
        let port = 5683; // Different port

        let writer = ShmWriter::create(&shm_path, shm_capacity).unwrap();
        let shm_writer = Arc::new(ArcSwap::from_pointee(writer));

        let filter = Arc::new(
            AtomicBloomFilter::with_false_pos(0.00001)
                .hasher(GxBuildHasher::default())
                .expected_items(20000),
        );
        let write_all = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));

        let filter_clone = Arc::clone(&filter);
        let write_all_clone = Arc::clone(&write_all);
        let shutdown_clone = Arc::clone(&shutdown);
        let shm_writer_clone = Arc::clone(&shm_writer);
        let shm_path_clone = shm_path.clone();

        // Start server
        let _server_handle = thread::spawn(move || {
            run_filter_server(
                port,
                filter_clone,
                write_all_clone,
                shutdown_clone,
                shm_writer_clone,
                shm_path_clone,
                shm_capacity,
            );
        });

        thread::sleep(Duration::from_millis(100));

        // Insert a key directly
        let test_pk = [88u8; 32];
        filter.insert(&test_pk);
        assert!(filter.contains(&test_pk), "Filter should contain key initially");

        // Send RESET command
        let response = send_command(port, "RESET");
        assert!(response.contains("OK RESET"), "Should get OK RESET");

        // Verify filter is cleared
        assert!(!filter.contains(&test_pk), "Filter should be empty after RESET");

        // Shutdown
        shutdown.store(true, Ordering::Relaxed);
        thread::sleep(Duration::from_millis(100));
        let _ = shm_unlink_if_exists(&shm_path);
    }
}
