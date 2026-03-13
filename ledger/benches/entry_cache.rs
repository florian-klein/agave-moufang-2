#![allow(clippy::arithmetic_side_effects)]
#![feature(test)]
extern crate solana_ledger;
extern crate test;

use {
    rand::Rng,
    solana_clock::Slot,
    solana_entry::entry::{create_ticks, Entry},
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_ledger::{
        blockstore::{entries_to_test_shreds, Blockstore},
        entry_cache::EntryCache,
        get_tmp_ledger_path_auto_delete,
    },
    solana_pubkey::Pubkey,
    solana_transaction::Transaction,
    std::time::Instant,
    test::Bencher,
};

// ============================================================================
// Entry Creation Utilities
// ============================================================================

/// Create simple tick entries (minimal data, fast creation)
fn create_tick_entries(num_entries: usize) -> Vec<Entry> {
    create_ticks(num_entries as u64, 0, Hash::default())
}

/// Create entries with realistic transactions (closer to real validator data)
fn create_realistic_entries(num_entries: usize, txs_per_entry: usize) -> Vec<Entry> {
    let mut rng = rand::rng();
    (0..num_entries)
        .map(|_| {
            let transactions: Vec<Transaction> = (0..txs_per_entry)
                .map(|_| {
                    solana_system_transaction::transfer(
                        &Keypair::new(),
                        &Pubkey::from(rng.random::<[u8; 32]>()),
                        rng.random::<u64>() % 1_000_000,
                        Hash::from(rng.random::<[u8; 32]>()),
                    )
                })
                .collect();
            Entry::new(&Hash::from(rng.random::<[u8; 32]>()), 1, transactions)
        })
        .collect()
}

/// Setup blockstore with entries for a given slot
fn setup_blockstore_with_entries(blockstore: &Blockstore, entries: &[Entry], slot: Slot) {
    let shreds = entries_to_test_shreds(
        entries,
        slot,
        slot.saturating_sub(1),
        true, // is_full_slot
        0,    // version
    );
    blockstore
        .insert_shreds(shreds, None, false)
        .expect("Expected successful insertion of shreds into ledger");
}

/// Setup blockstore and return the number of shreds created
fn setup_blockstore_with_entries_return_shred_count(
    blockstore: &Blockstore,
    entries: &[Entry],
    slot: Slot,
) -> u64 {
    let shreds = entries_to_test_shreds(
        entries,
        slot,
        slot.saturating_sub(1),
        true, // is_full_slot
        0,    // version
    );
    let num_shreds = shreds.len() as u64;
    blockstore
        .insert_shreds(shreds, None, false)
        .expect("Expected successful insertion of shreds into ledger");
    num_shreds
}

// ============================================================================
// Basic Benchmarks (using test harness)
// ============================================================================

/// Benchmark reading entries from blockstore (RocksDB)
#[bench]
fn bench_blockstore_get_slot_entries(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore =
        Blockstore::open(ledger_path.path()).expect("Expected to be able to open database ledger");

    let num_entries = 100;
    let entries = create_tick_entries(num_entries);
    let slot = 1;

    setup_blockstore_with_entries(&blockstore, &entries, slot);

    bench.iter(|| {
        let result = blockstore.get_slot_entries(slot, 0).unwrap();
        test::black_box(result);
    });
}

/// Benchmark reading entries from entry cache (in-memory)
#[bench]
fn bench_entry_cache_get_slot_entries(bench: &mut Bencher) {
    let cache = EntryCache::new(100);

    let num_entries = 100;
    let entries = create_tick_entries(num_entries);
    let slot = 1;

    // Pre-populate the cache
    cache.insert_entries(slot, 0, entries.clone(), 50, true);

    bench.iter(|| {
        let result = cache.get_slot_entries(slot, 0);
        test::black_box(result);
    });
}

/// Benchmark entry cache misses (slot not in cache)
#[bench]
fn bench_entry_cache_miss(bench: &mut Bencher) {
    let cache = EntryCache::new(100);

    // Populate cache with some slots but not the one we're looking for
    let entries = create_tick_entries(10);
    for slot in 0..10 {
        cache.insert_entries(slot, 0, entries.clone(), 5, true);
    }

    bench.iter(|| {
        // Query a slot that doesn't exist in the cache
        let result = cache.get_slot_entries(999, 0);
        test::black_box(result);
    });
}

/// Benchmark entry cache with incremental entry insertion (simulating shred arrival)
#[bench]
fn bench_entry_cache_incremental_insert(bench: &mut Bencher) {
    let cache = EntryCache::new(100);
    let entries = create_tick_entries(10);
    let mut slot = 0u64;

    bench.iter(|| {
        slot += 1;
        // Simulate incremental entry insertion as shreds arrive
        cache.insert_entries(slot, 0, entries[0..3].to_vec(), 5, false);
        cache.insert_entries(slot, 5, entries[3..6].to_vec(), 5, false);
        cache.insert_entries(slot, 10, entries[6..10].to_vec(), 5, true);

        // Read back all entries
        let result = cache.get_slot_entries(slot, 0);
        test::black_box(result);
    });
}

/// Benchmark comparing cache hit vs blockstore read latency
/// This test shows the performance benefit of the entry cache
#[bench]
fn bench_entry_cache_vs_blockstore_comparison(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore =
        Blockstore::open(ledger_path.path()).expect("Expected to be able to open database ledger");
    let cache = EntryCache::new(100);

    let num_entries = 100;
    let entries = create_tick_entries(num_entries);
    let slot = 1;

    // Setup both blockstore and cache
    setup_blockstore_with_entries(&blockstore, &entries, slot);
    cache.insert_entries(slot, 0, entries.clone(), 50, true);

    // Benchmark pattern: try cache first, fallback to blockstore
    bench.iter(|| {
        let result = if let Some((entries, _, _)) = cache.get_slot_entries(slot, 0) {
            entries
        } else {
            blockstore.get_slot_entries(slot, 0).unwrap()
        };
        test::black_box(result);
    });
}

/// Benchmark slot eviction from cache
#[bench]
fn bench_entry_cache_eviction(bench: &mut Bencher) {
    let cache = EntryCache::new(10); // Small cache to trigger evictions
    let entries = create_tick_entries(10);
    let mut slot = 0u64;

    bench.iter(|| {
        slot += 1;
        cache.insert_entries(slot, 0, entries.clone(), 20, true);
    });
}

/// Benchmark purging old slots from cache
#[bench]
fn bench_entry_cache_purge_slots_below(bench: &mut Bencher) {
    let cache = EntryCache::new(1000);
    let entries = create_tick_entries(5);

    // Pre-populate with many slots
    for slot in 0..500 {
        cache.insert_entries(slot, 0, entries.clone(), 10, true);
    }

    let mut purge_threshold = 100u64;
    bench.iter(|| {
        purge_threshold += 1;
        cache.purge_slots_below(purge_threshold);
    });
}

// ============================================================================
// Realistic Workload Benchmarks
// ============================================================================

/// Benchmark with realistic transaction data (100 entries, ~10 txs each)
#[bench]
fn bench_blockstore_realistic_entries(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore =
        Blockstore::open(ledger_path.path()).expect("Expected to be able to open database ledger");

    let entries = create_realistic_entries(100, 10);
    let slot = 1;
    setup_blockstore_with_entries(&blockstore, &entries, slot);

    // Warm up
    for _ in 0..5 {
        let _ = blockstore.get_slot_entries(slot, 0);
    }

    bench.iter(|| {
        let result = blockstore.get_slot_entries(slot, 0).unwrap();
        test::black_box(result);
    });
}

/// Benchmark cache with realistic transaction data
#[bench]
fn bench_entry_cache_realistic_entries(bench: &mut Bencher) {
    let cache = EntryCache::new(100);

    let entries = create_realistic_entries(100, 10);
    let slot = 1;
    cache.insert_entries(slot, 0, entries.clone(), 200, true);

    bench.iter(|| {
        let result = cache.get_slot_entries(slot, 0);
        test::black_box(result);
    });
}

/// Benchmark sequential slot replay pattern (simulating actual replay)
#[bench]
fn bench_blockstore_sequential_replay(bench: &mut Bencher) {
    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore =
        Blockstore::open(ledger_path.path()).expect("Expected to be able to open database ledger");

    // Setup multiple consecutive slots
    let num_slots = 10;
    let entries_per_slot = 50;
    for slot in 1..=num_slots {
        let entries = create_tick_entries(entries_per_slot);
        setup_blockstore_with_entries(&blockstore, &entries, slot);
    }

    // Warm up
    for slot in 1..=num_slots {
        let _ = blockstore.get_slot_entries(slot, 0);
    }

    let mut current_slot = 0u64;
    bench.iter(|| {
        current_slot = (current_slot % num_slots) + 1;
        let result = blockstore.get_slot_entries(current_slot, 0).unwrap();
        test::black_box(result);
    });
}

/// Benchmark cache sequential slot replay pattern
#[bench]
fn bench_entry_cache_sequential_replay(bench: &mut Bencher) {
    let cache = EntryCache::new(100);

    // Setup multiple consecutive slots
    let num_slots = 10;
    let entries_per_slot = 50;
    for slot in 1..=num_slots {
        let entries = create_tick_entries(entries_per_slot);
        cache.insert_entries(slot, 0, entries, 25, true);
    }

    let mut current_slot = 0u64;
    bench.iter(|| {
        current_slot = (current_slot % num_slots) + 1;
        let result = cache.get_slot_entries(current_slot, 0);
        test::black_box(result);
    });
}

// ============================================================================
// Comprehensive Tests with Statistics
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Calculate percentile from sorted samples
    fn percentile(sorted_samples: &[u64], p: f64) -> u64 {
        if sorted_samples.is_empty() {
            return 0;
        }
        let idx = ((sorted_samples.len() as f64 * p / 100.0) as usize).min(sorted_samples.len() - 1);
        sorted_samples[idx]
    }

    /// Run a comparison test showing cache vs blockstore latency with detailed stats
    #[test]
    fn test_entry_cache_latency_comparison() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let cache = EntryCache::new(100);

        let num_entries = 100;
        let entries = create_tick_entries(num_entries);
        let slot = 1;

        // Setup both
        setup_blockstore_with_entries(&blockstore, &entries, slot);
        cache.insert_entries(slot, 0, entries.clone(), 50, true);

        // Warm up blockstore
        for _ in 0..100 {
            let _ = blockstore.get_slot_entries(slot, 0);
        }

        // Benchmark blockstore reads with per-iteration timing
        let iterations = 1000;
        let mut blockstore_samples = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let start = Instant::now();
            let _ = blockstore.get_slot_entries(slot, 0);
            blockstore_samples.push(start.elapsed().as_nanos() as u64);
        }

        // Benchmark cache reads with per-iteration timing
        let mut cache_samples = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let start = Instant::now();
            let _ = cache.get_slot_entries(slot, 0);
            cache_samples.push(start.elapsed().as_nanos() as u64);
        }

        // Calculate statistics
        blockstore_samples.sort();
        cache_samples.sort();

        let blockstore_p50 = percentile(&blockstore_samples, 50.0);
        let blockstore_p99 = percentile(&blockstore_samples, 99.0);
        let blockstore_avg: u64 = blockstore_samples.iter().sum::<u64>() / iterations as u64;

        let cache_p50 = percentile(&cache_samples, 50.0);
        let cache_p99 = percentile(&cache_samples, 99.0);
        let cache_avg: u64 = cache_samples.iter().sum::<u64>() / iterations as u64;

        println!("\n=== Entry Cache vs Blockstore Performance ({} entries) ===", num_entries);
        println!("Iterations: {}", iterations);
        println!();
        println!("BLOCKSTORE (RocksDB):");
        println!("  Average:  {:>8} ns ({:.2} us)", blockstore_avg, blockstore_avg as f64 / 1000.0);
        println!("  p50:      {:>8} ns ({:.2} us)", blockstore_p50, blockstore_p50 as f64 / 1000.0);
        println!("  p99:      {:>8} ns ({:.2} us)", blockstore_p99, blockstore_p99 as f64 / 1000.0);
        println!();
        println!("ENTRY CACHE (in-memory):");
        println!("  Average:  {:>8} ns ({:.2} us)", cache_avg, cache_avg as f64 / 1000.0);
        println!("  p50:      {:>8} ns ({:.2} us)", cache_p50, cache_p50 as f64 / 1000.0);
        println!("  p99:      {:>8} ns ({:.2} us)", cache_p99, cache_p99 as f64 / 1000.0);
        println!();
        println!("SPEEDUP:");
        println!("  Average: {:.1}x faster with cache", blockstore_avg as f64 / cache_avg.max(1) as f64);
        println!("  p50:     {:.1}x faster with cache", blockstore_p50 as f64 / cache_p50.max(1) as f64);
        println!("  p99:     {:.1}x faster with cache", blockstore_p99 as f64 / cache_p99.max(1) as f64);
        println!();
        println!("LATENCY SAVINGS:");
        println!("  Average: {:.2} us saved per read", (blockstore_avg.saturating_sub(cache_avg)) as f64 / 1000.0);
        println!("  p50:     {:.2} us saved per read", (blockstore_p50.saturating_sub(cache_p50)) as f64 / 1000.0);
        println!("  p99:     {:.2} us saved per read", (blockstore_p99.saturating_sub(cache_p99)) as f64 / 1000.0);

        // Entry cache should be faster than blockstore
        assert!(
            cache_avg < blockstore_avg,
            "Entry cache ({} ns) should be faster than blockstore ({} ns)",
            cache_avg,
            blockstore_avg
        );
    }

    /// Test with realistic transaction data
    #[test]
    fn test_realistic_entry_latency_comparison() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let cache = EntryCache::new(100);

        // Create realistic entries with transactions
        let num_entries = 100;
        let txs_per_entry = 10;
        let entries = create_realistic_entries(num_entries, txs_per_entry);
        let slot = 1;

        let num_shreds = setup_blockstore_with_entries_return_shred_count(&blockstore, &entries, slot);
        cache.insert_entries(slot, 0, entries.clone(), num_shreds, true);

        // Warm up
        for _ in 0..50 {
            let _ = blockstore.get_slot_entries(slot, 0);
        }

        let iterations = 500;
        let mut blockstore_samples = Vec::with_capacity(iterations);
        let mut cache_samples = Vec::with_capacity(iterations);

        for _ in 0..iterations {
            let start = Instant::now();
            let _ = blockstore.get_slot_entries(slot, 0);
            blockstore_samples.push(start.elapsed().as_nanos() as u64);

            let start = Instant::now();
            let _ = cache.get_slot_entries(slot, 0);
            cache_samples.push(start.elapsed().as_nanos() as u64);
        }

        blockstore_samples.sort();
        cache_samples.sort();

        let blockstore_p50 = percentile(&blockstore_samples, 50.0);
        let blockstore_p99 = percentile(&blockstore_samples, 99.0);
        let cache_p50 = percentile(&cache_samples, 50.0);
        let cache_p99 = percentile(&cache_samples, 99.0);

        println!("\n=== Realistic Entry Performance ({} entries, {} txs each, {} shreds) ===",
                 num_entries, txs_per_entry, num_shreds);
        println!();
        println!("BLOCKSTORE: p50={:.2}us, p99={:.2}us",
                 blockstore_p50 as f64 / 1000.0, blockstore_p99 as f64 / 1000.0);
        println!("CACHE:      p50={:.2}us, p99={:.2}us",
                 cache_p50 as f64 / 1000.0, cache_p99 as f64 / 1000.0);
        println!("SPEEDUP:    p50={:.1}x, p99={:.1}x",
                 blockstore_p50 as f64 / cache_p50.max(1) as f64,
                 blockstore_p99 as f64 / cache_p99.max(1) as f64);
    }

    /// Test sequential replay pattern throughput
    #[test]
    fn test_sequential_replay_throughput() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let cache = EntryCache::new(100);

        // Setup multiple slots (simulating replay of multiple consecutive slots)
        let num_slots = 20u64;
        let entries_per_slot = 100;

        println!("\n=== Sequential Replay Throughput Test ===");
        println!("Slots: {}, Entries per slot: {}", num_slots, entries_per_slot);

        for slot in 1..=num_slots {
            let entries = create_tick_entries(entries_per_slot);
            let num_shreds = setup_blockstore_with_entries_return_shred_count(&blockstore, &entries, slot);
            cache.insert_entries(slot, 0, entries, num_shreds, true);
        }

        // Warm up blockstore
        for slot in 1..=num_slots {
            let _ = blockstore.get_slot_entries(slot, 0);
        }

        // Measure blockstore throughput (replay all slots sequentially)
        let replay_iterations = 100;
        let start = Instant::now();
        for _ in 0..replay_iterations {
            for slot in 1..=num_slots {
                let _ = blockstore.get_slot_entries(slot, 0).unwrap();
            }
        }
        let blockstore_duration = start.elapsed();
        let blockstore_slots_per_sec = (replay_iterations * num_slots as usize) as f64 / blockstore_duration.as_secs_f64();

        // Measure cache throughput
        let start = Instant::now();
        for _ in 0..replay_iterations {
            for slot in 1..=num_slots {
                let _ = cache.get_slot_entries(slot, 0);
            }
        }
        let cache_duration = start.elapsed();
        let cache_slots_per_sec = (replay_iterations * num_slots as usize) as f64 / cache_duration.as_secs_f64();

        println!();
        println!("THROUGHPUT (slots/second):");
        println!("  Blockstore: {:.0} slots/sec", blockstore_slots_per_sec);
        println!("  Cache:      {:.0} slots/sec", cache_slots_per_sec);
        println!("  Speedup:    {:.1}x", cache_slots_per_sec / blockstore_slots_per_sec);
        println!();
        println!("TIME for {} slot replays:", replay_iterations * num_slots as usize);
        println!("  Blockstore: {:.2}ms", blockstore_duration.as_secs_f64() * 1000.0);
        println!("  Cache:      {:.2}ms", cache_duration.as_secs_f64() * 1000.0);
    }

    /// Test incremental shred arrival pattern (simulating real validator data flow)
    #[test]
    fn test_incremental_shred_arrival_pattern() {
        let cache = EntryCache::new(100);

        println!("\n=== Incremental Shred Arrival Pattern ===");

        // Simulate batches of entries arriving as shreds complete
        let num_batches = 10;
        let entries_per_batch = 10;
        let slot = 1u64;

        let mut insert_samples = Vec::new();
        let mut read_samples = Vec::new();
        let mut shred_index = 0u64;

        for batch_idx in 0..num_batches {
            let entries = create_tick_entries(entries_per_batch);
            let num_shreds_in_batch = 5u64;
            let is_full = batch_idx == num_batches - 1;

            // Measure insert time
            let start = Instant::now();
            cache.insert_entries(slot, shred_index, entries, num_shreds_in_batch, is_full);
            insert_samples.push(start.elapsed().as_nanos() as u64);

            shred_index += num_shreds_in_batch;

            // Measure read time (from start of slot)
            let start = Instant::now();
            let _ = cache.get_slot_entries(slot, 0);
            read_samples.push(start.elapsed().as_nanos() as u64);
        }

        insert_samples.sort();
        read_samples.sort();

        println!("Batches: {}, Entries per batch: {}", num_batches, entries_per_batch);
        println!();
        println!("INSERT LATENCY:");
        println!("  p50: {:.2}us, p99: {:.2}us",
                 percentile(&insert_samples, 50.0) as f64 / 1000.0,
                 percentile(&insert_samples, 99.0) as f64 / 1000.0);
        println!("READ LATENCY (cumulative entries):");
        println!("  p50: {:.2}us, p99: {:.2}us",
                 percentile(&read_samples, 50.0) as f64 / 1000.0,
                 percentile(&read_samples, 99.0) as f64 / 1000.0);
    }

    /// Test cache miss with fallback to blockstore (simulating actual replay flow)
    #[test]
    fn test_cache_miss_fallback_pattern() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let cache = EntryCache::new(100);

        let num_entries = 100;
        let entries = create_tick_entries(num_entries);
        let slot = 1;

        // Only setup blockstore (cache miss scenario)
        let num_shreds = setup_blockstore_with_entries_return_shred_count(&blockstore, &entries, slot);

        // Warm up
        for _ in 0..50 {
            let _ = blockstore.get_slot_entries(slot, 0);
        }

        let iterations = 500;
        let mut cache_miss_fallback_samples = Vec::with_capacity(iterations);
        let mut direct_blockstore_samples = Vec::with_capacity(iterations);

        for _ in 0..iterations {
            // Pattern 1: Cache check + fallback to blockstore (actual replay flow)
            let start = Instant::now();
            let result = if let Some((entries, _, _)) = cache.get_slot_entries(slot, 0) {
                entries
            } else {
                blockstore.get_slot_entries(slot, 0).unwrap()
            };
            std::hint::black_box(&result);
            cache_miss_fallback_samples.push(start.elapsed().as_nanos() as u64);

            // Pattern 2: Direct blockstore (no cache check)
            let start = Instant::now();
            let result = blockstore.get_slot_entries(slot, 0).unwrap();
            std::hint::black_box(&result);
            direct_blockstore_samples.push(start.elapsed().as_nanos() as u64);
        }

        cache_miss_fallback_samples.sort();
        direct_blockstore_samples.sort();

        let fallback_p50 = percentile(&cache_miss_fallback_samples, 50.0);
        let fallback_p99 = percentile(&cache_miss_fallback_samples, 99.0);
        let direct_p50 = percentile(&direct_blockstore_samples, 50.0);
        let direct_p99 = percentile(&direct_blockstore_samples, 99.0);

        println!("\n=== Cache Miss Fallback Overhead ({} entries, {} shreds) ===", num_entries, num_shreds);
        println!();
        println!("CACHE MISS + FALLBACK:");
        println!("  p50: {:.2}us, p99: {:.2}us", fallback_p50 as f64 / 1000.0, fallback_p99 as f64 / 1000.0);
        println!("DIRECT BLOCKSTORE:");
        println!("  p50: {:.2}us, p99: {:.2}us", direct_p50 as f64 / 1000.0, direct_p99 as f64 / 1000.0);
        println!("OVERHEAD:");
        println!("  p50: {:.2}us ({:.1}% overhead)",
                 (fallback_p50.saturating_sub(direct_p50)) as f64 / 1000.0,
                 (fallback_p50 as f64 / direct_p50.max(1) as f64 - 1.0) * 100.0);
    }

    /// Memory overhead estimation
    #[test]
    fn test_memory_overhead() {
        println!("\n=== Memory Overhead Estimation ===");

        let entries_per_slot = 100;
        let txs_per_entry = 10;
        let num_slots = 32; // Default cache size

        // Create sample entries to estimate size
        let _entries = create_realistic_entries(entries_per_slot, txs_per_entry);

        // Estimate entry size (rough approximation)
        let estimated_entry_size = std::mem::size_of::<Entry>() +
            txs_per_entry * std::mem::size_of::<Transaction>() +
            txs_per_entry * 200; // ~200 bytes per transaction data

        let estimated_slot_size = entries_per_slot * estimated_entry_size;
        let estimated_cache_size = num_slots * estimated_slot_size;

        println!("Configuration:");
        println!("  Slots cached: {}", num_slots);
        println!("  Entries per slot: {}", entries_per_slot);
        println!("  Transactions per entry: {}", txs_per_entry);
        println!();
        println!("Estimated Memory Usage:");
        println!("  Per entry:  ~{} KB", estimated_entry_size / 1024);
        println!("  Per slot:   ~{} MB", estimated_slot_size / (1024 * 1024));
        println!("  Full cache: ~{} MB", estimated_cache_size / (1024 * 1024));

        // Create actual cache and measure
        let cache = EntryCache::new(num_slots);
        for slot in 0..num_slots as u64 {
            let entries = create_realistic_entries(entries_per_slot, txs_per_entry);
            cache.insert_entries(slot, 0, entries, 50, true);
        }

        println!("  Slots stored: {}", cache.len());
    }

    /// Comprehensive benchmark comparing all access patterns
    #[test]
    fn test_comprehensive_benchmark() {
        let _ledger_path = get_tmp_ledger_path_auto_delete!();
        let _blockstore = Blockstore::open(_ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let _cache = EntryCache::new(100);

        println!("\n================================================================================");
        println!("         ENTRY CACHE vs BLOCKSTORE COMPREHENSIVE BENCHMARK");
        println!("================================================================================\n");

        // Test configurations
        let configs = [
            (50, 0, "50 tick entries"),
            (100, 0, "100 tick entries"),
            (200, 0, "200 tick entries"),
            (50, 5, "50 entries, 5 txs each"),
            (100, 10, "100 entries, 10 txs each"),
        ];

        println!("{:<30} {:>12} {:>12} {:>12} {:>12}",
                 "Configuration", "BS p50(us)", "Cache p50(us)", "Speedup", "Saved(us)");
        println!("{}", "-".repeat(80));

        for (num_entries, txs_per_entry, description) in configs.iter() {
            let entries = if *txs_per_entry == 0 {
                create_tick_entries(*num_entries)
            } else {
                create_realistic_entries(*num_entries, *txs_per_entry)
            };

            let slot = 1;

            // Clear and setup
            let ledger_path_inner = get_tmp_ledger_path_auto_delete!();
            let blockstore_inner = Blockstore::open(ledger_path_inner.path())
                .expect("Expected to be able to open database ledger");
            let cache_inner = EntryCache::new(100);

            let num_shreds = setup_blockstore_with_entries_return_shred_count(&blockstore_inner, &entries, slot);
            cache_inner.insert_entries(slot, 0, entries.clone(), num_shreds, true);

            // Warm up
            for _ in 0..50 {
                let _ = blockstore_inner.get_slot_entries(slot, 0);
            }

            // Measure
            let iterations = 200;
            let mut bs_samples = Vec::with_capacity(iterations);
            let mut cache_samples = Vec::with_capacity(iterations);

            for _ in 0..iterations {
                let start = Instant::now();
                let _ = blockstore_inner.get_slot_entries(slot, 0);
                bs_samples.push(start.elapsed().as_nanos() as u64);

                let start = Instant::now();
                let _ = cache_inner.get_slot_entries(slot, 0);
                cache_samples.push(start.elapsed().as_nanos() as u64);
            }

            bs_samples.sort();
            cache_samples.sort();

            let bs_p50 = percentile(&bs_samples, 50.0);
            let cache_p50 = percentile(&cache_samples, 50.0);
            let speedup = bs_p50 as f64 / cache_p50.max(1) as f64;
            let saved = bs_p50.saturating_sub(cache_p50);

            println!("{:<30} {:>12.2} {:>12.2} {:>11.1}x {:>12.2}",
                     description,
                     bs_p50 as f64 / 1000.0,
                     cache_p50 as f64 / 1000.0,
                     speedup,
                     saved as f64 / 1000.0);
        }

        println!("\n{}", "-".repeat(80));
        println!("\nNOTE: Results may vary based on system load, disk cache state, and hardware.");
        println!("Run with --nocapture to see output: cargo test --release -- --nocapture");
    }

    /// Test varying data sizes to understand scaling behavior
    #[test]
    fn test_scaling_behavior() {
        println!("\n=== Scaling Behavior Analysis ===\n");

        let cache = EntryCache::new(100);

        let entry_counts = [10, 50, 100, 200, 500];

        println!("{:<15} {:>12} {:>12} {:>12}",
                 "Entries", "Insert(us)", "Read(us)", "Entries/us");
        println!("{}", "-".repeat(55));

        for &num_entries in &entry_counts {
            let entries = create_tick_entries(num_entries);
            let slot = num_entries as u64; // Use different slots

            // Measure insert
            let start = Instant::now();
            cache.insert_entries(slot, 0, entries.clone(), num_entries as u64, true);
            let insert_time = start.elapsed().as_nanos() as u64;

            // Measure read (average of 100 reads)
            let mut read_times = Vec::new();
            for _ in 0..100 {
                let start = Instant::now();
                let _ = cache.get_slot_entries(slot, 0);
                read_times.push(start.elapsed().as_nanos() as u64);
            }
            let avg_read_time: u64 = read_times.iter().sum::<u64>() / 100;

            let entries_per_us = (num_entries as f64 * 1000.0) / avg_read_time as f64;

            println!("{:<15} {:>12.2} {:>12.2} {:>12.1}",
                     num_entries,
                     insert_time as f64 / 1000.0,
                     avg_read_time as f64 / 1000.0,
                     entries_per_us);
        }
    }
}
