#![allow(clippy::arithmetic_side_effects)]
//! Benchmarks for `process_entries()` — the core replay scheduling path.
//!
//! `process_entries_us` measures the time to iterate entries, split into
//! conflict-free batches, lock/unlock accounts, and schedule transactions
//! to the unified scheduler. This benchmark captures that full path.

use {
    criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput},
    solana_entry::entry::{next_hash, Entry},
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_ledger::{
        blockstore_processor::process_entries_for_tests,
        genesis_utils::{create_genesis_config, GenesisConfigInfo},
    },
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
        installed_scheduler_pool::{
            BankWithScheduler, InstalledSchedulerPool, InstalledSchedulerPoolArc, SchedulingContext,
        },
    },
    solana_signer::Signer,
    solana_system_transaction,
    solana_transaction::versioned::VersionedTransaction,
    std::{
        iter::repeat_with,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::Instant,
    },
};

/// Global slot counter to ensure unique slots across iterations.
static NEXT_SLOT: AtomicU64 = AtomicU64::new(100);

/// Create a parent bank with fork graph.
fn make_parent_bank(
    genesis_config: &solana_genesis_config::GenesisConfig,
) -> (Arc<Bank>, Arc<std::sync::RwLock<BankForks>>) {
    let bank = Bank::new_for_benches(genesis_config);
    let bank_forks = BankForks::new_rw_arc(bank);
    let parent = bank_forks.read().unwrap().root_bank();
    parent.set_fork_graph_in_program_cache(Arc::downgrade(&bank_forks));
    (parent, bank_forks)
}

/// Create a shared scheduler pool (reuse across iterations to avoid thread exhaustion).
fn make_scheduler_pool() -> InstalledSchedulerPoolArc {
    solana_unified_scheduler_pool::DefaultSchedulerPool::new_for_verification(
        None, None, None, None, None,
    )
}

/// Create a fresh child bank with a scheduler from the shared pool.
fn make_child_bank(
    parent: &Arc<Bank>,
    bank_forks: &Arc<std::sync::RwLock<BankForks>>,
    pool: &InstalledSchedulerPoolArc,
) -> BankWithScheduler {
    let slot = NEXT_SLOT.fetch_add(1, Ordering::Relaxed);
    let child = Bank::new_from_parent(parent.clone(), &Pubkey::new_unique(), slot);
    let child_arc = Arc::new(child);

    let context = SchedulingContext::for_verification(child_arc.clone());
    let scheduler = pool.take_scheduler(context).unwrap();
    let bank_with_scheduler = BankWithScheduler::new(child_arc.clone(), Some(scheduler));
    child_arc.set_fork_graph_in_program_cache(Arc::downgrade(bank_forks));
    bank_with_scheduler
}

/// Fund `count` accounts from the mint, returning their keypairs.
fn fund_accounts(bank: &Bank, mint: &Keypair, count: usize, lamports_each: u64) -> Vec<Keypair> {
    let keypairs: Vec<Keypair> = repeat_with(Keypair::new).take(count).collect();
    for kp in &keypairs {
        bank.transfer(lamports_each, mint, &kp.pubkey()).unwrap();
    }
    keypairs
}

/// Build entries containing `num_txs` transfer transactions.
/// Transactions are between unique pairs of funded accounts (no conflicts).
fn build_transfer_entries(
    keypairs: &[Keypair],
    recent_blockhash: Hash,
    num_txs: usize,
    txs_per_entry: usize,
) -> Vec<Entry> {
    assert!(
        num_txs * 2 <= keypairs.len(),
        "Need at least 2x keypairs for non-conflicting transfers"
    );

    let mut entries = Vec::new();
    let mut tx_buf: Vec<VersionedTransaction> = Vec::with_capacity(txs_per_entry);
    let mut prev_hash = recent_blockhash;

    for i in 0..num_txs {
        let from = &keypairs[i * 2];
        let to = &keypairs[i * 2 + 1];
        let tx: VersionedTransaction =
            solana_system_transaction::transfer(from, &to.pubkey(), 1, recent_blockhash).into();
        tx_buf.push(tx);

        if tx_buf.len() == txs_per_entry || i == num_txs - 1 {
            let hash = next_hash(&prev_hash, 1, &tx_buf);
            entries.push(Entry {
                num_hashes: 1,
                hash,
                transactions: std::mem::take(&mut tx_buf),
            });
            prev_hash = hash;
        }
    }
    entries
}

/// Build entries with conflicting transactions (many txs touch same account).
fn build_conflicting_entries(
    keypairs: &[Keypair],
    hot_account: &Pubkey,
    recent_blockhash: Hash,
    num_txs: usize,
    txs_per_entry: usize,
) -> Vec<Entry> {
    let mut entries = Vec::new();
    let mut tx_buf: Vec<VersionedTransaction> = Vec::with_capacity(txs_per_entry);
    let mut prev_hash = recent_blockhash;

    for i in 0..num_txs.min(keypairs.len()) {
        let tx: VersionedTransaction =
            solana_system_transaction::transfer(&keypairs[i], hot_account, 1, recent_blockhash)
                .into();
        tx_buf.push(tx);

        if tx_buf.len() == txs_per_entry || i == num_txs.min(keypairs.len()) - 1 {
            let hash = next_hash(&prev_hash, 1, &tx_buf);
            entries.push(Entry {
                num_hashes: 1,
                hash,
                transactions: std::mem::take(&mut tx_buf),
            });
            prev_hash = hash;
        }
    }
    entries
}

/// Build entries with realistic mixed conflicts: `conflict_pct`% of transactions
/// write to one of `num_hot` hot accounts, the rest are independent transfers.
fn build_mixed_conflict_entries(
    keypairs: &[Keypair],
    hot_accounts: &[Pubkey],
    recent_blockhash: Hash,
    num_txs: usize,
    txs_per_entry: usize,
    conflict_pct: usize,
) -> Vec<Entry> {
    let mut entries = Vec::new();
    let mut tx_buf: Vec<VersionedTransaction> = Vec::with_capacity(txs_per_entry);
    let mut prev_hash = recent_blockhash;
    // Use first keypairs for conflicting txs, remaining pairs for independent txs
    let conflict_count = num_txs * conflict_pct / 100;
    let independent_count = num_txs - conflict_count;

    for i in 0..num_txs {
        let tx: VersionedTransaction = if i < conflict_count {
            // Conflicting: write to a hot account
            let hot = &hot_accounts[i % hot_accounts.len()];
            solana_system_transaction::transfer(&keypairs[i], hot, 1, recent_blockhash).into()
        } else {
            // Independent: unique sender/receiver pair
            let pair_idx = (i - conflict_count) * 2 + conflict_count;
            let from = &keypairs[pair_idx];
            let to = &keypairs[pair_idx + 1];
            solana_system_transaction::transfer(from, &to.pubkey(), 1, recent_blockhash).into()
        };
        tx_buf.push(tx);

        if tx_buf.len() == txs_per_entry || i == num_txs - 1 {
            let hash = next_hash(&prev_hash, 1, &tx_buf);
            entries.push(Entry {
                num_hashes: 1,
                hash,
                transactions: std::mem::take(&mut tx_buf),
            });
            prev_hash = hash;
        }
    }
    entries
}

fn bench_process_entries_no_conflicts(c: &mut Criterion) {
    let mut group = c.benchmark_group("process_entries_no_conflicts");
    let pool = make_scheduler_pool();

    for num_txs in [50, 100, 200, 500] {
        let num_accounts = num_txs * 2 + 10;
        let lamports_each = 1_000_000;

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config((num_accounts as u64 + 1) * lamports_each);

        let (parent_arc, bank_forks) = make_parent_bank(&genesis_config);
        let keypairs = fund_accounts(&parent_arc, &mint_keypair, num_accounts, lamports_each);

        group.throughput(Throughput::Elements(num_txs as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_txs),
            &num_txs,
            |b, &num_txs| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let child_bank = make_child_bank(&parent_arc, &bank_forks, &pool);
                        let blockhash = child_bank.last_blockhash();
                        let entries =
                            build_transfer_entries(&keypairs, blockhash, num_txs, 64);

                        let start = Instant::now();
                        process_entries_for_tests(&child_bank, entries, None, None).unwrap();
                        if let Some((result, _)) = child_bank.wait_for_completed_scheduler() {
                            result.unwrap();
                        }
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

fn bench_process_entries_with_conflicts(c: &mut Criterion) {
    let mut group = c.benchmark_group("process_entries_with_conflicts");
    let pool = make_scheduler_pool();

    for num_txs in [50, 100, 200] {
        let num_accounts = num_txs + 10;
        let lamports_each = 1_000_000;

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config((num_accounts as u64 + 1) * lamports_each);

        let (parent_arc, bank_forks) = make_parent_bank(&genesis_config);
        let keypairs = fund_accounts(&parent_arc, &mint_keypair, num_accounts, lamports_each);
        let hot_account = Pubkey::new_unique();

        group.throughput(Throughput::Elements(num_txs as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_txs),
            &num_txs,
            |b, &num_txs| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let child_bank = make_child_bank(&parent_arc, &bank_forks, &pool);
                        let blockhash = child_bank.last_blockhash();
                        let entries = build_conflicting_entries(
                            &keypairs,
                            &hot_account,
                            blockhash,
                            num_txs,
                            64,
                        );

                        let start = Instant::now();
                        process_entries_for_tests(&child_bank, entries, None, None).unwrap();
                        if let Some((result, _)) = child_bank.wait_for_completed_scheduler() {
                            result.unwrap();
                        }
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

/// Measures entry build cost vs process_entries cost at 200 txs.
fn bench_process_entries_breakdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("process_entries_breakdown");
    let pool = make_scheduler_pool();
    let num_txs = 200;
    let txs_per_entry = 64;
    let num_accounts = num_txs * 2 + 10;
    let lamports_each = 1_000_000;

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config((num_accounts as u64 + 1) * lamports_each);

    let (parent_arc, bank_forks) = make_parent_bank(&genesis_config);
    let keypairs = fund_accounts(&parent_arc, &mint_keypair, num_accounts, lamports_each);

    // Measure entry building overhead separately
    group.bench_function("build_entries_200tx", |b| {
        let blockhash = parent_arc.last_blockhash();
        b.iter(|| {
            build_transfer_entries(&keypairs, blockhash, num_txs, txs_per_entry);
        });
    });

    // Measure full process_entries
    group.throughput(Throughput::Elements(num_txs as u64));
    group.bench_function("process_200tx_64per_entry", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let child_bank = make_child_bank(&parent_arc, &bank_forks, &pool);
                let blockhash = child_bank.last_blockhash();
                let entries =
                    build_transfer_entries(&keypairs, blockhash, num_txs, txs_per_entry);

                let start = Instant::now();
                process_entries_for_tests(&child_bank, entries, None, None).unwrap();
                if let Some((result, _)) = child_bank.wait_for_completed_scheduler() {
                    result.unwrap();
                }
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

/// Realistic production mix: 20% of txs conflict on 5 hot accounts,
/// 80% are independent transfers.
fn bench_process_entries_mixed_conflicts(c: &mut Criterion) {
    let mut group = c.benchmark_group("process_entries_mixed_20pct");
    let pool = make_scheduler_pool();

    for num_txs in [100, 200, 500] {
        // Need enough keypairs: conflict_count + 2*(independent_count)
        let conflict_count = num_txs * 20 / 100;
        let independent_count = num_txs - conflict_count;
        let num_accounts = conflict_count + independent_count * 2 + 20;
        let lamports_each = 1_000_000;

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config((num_accounts as u64 + 1) * lamports_each);

        let (parent_arc, bank_forks) = make_parent_bank(&genesis_config);
        let keypairs = fund_accounts(&parent_arc, &mint_keypair, num_accounts, lamports_each);
        let hot_accounts: Vec<Pubkey> = (0..5).map(|_| Pubkey::new_unique()).collect();

        group.throughput(Throughput::Elements(num_txs as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_txs),
            &num_txs,
            |b, &num_txs| {
                b.iter_custom(|iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let child_bank = make_child_bank(&parent_arc, &bank_forks, &pool);
                        let blockhash = child_bank.last_blockhash();
                        let entries = build_mixed_conflict_entries(
                            &keypairs,
                            &hot_accounts,
                            blockhash,
                            num_txs,
                            64,
                            20,
                        );

                        let start = Instant::now();
                        process_entries_for_tests(&child_bank, entries, None, None).unwrap();
                        if let Some((result, _)) = child_bank.wait_for_completed_scheduler() {
                            result.unwrap();
                        }
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

/// Per-phase timing: measures where time actually goes at 200 txs.
/// Separates: verification, scheduling (process_entries), and execution (wait).
fn bench_phase_timing(c: &mut Criterion) {
    use solana_entry::entry::{self, EntryType};
    use solana_transaction::{sanitized::SanitizedTransaction, TransactionVerificationMode};
    use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;

    let mut group = c.benchmark_group("phase_timing_200tx");
    let pool = make_scheduler_pool();
    let num_txs = 200;
    let txs_per_entry = 64;
    let num_accounts = num_txs * 2 + 10;
    let lamports_each = 1_000_000;

    let GenesisConfigInfo {
        genesis_config,
        mint_keypair,
        ..
    } = create_genesis_config((num_accounts as u64 + 1) * lamports_each);

    let (parent_arc, bank_forks) = make_parent_bank(&genesis_config);
    let keypairs = fund_accounts(&parent_arc, &mint_keypair, num_accounts, lamports_each);
    let replay_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .thread_name(|i| format!("solReplayTx{i:02}"))
        .build()
        .unwrap();

    // Phase 1: Verification only (HashOnly mode)
    group.bench_function("1_verify_only", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let child_bank = make_child_bank(&parent_arc, &bank_forks, &pool);
                let blockhash = child_bank.last_blockhash();
                let entries = build_transfer_entries(&keypairs, blockhash, num_txs, txs_per_entry);

                let verify_fn = {
                    let bank = child_bank.clone_with_scheduler();
                    move |versioned_tx: VersionedTransaction| -> solana_transaction_error::TransactionResult<RuntimeTransaction<SanitizedTransaction>> {
                        bank.verify_transaction(versioned_tx, TransactionVerificationMode::HashOnly)
                    }
                };

                let start = Instant::now();
                let _verified = entry::verify_transactions(
                    entries,
                    &replay_pool,
                    Arc::new(verify_fn),
                ).unwrap();
                total += start.elapsed();

                // Clean up scheduler
                child_bank.wait_for_completed_scheduler();
            }
            total
        });
    });

    // Phase 2: Full pipeline (verify + schedule + execute + wait)
    group.bench_function("2_full_pipeline", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let child_bank = make_child_bank(&parent_arc, &bank_forks, &pool);
                let blockhash = child_bank.last_blockhash();
                let entries = build_transfer_entries(&keypairs, blockhash, num_txs, txs_per_entry);

                let start = Instant::now();
                process_entries_for_tests(&child_bank, entries, None, None).unwrap();
                if let Some((result, _)) = child_bank.wait_for_completed_scheduler() {
                    result.unwrap();
                }
                total += start.elapsed();
            }
            total
        });
    });

    // Phase 3: Schedule + execute only (pre-verified entries)
    group.bench_function("3_schedule_and_execute", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let child_bank = make_child_bank(&parent_arc, &bank_forks, &pool);
                let blockhash = child_bank.last_blockhash();
                let entries = build_transfer_entries(&keypairs, blockhash, num_txs, txs_per_entry);

                // Pre-verify outside timing
                let verify_fn = {
                    let bank = child_bank.clone_with_scheduler();
                    move |versioned_tx: VersionedTransaction| -> solana_transaction_error::TransactionResult<RuntimeTransaction<SanitizedTransaction>> {
                        bank.verify_transaction(versioned_tx, TransactionVerificationMode::HashOnly)
                    }
                };
                let verified = entry::verify_transactions(
                    entries,
                    &replay_pool,
                    Arc::new(verify_fn),
                ).unwrap();

                // Now time only scheduling + execution
                let start = Instant::now();
                // Schedule each verified entry's transactions
                for entry_type in verified {
                    if let EntryType::Transactions(txs) = entry_type {
                        let tx_count = txs.len();
                        let pairs: Vec<_> = txs.into_iter()
                            .enumerate()
                            .map(|(i, tx)| (tx, i as u128))
                            .collect();
                        child_bank.schedule_transaction_executions(pairs.into_iter()).unwrap();
                    }
                }
                if let Some((result, _)) = child_bank.wait_for_completed_scheduler() {
                    result.unwrap();
                }
                total += start.elapsed();
            }
            total
        });
    });

    // Phase 4: Just execution wait (schedule in setup, time only wait)
    group.bench_function("4_execute_wait_only", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let child_bank = make_child_bank(&parent_arc, &bank_forks, &pool);
                let blockhash = child_bank.last_blockhash();
                let entries = build_transfer_entries(&keypairs, blockhash, num_txs, txs_per_entry);

                // Pre-verify + schedule outside timing
                process_entries_for_tests(&child_bank, entries, None, None).unwrap();

                // Time only the wait for execution
                let start = Instant::now();
                if let Some((result, _)) = child_bank.wait_for_completed_scheduler() {
                    result.unwrap();
                }
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_phase_timing,
    bench_process_entries_no_conflicts,
    bench_process_entries_with_conflicts,
    bench_process_entries_mixed_conflicts,
    bench_process_entries_breakdown,
);
criterion_main!(benches);
