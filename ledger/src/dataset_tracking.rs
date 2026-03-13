//! Dataset-level Parquet tracking for signatures, execution timing, and ticks.
//!
//! Captures rotating Parquet files alongside shred arrival data:
//! - `dataset_signatures_*.parquet` — first & last transaction signature per dataset
//! - `dataset_execution_*.parquet` — execution start/end timestamps per dataset batch
//! - `ticks_*.parquet` — tick timestamps with preceding entry's transaction signatures

use {
    crate::parquet_writer::{IntoRecordBatch, RotatingParquetWriter},
    arrow::{
        array::*,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    crossbeam_channel::{Receiver, Sender},
    solana_clock::Slot,
    solana_hash::Hash,
    solana_signature::Signature,
    std::{
        fs,
        ops::Range,
        path::PathBuf,
        sync::Arc,
        thread::{self, JoinHandle},
        time::Instant,
    },
};

#[inline(always)]
pub fn monotonic_micros() -> u64 {
    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts) };
    (ts.tv_sec as u64) * 1_000_000 + (ts.tv_nsec as u64) / 1_000
}

// =============================================================================
// Dataset Signature Records
// =============================================================================

/// Record of first/last transaction signatures for a completed dataset.
#[derive(Debug)]
pub struct DatasetSignatureRecord {
    pub slot: Slot,
    pub indices: Range<u32>,
    pub first_tx_signature: Signature,
    pub last_tx_signature: Signature,
    pub num_transactions: u64,
    pub timestamp_us: u64,
}

impl IntoRecordBatch for DatasetSignatureRecord {
    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("dataset_id", DataType::Utf8, false),
            Field::new("slot", DataType::UInt64, false),
            Field::new("first_tx_signature", DataType::Utf8, false),
            Field::new("last_tx_signature", DataType::Utf8, false),
            Field::new("num_transactions", DataType::UInt64, false),
            Field::new("timestamp_us", DataType::UInt64, false),
        ]))
    }

    fn to_record_batch(records: &[Self]) -> RecordBatch {
        let dataset_ids: Vec<String> = records
            .iter()
            .map(|r| format!("{}_{}", r.slot, r.indices.start))
            .collect();
        let slots: Vec<u64> = records.iter().map(|r| r.slot).collect();
        let first_sigs: Vec<String> = records.iter().map(|r| r.first_tx_signature.to_string()).collect();
        let last_sigs: Vec<String> = records.iter().map(|r| r.last_tx_signature.to_string()).collect();
        let num_txs: Vec<u64> = records.iter().map(|r| r.num_transactions).collect();
        let timestamps: Vec<u64> = records.iter().map(|r| r.timestamp_us).collect();

        RecordBatch::try_new(
            Self::arrow_schema(),
            vec![
                Arc::new(StringArray::from(
                    dataset_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(slots)),
                Arc::new(StringArray::from(
                    first_sigs.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    last_sigs.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(num_txs)),
                Arc::new(UInt64Array::from(timestamps)),
            ],
        )
        .expect("Failed to create DatasetSignature RecordBatch")
    }
}

/// Sender handle for dataset signature records (used in window_service).
#[derive(Clone)]
pub struct DatasetSignatureSender {
    sender: Sender<DatasetSignatureRecord>,
}

impl DatasetSignatureSender {
    pub fn new(sender: Sender<DatasetSignatureRecord>) -> Self {
        Self { sender }
    }

    pub fn record(
        &self,
        slot: Slot,
        indices: &Range<u32>,
        first_tx_signature: Signature,
        last_tx_signature: Signature,
        num_transactions: u64,
    ) {
        let timestamp_us = monotonic_micros();
        let _ = self.sender.try_send(DatasetSignatureRecord {
            slot,
            indices: indices.clone(),
            first_tx_signature,
            last_tx_signature,
            num_transactions,
            timestamp_us,
        });
    }
}

/// Parquet writer for dataset signature records.
pub type DatasetSignatureCsvWriter = RotatingParquetWriter<DatasetSignatureRecord>;

impl DatasetSignatureCsvWriter {
    pub fn spawn_writer(output_dir: PathBuf, receiver: Receiver<DatasetSignatureRecord>) -> Self {
        Self::spawn(output_dir, "dataset_signatures".into(), receiver)
    }
}

// =============================================================================
// Dataset Execution Records
// =============================================================================

/// Record of execution timing for a confirm_slot_entries call.
#[derive(Debug)]
pub struct DatasetExecutionRecord {
    pub slot: Slot,
    pub shred_start: u64,
    pub shred_end: u64,
    pub execution_start_us: u64,
    pub execution_end_us: u64,
    pub duration_us: u64,
    pub num_entries: u64,
    pub num_transactions: u64,
}

/// Message sent to execution tracking writer thread.
#[derive(Debug)]
pub enum ExecutionTrackingMessage {
    /// Record completed execution for a dataset batch (sent after process_entries returns).
    DatasetComplete {
        slot: Slot,
        shred_start: u64,
        shred_end: u64,
        execution_start_us: u64,
        execution_end_us: u64,
        num_entries: u64,
        num_transactions: u64,
    },
    /// Signal that a slot's execution is complete (after wait_for_completed_scheduler).
    SlotComplete { slot: Slot },
}

/// Sender handle for dataset execution records (used in blockstore_processor).
#[derive(Clone)]
pub struct DatasetExecutionSender {
    sender: Sender<ExecutionTrackingMessage>,
}

impl DatasetExecutionSender {
    pub fn new(sender: Sender<ExecutionTrackingMessage>) -> Self {
        Self { sender }
    }

    /// Record completed execution for a dataset batch. Non-blocking channel send.
    pub fn record_dataset_complete(
        &self,
        slot: Slot,
        shred_start: u64,
        shred_end: u64,
        execution_start_us: u64,
        execution_end_us: u64,
        num_entries: u64,
        num_transactions: u64,
    ) {
        let _ = self
            .sender
            .try_send(ExecutionTrackingMessage::DatasetComplete {
                slot,
                shred_start,
                shred_end,
                execution_start_us,
                execution_end_us,
                num_entries,
                num_transactions,
            });
    }

    /// Signal slot execution complete. Non-blocking channel send.
    pub fn finalize_slot(&self, slot: Slot) {
        let _ = self
            .sender
            .try_send(ExecutionTrackingMessage::SlotComplete { slot });
    }
}

/// Flat row for dataset execution parquet output.
struct DatasetExecutionRow {
    dataset_id: String,
    slot: u64,
    execution_start_us: u64,
    execution_end_us: u64,
    duration_us: u64,
    num_entries: u64,
    num_transactions: u64,
}

impl IntoRecordBatch for DatasetExecutionRow {
    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("dataset_id", DataType::Utf8, false),
            Field::new("slot", DataType::UInt64, false),
            Field::new("execution_start_us", DataType::UInt64, false),
            Field::new("execution_end_us", DataType::UInt64, false),
            Field::new("duration_us", DataType::UInt64, false),
            Field::new("num_entries", DataType::UInt64, false),
            Field::new("num_transactions", DataType::UInt64, false),
        ]))
    }

    fn to_record_batch(records: &[Self]) -> RecordBatch {
        let dataset_ids: Vec<&str> = records.iter().map(|r| r.dataset_id.as_str()).collect();
        let slots: Vec<u64> = records.iter().map(|r| r.slot).collect();
        let starts: Vec<u64> = records.iter().map(|r| r.execution_start_us).collect();
        let ends: Vec<u64> = records.iter().map(|r| r.execution_end_us).collect();
        let durations: Vec<u64> = records.iter().map(|r| r.duration_us).collect();
        let entries: Vec<u64> = records.iter().map(|r| r.num_entries).collect();
        let txs: Vec<u64> = records.iter().map(|r| r.num_transactions).collect();

        RecordBatch::try_new(
            Self::arrow_schema(),
            vec![
                Arc::new(StringArray::from(dataset_ids)),
                Arc::new(UInt64Array::from(slots)),
                Arc::new(UInt64Array::from(starts)),
                Arc::new(UInt64Array::from(ends)),
                Arc::new(UInt64Array::from(durations)),
                Arc::new(UInt64Array::from(entries)),
                Arc::new(UInt64Array::from(txs)),
            ],
        )
        .expect("Failed to create DatasetExecution RecordBatch")
    }
}

/// Background Parquet writer for dataset execution records.
/// Handles the ExecutionTrackingMessage enum dispatch.
pub struct DatasetExecutionCsvWriter {
    thread_handle: Option<JoinHandle<()>>,
}

const EXECUTION_FLUSH_THRESHOLD: usize = 100_000;
const EXECUTION_FLUSH_INTERVAL_SECS: u64 = 60;

impl DatasetExecutionCsvWriter {
    pub fn spawn(output_dir: PathBuf, receiver: Receiver<ExecutionTrackingMessage>) -> Self {
        let handle = thread::Builder::new()
            .name("soldataset_Pqt".to_string())
            .spawn(move || {
                Self::writer_loop(output_dir, receiver);
            })
            .expect("Failed to spawn dataset execution parquet writer thread");

        Self {
            thread_handle: Some(handle),
        }
    }

    fn writer_loop(output_dir: PathBuf, receiver: Receiver<ExecutionTrackingMessage>) {
        use crate::parquet_writer::IntoRecordBatch as _;
        use parquet::arrow::ArrowWriter;
        use parquet::basic::Compression;
        use parquet::file::properties::WriterProperties;

        fs::create_dir_all(&output_dir).expect("Failed to create output dir");

        let prefix = "dataset_execution";
        let mut sequence = Self::find_next_sequence(&output_dir, prefix);
        let mut buffer: Vec<DatasetExecutionRow> = Vec::with_capacity(EXECUTION_FLUSH_THRESHOLD);
        let mut last_flush = Instant::now();

        let flush = |output_dir: &PathBuf,
                     sequence: &mut u64,
                     buffer: &mut Vec<DatasetExecutionRow>| {
            if buffer.is_empty() {
                return;
            }
            let path = output_dir.join(format!("{}_{:06}.parquet", prefix, sequence));
            *sequence += 1;

            let schema = DatasetExecutionRow::arrow_schema();
            let batch = DatasetExecutionRow::to_record_batch(buffer);
            buffer.clear();

            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();

            match std::fs::File::create(&path) {
                Ok(file) => match ArrowWriter::try_new(file, schema, Some(props)) {
                    Ok(mut writer) => {
                        if let Err(e) = writer.write(&batch) {
                            log::error!("Failed to write parquet batch to {:?}: {}", path, e);
                        }
                        if let Err(e) = writer.close() {
                            log::error!("Failed to close parquet file {:?}: {}", path, e);
                        }
                    }
                    Err(e) => log::error!("Failed to create ArrowWriter for {:?}: {}", path, e),
                },
                Err(e) => log::error!("Failed to create parquet file {:?}: {}", path, e),
            }
        };

        loop {
            match receiver.recv_timeout(std::time::Duration::from_secs(1)) {
                Ok(msg) => match msg {
                    ExecutionTrackingMessage::DatasetComplete {
                        slot,
                        shred_start,
                        shred_end,
                        execution_start_us,
                        execution_end_us,
                        num_entries,
                        num_transactions,
                    } => {
                        let duration_us = execution_end_us.saturating_sub(execution_start_us);
                        buffer.push(DatasetExecutionRow {
                            dataset_id: format!("{}_{}_{}", slot, shred_start, shred_end),
                            slot,
                            execution_start_us,
                            execution_end_us,
                            duration_us,
                            num_entries,
                            num_transactions,
                        });
                        if buffer.len() >= EXECUTION_FLUSH_THRESHOLD {
                            flush(&output_dir, &mut sequence, &mut buffer);
                            last_flush = Instant::now();
                        }
                    }
                    ExecutionTrackingMessage::SlotComplete { slot: _ } => {
                        // Don't flush on every slot complete - let the standard
                        // batch threshold (100K records) or timeout (60s) handle it.
                        // Flushing per-slot creates hundreds of thousands of tiny files.
                    }
                },
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if !buffer.is_empty()
                        && last_flush.elapsed().as_secs() >= EXECUTION_FLUSH_INTERVAL_SECS
                    {
                        flush(&output_dir, &mut sequence, &mut buffer);
                        last_flush = Instant::now();
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    if !buffer.is_empty() {
                        flush(&output_dir, &mut sequence, &mut buffer);
                    }
                    break;
                }
            }
        }
    }

    fn find_next_sequence(output_dir: &PathBuf, prefix: &str) -> u64 {
        let mut max_seq = 0u64;
        if let Ok(entries) = fs::read_dir(output_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with(prefix) && name.ends_with(".parquet") {
                    if let Some(seq_str) = name
                        .strip_prefix(&format!("{}_", prefix))
                        .and_then(|s| s.strip_suffix(".parquet"))
                    {
                        if let Ok(seq) = seq_str.parse::<u64>() {
                            max_seq = max_seq.max(seq + 1);
                        }
                    }
                }
            }
        }
        max_seq
    }

    pub fn join(mut self) {
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for DatasetExecutionCsvWriter {
    fn drop(&mut self) {
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

// =============================================================================
// Tick Records
// =============================================================================

/// Record of a tick with transaction context from the preceding entry.
#[derive(Debug)]
pub struct TickRecord {
    pub slot: Slot,
    pub tick_height: u64,
    pub tick_hash: Hash,
    pub first_tx_signature: Option<Signature>,
    pub last_tx_signature: Option<Signature>,
    pub num_transactions: u64,
    pub timestamp_us: u64,
}

impl IntoRecordBatch for TickRecord {
    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("slot", DataType::UInt64, false),
            Field::new("tick_height", DataType::UInt64, false),
            Field::new("tick_hash", DataType::Utf8, false),
            Field::new("first_tx_signature", DataType::Utf8, false),
            Field::new("last_tx_signature", DataType::Utf8, false),
            Field::new("num_transactions", DataType::UInt64, false),
            Field::new("timestamp_us", DataType::UInt64, false),
        ]))
    }

    fn to_record_batch(records: &[Self]) -> RecordBatch {
        let slots: Vec<u64> = records.iter().map(|r| r.slot).collect();
        let heights: Vec<u64> = records.iter().map(|r| r.tick_height).collect();
        let hashes: Vec<String> = records.iter().map(|r| r.tick_hash.to_string()).collect();
        let first_sigs: Vec<String> = records
            .iter()
            .map(|r| {
                r.first_tx_signature
                    .map(|s| s.to_string())
                    .unwrap_or_default()
            })
            .collect();
        let last_sigs: Vec<String> = records
            .iter()
            .map(|r| {
                r.last_tx_signature
                    .map(|s| s.to_string())
                    .unwrap_or_default()
            })
            .collect();
        let num_txs: Vec<u64> = records.iter().map(|r| r.num_transactions).collect();
        let timestamps: Vec<u64> = records.iter().map(|r| r.timestamp_us).collect();

        RecordBatch::try_new(
            Self::arrow_schema(),
            vec![
                Arc::new(UInt64Array::from(slots)),
                Arc::new(UInt64Array::from(heights)),
                Arc::new(StringArray::from(
                    hashes.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    first_sigs.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    last_sigs.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(num_txs)),
                Arc::new(UInt64Array::from(timestamps)),
            ],
        )
        .expect("Failed to create TickRecord RecordBatch")
    }
}

/// Sender handle for tick records.
#[derive(Clone)]
pub struct TickTrackingSender {
    sender: Sender<TickRecord>,
}

impl TickTrackingSender {
    pub fn new(sender: Sender<TickRecord>) -> Self {
        Self { sender }
    }

    pub fn record(
        &self,
        slot: Slot,
        tick_height: u64,
        tick_hash: Hash,
        first_tx_signature: Option<Signature>,
        last_tx_signature: Option<Signature>,
        num_transactions: u64,
    ) {
        let timestamp_us = monotonic_micros();
        let _ = self.sender.try_send(TickRecord {
            slot,
            tick_height,
            tick_hash,
            first_tx_signature,
            last_tx_signature,
            num_transactions,
            timestamp_us,
        });
    }
}

/// Parquet writer for tick records.
pub type TickTrackingCsvWriter = RotatingParquetWriter<TickRecord>;

impl TickTrackingCsvWriter {
    pub fn spawn_writer(output_dir: PathBuf, receiver: Receiver<TickRecord>) -> Self {
        Self::spawn(output_dir, "ticks".into(), receiver)
    }
}

// =============================================================================
// Replay Batch Tracking
// =============================================================================

/// Replay batch event type.
#[derive(Debug, Clone, Copy)]
pub enum ReplayEventType {
    Wakeup,
    Finish,
}

/// Record of a replay batch event (wakeup or finish).
#[derive(Debug)]
pub struct ReplayBatchRecord {
    pub slot: Slot,
    pub timestamp_us: u64,
    pub event_type: ReplayEventType,
    /// Number of entries processed (only set for Finish events)
    pub num_entries: u64,
    /// Number of transactions processed (only set for Finish events)
    pub num_transactions: u64,
    /// Duration of replay in microseconds (only set for Finish events)
    pub duration_us: u64,
}

impl IntoRecordBatch for ReplayBatchRecord {
    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("slot", DataType::UInt64, false),
            Field::new("timestamp_us", DataType::UInt64, false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("num_entries", DataType::UInt64, false),
            Field::new("num_transactions", DataType::UInt64, false),
            Field::new("duration_us", DataType::UInt64, false),
        ]))
    }

    fn to_record_batch(records: &[Self]) -> RecordBatch {
        let slots: Vec<u64> = records.iter().map(|r| r.slot).collect();
        let timestamps: Vec<u64> = records.iter().map(|r| r.timestamp_us).collect();
        let event_types: Vec<&str> = records
            .iter()
            .map(|r| match r.event_type {
                ReplayEventType::Wakeup => "wakeup",
                ReplayEventType::Finish => "finish",
            })
            .collect();
        let entries: Vec<u64> = records.iter().map(|r| r.num_entries).collect();
        let txs: Vec<u64> = records.iter().map(|r| r.num_transactions).collect();
        let durations: Vec<u64> = records.iter().map(|r| r.duration_us).collect();

        RecordBatch::try_new(
            Self::arrow_schema(),
            vec![
                Arc::new(UInt64Array::from(slots)),
                Arc::new(UInt64Array::from(timestamps)),
                Arc::new(StringArray::from(event_types)),
                Arc::new(UInt64Array::from(entries)),
                Arc::new(UInt64Array::from(txs)),
                Arc::new(UInt64Array::from(durations)),
            ],
        )
        .expect("Failed to create ReplayBatch RecordBatch")
    }
}

/// Sender handle for replay batch records (used in replay_stage).
#[derive(Clone)]
pub struct ReplayBatchSender {
    sender: Sender<ReplayBatchRecord>,
}

impl ReplayBatchSender {
    pub fn new(sender: Sender<ReplayBatchRecord>) -> Self {
        Self { sender }
    }

    /// Record replay wakeup for a slot. Non-blocking channel send.
    pub fn record_wakeup(&self, slot: Slot) {
        let timestamp_us = monotonic_micros();
        let _ = self.sender.try_send(ReplayBatchRecord {
            slot,
            timestamp_us,
            event_type: ReplayEventType::Wakeup,
            num_entries: 0,
            num_transactions: 0,
            duration_us: 0,
        });
    }

    /// Record replay finish for a slot. Non-blocking channel send.
    pub fn record_finish(
        &self,
        slot: Slot,
        num_entries: u64,
        num_transactions: u64,
        duration_us: u64,
    ) {
        let timestamp_us = monotonic_micros();
        let _ = self.sender.try_send(ReplayBatchRecord {
            slot,
            timestamp_us,
            event_type: ReplayEventType::Finish,
            num_entries,
            num_transactions,
            duration_us,
        });
    }
}

/// Parquet writer for replay batch records.
pub type ReplayBatchCsvWriter = RotatingParquetWriter<ReplayBatchRecord>;

impl ReplayBatchCsvWriter {
    pub fn spawn_writer(output_dir: PathBuf, receiver: Receiver<ReplayBatchRecord>) -> Self {
        Self::spawn(output_dir, "replay_batches".into(), receiver)
    }
}

// =============================================================================
// Blockstore Insertion Phase Tracking
// =============================================================================

/// Phase types for blockstore shred insertion.
#[derive(Debug, Clone, Copy)]
pub enum BlockstoreInsertPhase {
    /// Acquiring the insert_shreds_lock
    LockAcquire,
    /// Attempting to insert shreds (check_insert_data_shred, check_insert_coding_shred)
    AttemptInsertion,
    /// Erasure recovery (handle_shred_recovery)
    ShredRecovery,
    /// Updating slot chaining metadata (handle_chaining)
    Chaining,
    /// Committing updates to write batch (commit_updates_to_write_batch)
    CommitUpdates,
    /// Writing batch to RocksDB (write_batch)
    WriteBatch,
    /// Sending signals for newly completed slots
    SignalCompletion,
}

impl BlockstoreInsertPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LockAcquire => "lock_acquire",
            Self::AttemptInsertion => "attempt_insertion",
            Self::ShredRecovery => "shred_recovery",
            Self::Chaining => "chaining",
            Self::CommitUpdates => "commit_updates",
            Self::WriteBatch => "write_batch",
            Self::SignalCompletion => "signal_completion",
        }
    }
}

/// Record of a blockstore insertion phase.
#[derive(Debug)]
pub struct BlockstoreInsertPhaseRecord {
    /// Batch ID (unique per insert_shreds call)
    pub batch_id: u64,
    /// Primary slot being inserted (first slot in batch)
    pub slot: Slot,
    /// Phase of insertion
    pub phase: BlockstoreInsertPhase,
    /// Start timestamp in microseconds
    pub start_us: u64,
    /// End timestamp in microseconds
    pub end_us: u64,
    /// Number of shreds processed in this phase (if applicable)
    pub num_shreds: u64,
    /// Number of shreds recovered (only for ShredRecovery phase)
    pub num_recovered: u64,
    /// Number of completed data sets (only for CommitUpdates phase)
    pub num_completed_datasets: u64,
}

impl IntoRecordBatch for BlockstoreInsertPhaseRecord {
    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("batch_id", DataType::UInt64, false),
            Field::new("slot", DataType::UInt64, false),
            Field::new("phase", DataType::Utf8, false),
            Field::new("start_us", DataType::UInt64, false),
            Field::new("end_us", DataType::UInt64, false),
            Field::new("duration_us", DataType::UInt64, false),
            Field::new("num_shreds", DataType::UInt64, false),
            Field::new("num_recovered", DataType::UInt64, false),
            Field::new("num_completed_datasets", DataType::UInt64, false),
        ]))
    }

    fn to_record_batch(records: &[Self]) -> RecordBatch {
        let batch_ids: Vec<u64> = records.iter().map(|r| r.batch_id).collect();
        let slots: Vec<u64> = records.iter().map(|r| r.slot).collect();
        let phases: Vec<&str> = records.iter().map(|r| r.phase.as_str()).collect();
        let starts: Vec<u64> = records.iter().map(|r| r.start_us).collect();
        let ends: Vec<u64> = records.iter().map(|r| r.end_us).collect();
        let durations: Vec<u64> = records
            .iter()
            .map(|r| r.end_us.saturating_sub(r.start_us))
            .collect();
        let shreds: Vec<u64> = records.iter().map(|r| r.num_shreds).collect();
        let recovered: Vec<u64> = records.iter().map(|r| r.num_recovered).collect();
        let completed: Vec<u64> = records.iter().map(|r| r.num_completed_datasets).collect();

        RecordBatch::try_new(
            Self::arrow_schema(),
            vec![
                Arc::new(UInt64Array::from(batch_ids)),
                Arc::new(UInt64Array::from(slots)),
                Arc::new(StringArray::from(phases)),
                Arc::new(UInt64Array::from(starts)),
                Arc::new(UInt64Array::from(ends)),
                Arc::new(UInt64Array::from(durations)),
                Arc::new(UInt64Array::from(shreds)),
                Arc::new(UInt64Array::from(recovered)),
                Arc::new(UInt64Array::from(completed)),
            ],
        )
        .expect("Failed to create BlockstoreInsertPhase RecordBatch")
    }
}

/// Sender handle for blockstore insertion phase records.
#[derive(Clone)]
pub struct BlockstoreInsertPhaseSender {
    sender: Sender<BlockstoreInsertPhaseRecord>,
    batch_counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl BlockstoreInsertPhaseSender {
    pub fn new(sender: Sender<BlockstoreInsertPhaseRecord>) -> Self {
        Self {
            sender,
            batch_counter: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Generate a new unique batch ID for this insertion.
    pub fn next_batch_id(&self) -> u64 {
        self.batch_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Record a phase completion. Non-blocking channel send.
    pub fn record_phase(
        &self,
        batch_id: u64,
        slot: Slot,
        phase: BlockstoreInsertPhase,
        start_us: u64,
        end_us: u64,
        num_shreds: u64,
        num_recovered: u64,
        num_completed_datasets: u64,
    ) {
        let _ = self.sender.try_send(BlockstoreInsertPhaseRecord {
            batch_id,
            slot,
            phase,
            start_us,
            end_us,
            num_shreds,
            num_recovered,
            num_completed_datasets,
        });
    }
}

/// Parquet writer for blockstore insertion phase records.
pub type BlockstoreInsertPhaseCsvWriter = RotatingParquetWriter<BlockstoreInsertPhaseRecord>;

impl BlockstoreInsertPhaseCsvWriter {
    pub fn spawn_writer(
        output_dir: PathBuf,
        receiver: Receiver<BlockstoreInsertPhaseRecord>,
    ) -> Self {
        Self::spawn(output_dir, "blockstore_insert_phases".into(), receiver)
    }
}

// =============================================================================
// Shred Insert Tracking
// =============================================================================

/// Lightweight record for individual shred insertions.
/// No allocations - just primitive fields.
#[derive(Debug, Clone, Copy)]
pub struct ShredInsertRecord {
    pub batch_id: u64,
    pub slot: Slot,
    pub shred_index: u32,
    pub is_data: bool,
    pub is_recovered: bool,
    pub timestamp_us: u64,
}

impl IntoRecordBatch for ShredInsertRecord {
    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("batch_id", DataType::UInt64, false),
            Field::new("slot", DataType::UInt64, false),
            Field::new("shred_index", DataType::UInt32, false),
            Field::new("is_data", DataType::Boolean, false),
            Field::new("is_recovered", DataType::Boolean, false),
            Field::new("timestamp_us", DataType::UInt64, false),
        ]))
    }

    fn to_record_batch(records: &[Self]) -> RecordBatch {
        let batch_ids: Vec<u64> = records.iter().map(|r| r.batch_id).collect();
        let slots: Vec<u64> = records.iter().map(|r| r.slot).collect();
        let indices: Vec<u32> = records.iter().map(|r| r.shred_index).collect();
        let is_data: Vec<bool> = records.iter().map(|r| r.is_data).collect();
        let is_recovered: Vec<bool> = records.iter().map(|r| r.is_recovered).collect();
        let timestamps: Vec<u64> = records.iter().map(|r| r.timestamp_us).collect();

        RecordBatch::try_new(
            Self::arrow_schema(),
            vec![
                Arc::new(UInt64Array::from(batch_ids)),
                Arc::new(UInt64Array::from(slots)),
                Arc::new(UInt32Array::from(indices)),
                Arc::new(BooleanArray::from(is_data)),
                Arc::new(BooleanArray::from(is_recovered)),
                Arc::new(UInt64Array::from(timestamps)),
            ],
        )
        .expect("Failed to create ShredInsert RecordBatch")
    }
}

/// Sender for shred insert records. Shares batch_counter with phase sender.
#[derive(Clone)]
pub struct ShredInsertSender {
    sender: Sender<ShredInsertRecord>,
}

impl ShredInsertSender {
    pub fn new(sender: Sender<ShredInsertRecord>) -> Self {
        Self { sender }
    }

    /// Record a shred insertion. Non-blocking, no allocations.
    #[inline]
    pub fn record(
        &self,
        batch_id: u64,
        slot: Slot,
        shred_index: u32,
        is_data: bool,
        is_recovered: bool,
    ) {
        let timestamp_us = monotonic_micros();
        let _ = self.sender.try_send(ShredInsertRecord {
            batch_id,
            slot,
            shred_index,
            is_data,
            is_recovered,
            timestamp_us,
        });
    }
}

/// Parquet writer for shred insert records.
pub type ShredInsertCsvWriter = RotatingParquetWriter<ShredInsertRecord>;

impl ShredInsertCsvWriter {
    pub fn spawn_writer(output_dir: PathBuf, receiver: Receiver<ShredInsertRecord>) -> Self {
        Self::spawn(output_dir, "shred_inserts".into(), receiver)
    }
}

// =============================================================================
// Transaction Execution Tracking
// =============================================================================

/// Record of a single transaction execution with start/end timestamps.
#[derive(Debug, Clone)]
pub struct TxExecutionRecord {
    pub slot: Slot,
    pub signature: Signature,
    pub execution_start_us: u64,
    pub execution_end_us: u64,
}

impl IntoRecordBatch for TxExecutionRecord {
    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("slot", DataType::UInt64, false),
            Field::new("signature", DataType::Utf8, false),
            Field::new("execution_start_us", DataType::UInt64, false),
            Field::new("execution_end_us", DataType::UInt64, false),
            Field::new("duration_us", DataType::UInt64, false),
        ]))
    }

    fn to_record_batch(records: &[Self]) -> RecordBatch {
        let slots: Vec<u64> = records.iter().map(|r| r.slot).collect();
        let sigs: Vec<String> = records.iter().map(|r| r.signature.to_string()).collect();
        let starts: Vec<u64> = records.iter().map(|r| r.execution_start_us).collect();
        let ends: Vec<u64> = records.iter().map(|r| r.execution_end_us).collect();
        let durations: Vec<u64> = records
            .iter()
            .map(|r| r.execution_end_us.saturating_sub(r.execution_start_us))
            .collect();

        RecordBatch::try_new(
            Self::arrow_schema(),
            vec![
                Arc::new(UInt64Array::from(slots)),
                Arc::new(StringArray::from(
                    sigs.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(starts)),
                Arc::new(UInt64Array::from(ends)),
                Arc::new(UInt64Array::from(durations)),
            ],
        )
        .expect("Failed to create TxExecution RecordBatch")
    }
}

/// Sender handle for transaction execution records.
#[derive(Clone, Debug)]
pub struct TxExecutionSender {
    sender: Sender<TxExecutionRecord>,
}

impl TxExecutionSender {
    pub fn new(sender: Sender<TxExecutionRecord>) -> Self {
        Self { sender }
    }

    /// Record transaction execution start and end. Non-blocking channel send.
    pub fn record(
        &self,
        slot: Slot,
        signature: Signature,
        execution_start_us: u64,
        execution_end_us: u64,
    ) {
        let _ = self.sender.try_send(TxExecutionRecord {
            slot,
            signature,
            execution_start_us,
            execution_end_us,
        });
    }
}

/// Parquet writer for transaction execution records.
pub type TxExecutionCsvWriter = RotatingParquetWriter<TxExecutionRecord>;

impl TxExecutionCsvWriter {
    pub fn spawn_writer(output_dir: PathBuf, receiver: Receiver<TxExecutionRecord>) -> Self {
        Self::spawn(output_dir, "tx_execution".into(), receiver)
    }
}

// =============================================================================
// Transaction Cost/Priority Tracking
// =============================================================================

/// Record of per-transaction cost model breakdown and priority score.
#[derive(Debug, Clone)]
pub struct TxCostPriorityRecord {
    pub slot: Slot,
    pub signature: Signature,
    pub priority: u64,
    pub reward: u64,
    pub cost: u64,
    pub signature_cost: u64,
    pub write_lock_cost: u64,
    pub data_bytes_cost: u64,
    pub programs_execution_cost: u64,
    pub loaded_accounts_data_size_cost: u64,
    pub allocated_accounts_data_size: u64,
    pub is_simple_vote: bool,
    pub timestamp_us: u64,
}

impl IntoRecordBatch for TxCostPriorityRecord {
    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("slot", DataType::UInt64, false),
            Field::new("signature", DataType::Utf8, false),
            Field::new("priority", DataType::UInt64, false),
            Field::new("reward", DataType::UInt64, false),
            Field::new("cost", DataType::UInt64, false),
            Field::new("signature_cost", DataType::UInt64, false),
            Field::new("write_lock_cost", DataType::UInt64, false),
            Field::new("data_bytes_cost", DataType::UInt64, false),
            Field::new("programs_execution_cost", DataType::UInt64, false),
            Field::new("loaded_accounts_data_size_cost", DataType::UInt64, false),
            Field::new("allocated_accounts_data_size", DataType::UInt64, false),
            Field::new("is_simple_vote", DataType::Boolean, false),
            Field::new("timestamp_us", DataType::UInt64, false),
        ]))
    }

    fn to_record_batch(records: &[Self]) -> RecordBatch {
        let slots: Vec<u64> = records.iter().map(|r| r.slot).collect();
        let sigs: Vec<String> = records.iter().map(|r| r.signature.to_string()).collect();
        let priorities: Vec<u64> = records.iter().map(|r| r.priority).collect();
        let rewards: Vec<u64> = records.iter().map(|r| r.reward).collect();
        let costs: Vec<u64> = records.iter().map(|r| r.cost).collect();
        let sig_costs: Vec<u64> = records.iter().map(|r| r.signature_cost).collect();
        let write_lock_costs: Vec<u64> = records.iter().map(|r| r.write_lock_cost).collect();
        let data_bytes_costs: Vec<u64> = records.iter().map(|r| r.data_bytes_cost).collect();
        let programs_exec_costs: Vec<u64> =
            records.iter().map(|r| r.programs_execution_cost).collect();
        let loaded_data_costs: Vec<u64> = records
            .iter()
            .map(|r| r.loaded_accounts_data_size_cost)
            .collect();
        let alloc_sizes: Vec<u64> = records
            .iter()
            .map(|r| r.allocated_accounts_data_size)
            .collect();
        let simple_votes: Vec<bool> = records.iter().map(|r| r.is_simple_vote).collect();
        let timestamps: Vec<u64> = records.iter().map(|r| r.timestamp_us).collect();

        RecordBatch::try_new(
            Self::arrow_schema(),
            vec![
                Arc::new(UInt64Array::from(slots)),
                Arc::new(StringArray::from(
                    sigs.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(priorities)),
                Arc::new(UInt64Array::from(rewards)),
                Arc::new(UInt64Array::from(costs)),
                Arc::new(UInt64Array::from(sig_costs)),
                Arc::new(UInt64Array::from(write_lock_costs)),
                Arc::new(UInt64Array::from(data_bytes_costs)),
                Arc::new(UInt64Array::from(programs_exec_costs)),
                Arc::new(UInt64Array::from(loaded_data_costs)),
                Arc::new(UInt64Array::from(alloc_sizes)),
                Arc::new(BooleanArray::from(simple_votes)),
                Arc::new(UInt64Array::from(timestamps)),
            ],
        )
        .expect("Failed to create TxCostPriority RecordBatch")
    }
}

/// Sender handle for transaction cost/priority records.
#[derive(Clone, Debug)]
pub struct TxCostPrioritySender {
    sender: Sender<TxCostPriorityRecord>,
}

impl TxCostPrioritySender {
    pub fn new(sender: Sender<TxCostPriorityRecord>) -> Self {
        Self { sender }
    }

    /// Record transaction cost/priority breakdown. Non-blocking channel send.
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &self,
        slot: Slot,
        signature: Signature,
        priority: u64,
        reward: u64,
        cost: u64,
        signature_cost: u64,
        write_lock_cost: u64,
        data_bytes_cost: u64,
        programs_execution_cost: u64,
        loaded_accounts_data_size_cost: u64,
        allocated_accounts_data_size: u64,
        is_simple_vote: bool,
    ) {
        let timestamp_us = monotonic_micros();
        let _ = self.sender.try_send(TxCostPriorityRecord {
            slot,
            signature,
            priority,
            reward,
            cost,
            signature_cost,
            write_lock_cost,
            data_bytes_cost,
            programs_execution_cost,
            loaded_accounts_data_size_cost,
            allocated_accounts_data_size,
            is_simple_vote,
            timestamp_us,
        });
    }
}

/// Parquet writer for transaction cost/priority records.
pub type TxCostPriorityCsvWriter = RotatingParquetWriter<TxCostPriorityRecord>;

impl TxCostPriorityCsvWriter {
    pub fn spawn_writer(output_dir: PathBuf, receiver: Receiver<TxCostPriorityRecord>) -> Self {
        Self::spawn(output_dir, "tx_cost_priority".into(), receiver)
    }
}
