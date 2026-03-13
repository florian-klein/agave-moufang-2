//! Window service event tracing for debugging and latency analysis.
//!
//! When enabled, this module captures timing events throughout the window service
//! processing pipeline, allowing detailed analysis of where time is spent.

use {
    crate::dataset_tracking::monotonic_micros,
    crate::parquet_writer::{IntoRecordBatch, RotatingParquetWriter},
    arrow::{
        array::*,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    crossbeam_channel::Sender,
    solana_clock::Slot,
    std::{
        path::PathBuf,
        sync::Arc,
    },
};

/// Types of events in window service processing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowServiceEventType {
    /// Start of run_insert iteration
    RunInsertStart,
    /// Waiting to receive from channel
    RecvWaitStart,
    RecvWaitEnd,
    /// Merging additional batches from channel
    BatchMergeStart,
    BatchMergeEnd,
    /// Deserializing shreds from packets
    HandlePacketsStart,
    HandlePacketsEnd,
    /// Building payload index for entry cache
    PayloadBuildStart,
    PayloadBuildEnd,
    /// Recording first shred latency event
    LatencyEventRecord,
    /// Inserting shreds into blockstore
    BlockstoreInsertStart,
    BlockstoreInsertEnd,
    /// Populating entry cache
    EntryCacheStart,
    EntryCacheEnd,
    /// Recording shred arrivals for CSV
    ShredArrivalRecordStart,
    ShredArrivalRecordEnd,
    /// Sending signal to replay stage
    SignalStart,
    SignalEnd,
    /// End of run_insert iteration
    RunInsertEnd,
}

impl WindowServiceEventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::RunInsertStart => "run_insert_start",
            Self::RecvWaitStart => "recv_wait_start",
            Self::RecvWaitEnd => "recv_wait_end",
            Self::BatchMergeStart => "batch_merge_start",
            Self::BatchMergeEnd => "batch_merge_end",
            Self::HandlePacketsStart => "handle_packets_start",
            Self::HandlePacketsEnd => "handle_packets_end",
            Self::PayloadBuildStart => "payload_build_start",
            Self::PayloadBuildEnd => "payload_build_end",
            Self::LatencyEventRecord => "latency_event_record",
            Self::BlockstoreInsertStart => "blockstore_insert_start",
            Self::BlockstoreInsertEnd => "blockstore_insert_end",
            Self::EntryCacheStart => "entry_cache_start",
            Self::EntryCacheEnd => "entry_cache_end",
            Self::ShredArrivalRecordStart => "shred_arrival_record_start",
            Self::ShredArrivalRecordEnd => "shred_arrival_record_end",
            Self::SignalStart => "signal_start",
            Self::SignalEnd => "signal_end",
            Self::RunInsertEnd => "run_insert_end",
        }
    }
}

/// Record of a window service event
#[derive(Debug, Clone)]
pub struct WindowServiceEvent {
    /// Unix timestamp in microseconds
    pub timestamp_us: u64,
    /// Event type
    pub event_type: WindowServiceEventType,
    /// Batch ID (unique per run_insert call)
    pub batch_id: u64,
    /// Number of shreds in this batch (if applicable)
    pub num_shreds: Option<usize>,
    /// Slots involved (comma-separated list of unique slots)
    pub slots: Option<String>,
    /// Number of completed data sets (if applicable)
    pub num_completed_data_sets: Option<usize>,
    /// Additional context string
    pub context: Option<String>,
}

impl WindowServiceEvent {
    /// Create a new event with current timestamp
    pub fn new(event_type: WindowServiceEventType, batch_id: u64) -> Self {
        let timestamp_us = monotonic_micros();

        Self {
            timestamp_us,
            event_type,
            batch_id,
            num_shreds: None,
            slots: None,
            num_completed_data_sets: None,
            context: None,
        }
    }

    /// Add number of shreds
    pub fn with_num_shreds(mut self, num_shreds: usize) -> Self {
        self.num_shreds = Some(num_shreds);
        self
    }

    /// Add slots information
    pub fn with_slots(mut self, slots: impl IntoIterator<Item = Slot>) -> Self {
        let slots_str: String = slots
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(";");
        if !slots_str.is_empty() {
            self.slots = Some(slots_str);
        }
        self
    }

    /// Add number of completed data sets
    pub fn with_completed_data_sets(mut self, count: usize) -> Self {
        self.num_completed_data_sets = Some(count);
        self
    }

    /// Add context string
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context = Some(context.into());
        self
    }
}

impl IntoRecordBatch for WindowServiceEvent {
    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp_us", DataType::UInt64, false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("batch_id", DataType::UInt64, false),
            Field::new("num_shreds", DataType::UInt64, true),
            Field::new("slots", DataType::Utf8, true),
            Field::new("num_completed_data_sets", DataType::UInt64, true),
            Field::new("context", DataType::Utf8, true),
        ]))
    }

    fn to_record_batch(records: &[Self]) -> RecordBatch {
        let timestamps: Vec<u64> = records.iter().map(|r| r.timestamp_us).collect();
        let event_types: Vec<&str> = records.iter().map(|r| r.event_type.as_str()).collect();
        let batch_ids: Vec<u64> = records.iter().map(|r| r.batch_id).collect();
        let num_shreds: Vec<Option<u64>> = records
            .iter()
            .map(|r| r.num_shreds.map(|n| n as u64))
            .collect();
        let slots: Vec<Option<&str>> = records.iter().map(|r| r.slots.as_deref()).collect();
        let completed: Vec<Option<u64>> = records
            .iter()
            .map(|r| r.num_completed_data_sets.map(|n| n as u64))
            .collect();
        let contexts: Vec<Option<&str>> = records.iter().map(|r| r.context.as_deref()).collect();

        RecordBatch::try_new(
            Self::arrow_schema(),
            vec![
                Arc::new(UInt64Array::from(timestamps)),
                Arc::new(StringArray::from(event_types)),
                Arc::new(UInt64Array::from(batch_ids)),
                Arc::new(UInt64Array::from(num_shreds)),
                Arc::new(StringArray::from(slots)),
                Arc::new(UInt64Array::from(completed)),
                Arc::new(StringArray::from(contexts)),
            ],
        )
        .expect("Failed to create WindowServiceEvent RecordBatch")
    }
}

/// Sender for window service events (non-blocking)
#[derive(Clone)]
pub struct WindowServiceEventSender {
    sender: Sender<WindowServiceEvent>,
    batch_id_counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl WindowServiceEventSender {
    pub fn new(sender: Sender<WindowServiceEvent>) -> Self {
        Self {
            sender,
            batch_id_counter: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Get the next batch ID
    pub fn next_batch_id(&self) -> u64 {
        self.batch_id_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Record an event (non-blocking, drops if channel full)
    pub fn record(&self, event: WindowServiceEvent) {
        let _ = self.sender.try_send(event);
    }

    /// Record a simple event with just type and batch_id
    pub fn record_simple(&self, event_type: WindowServiceEventType, batch_id: u64) {
        self.record(WindowServiceEvent::new(event_type, batch_id));
    }
}

/// Parquet writer for window service events
pub type WindowServiceEventCsvWriter = RotatingParquetWriter<WindowServiceEvent>;

/// Create a window service event tracing channel
pub fn create_window_service_tracer(
    output_dir: PathBuf,
) -> (WindowServiceEventSender, WindowServiceEventCsvWriter) {
    let (tx, rx) = crossbeam_channel::bounded(100_000);
    let sender = WindowServiceEventSender::new(tx);
    let writer = WindowServiceEventCsvWriter::spawn(
        output_dir,
        "window_service_events".into(),
        rx,
    );
    (sender, writer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_window_service_event_creation() {
        let event = WindowServiceEvent::new(WindowServiceEventType::RunInsertStart, 42)
            .with_num_shreds(10)
            .with_slots([100, 101, 102])
            .with_context("test context");

        assert_eq!(event.batch_id, 42);
        assert_eq!(event.event_type, WindowServiceEventType::RunInsertStart);
        assert_eq!(event.num_shreds, Some(10));
        assert_eq!(event.slots, Some("100;101;102".to_string()));
        assert_eq!(event.context, Some("test context".to_string()));
        assert!(event.timestamp_us > 0);
    }

    #[test]
    fn test_window_service_parquet_writer() {
        let temp_dir = TempDir::new().unwrap();
        let (sender, writer) = create_window_service_tracer(temp_dir.path().to_path_buf());

        // Get a batch ID
        let batch_id = sender.next_batch_id();

        // Send some events
        sender.record(
            WindowServiceEvent::new(WindowServiceEventType::RunInsertStart, batch_id)
                .with_num_shreds(5),
        );

        sender.record_simple(WindowServiceEventType::RecvWaitStart, batch_id);

        sender.record(
            WindowServiceEvent::new(WindowServiceEventType::RunInsertEnd, batch_id)
                .with_completed_data_sets(2),
        );

        // Drop sender to close channel and let writer finish
        drop(sender);
        writer.join();

        // Verify parquet file was written
        let parquet_files: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .ends_with(".parquet")
            })
            .collect();
        assert!(!parquet_files.is_empty());
    }

    #[test]
    fn test_event_type_as_str() {
        assert_eq!(
            WindowServiceEventType::RunInsertStart.as_str(),
            "run_insert_start"
        );
        assert_eq!(
            WindowServiceEventType::BlockstoreInsertEnd.as_str(),
            "blockstore_insert_end"
        );
    }
}
