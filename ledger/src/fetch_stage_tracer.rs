//! Fetch stage arrival tracing for debugging and latency analysis.
//!
//! When enabled, this module captures the actual arrival time of shred packets
//! at the UDP socket level (in fetch stage), before any processing like
//! deduplication or signature verification.

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
        net::IpAddr,
        path::PathBuf,
        sync::Arc,
    },
};

/// Record of a shred arrival at fetch stage (UDP socket level)
#[derive(Debug, Clone)]
pub struct FetchStageArrival {
    /// Unix timestamp in microseconds when packet was received
    pub timestamp_us: u64,
    /// Slot from shred header (if parseable)
    pub slot: Option<Slot>,
    /// Shred index from header (if parseable)
    pub shred_index: Option<u32>,
    /// Whether this is a data shred (vs coding)
    pub is_data: Option<bool>,
    /// Source IP address
    pub source_ip: IpAddr,
    /// Source port
    pub source_port: u16,
    /// Packet size in bytes
    pub packet_size: usize,
    /// Whether this is a repair packet
    pub is_repair: bool,
    /// Whether this arrived via QUIC (vs UDP)
    pub is_quic: bool,
}

impl FetchStageArrival {
    /// Create a new arrival record with current timestamp
    pub fn new(
        slot: Option<Slot>,
        shred_index: Option<u32>,
        is_data: Option<bool>,
        source_ip: IpAddr,
        source_port: u16,
        packet_size: usize,
        is_repair: bool,
        is_quic: bool,
    ) -> Self {
        let timestamp_us = monotonic_micros();

        Self {
            timestamp_us,
            slot,
            shred_index,
            is_data,
            source_ip,
            source_port,
            packet_size,
            is_repair,
            is_quic,
        }
    }

    /// Create with explicit timestamp (for testing or when timestamp already captured)
    pub fn with_timestamp(
        timestamp_us: u64,
        slot: Option<Slot>,
        shred_index: Option<u32>,
        is_data: Option<bool>,
        source_ip: IpAddr,
        source_port: u16,
        packet_size: usize,
        is_repair: bool,
        is_quic: bool,
    ) -> Self {
        Self {
            timestamp_us,
            slot,
            shred_index,
            is_data,
            source_ip,
            source_port,
            packet_size,
            is_repair,
            is_quic,
        }
    }
}

impl IntoRecordBatch for FetchStageArrival {
    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("timestamp_us", DataType::UInt64, false),
            Field::new("slot", DataType::UInt64, true),
            Field::new("shred_index", DataType::UInt32, true),
            Field::new("is_data", DataType::Boolean, true),
            Field::new("source_ip", DataType::Utf8, false),
            Field::new("source_port", DataType::UInt16, false),
            Field::new("packet_size", DataType::UInt64, false),
            Field::new("is_repair", DataType::Boolean, false),
            Field::new("is_quic", DataType::Boolean, false),
        ]))
    }

    fn to_record_batch(records: &[Self]) -> RecordBatch {
        let timestamps: Vec<u64> = records.iter().map(|r| r.timestamp_us).collect();
        let slots: Vec<Option<u64>> = records.iter().map(|r| r.slot).collect();
        let indices: Vec<Option<u32>> = records.iter().map(|r| r.shred_index).collect();
        let is_data: Vec<Option<bool>> = records.iter().map(|r| r.is_data).collect();
        let ips: Vec<String> = records.iter().map(|r| r.source_ip.to_string()).collect();
        let ports: Vec<u16> = records.iter().map(|r| r.source_port).collect();
        let sizes: Vec<u64> = records.iter().map(|r| r.packet_size as u64).collect();
        let is_repair: Vec<bool> = records.iter().map(|r| r.is_repair).collect();
        let is_quic: Vec<bool> = records.iter().map(|r| r.is_quic).collect();

        RecordBatch::try_new(
            Self::arrow_schema(),
            vec![
                Arc::new(UInt64Array::from(timestamps)),
                Arc::new(UInt64Array::from(slots)),
                Arc::new(UInt32Array::from(indices)),
                Arc::new(BooleanArray::from(is_data)),
                Arc::new(StringArray::from(
                    ips.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(UInt16Array::from(ports)),
                Arc::new(UInt64Array::from(sizes)),
                Arc::new(BooleanArray::from(is_repair)),
                Arc::new(BooleanArray::from(is_quic)),
            ],
        )
        .expect("Failed to create FetchStageArrival RecordBatch")
    }
}

/// Sender for fetch stage arrivals (non-blocking)
#[derive(Clone)]
pub struct FetchStageArrivalSender {
    sender: Sender<FetchStageArrival>,
}

impl FetchStageArrivalSender {
    pub fn new(sender: Sender<FetchStageArrival>) -> Self {
        Self { sender }
    }

    /// Record an arrival (non-blocking, drops if channel full)
    pub fn record(&self, arrival: FetchStageArrival) {
        let _ = self.sender.try_send(arrival);
    }

    /// Record multiple arrivals
    pub fn record_batch(&self, arrivals: impl IntoIterator<Item = FetchStageArrival>) {
        for arrival in arrivals {
            let _ = self.sender.try_send(arrival);
        }
    }
}

/// Parquet writer for fetch stage arrivals
pub type FetchStageArrivalCsvWriter = RotatingParquetWriter<FetchStageArrival>;

/// Create a fetch stage arrival tracing channel
pub fn create_fetch_stage_tracer(
    output_dir: PathBuf,
) -> (FetchStageArrivalSender, FetchStageArrivalCsvWriter) {
    let (tx, rx) = crossbeam_channel::bounded(100_000);
    let sender = FetchStageArrivalSender::new(tx);
    let writer = FetchStageArrivalCsvWriter::spawn(
        output_dir,
        "fetch_stage_arrivals".into(),
        rx,
    );
    (sender, writer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use tempfile::TempDir;

    #[test]
    fn test_fetch_stage_arrival_creation() {
        let arrival = FetchStageArrival::new(
            Some(12345),
            Some(42),
            Some(true),
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            8001,
            1228,
            false,
            false,
        );

        assert_eq!(arrival.slot, Some(12345));
        assert_eq!(arrival.shred_index, Some(42));
        assert_eq!(arrival.is_data, Some(true));
        assert_eq!(arrival.packet_size, 1228);
        assert!(!arrival.is_repair);
        assert!(!arrival.is_quic);
        assert!(arrival.timestamp_us > 0);
    }

    #[test]
    fn test_fetch_stage_parquet_writer() {
        let temp_dir = TempDir::new().unwrap();
        let (sender, writer) = create_fetch_stage_tracer(temp_dir.path().to_path_buf());

        // Send some arrivals
        sender.record(FetchStageArrival::with_timestamp(
            1000000,
            Some(100),
            Some(0),
            Some(true),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            8001,
            1228,
            false,
            false,
        ));

        sender.record(FetchStageArrival::with_timestamp(
            1000100,
            Some(100),
            Some(1),
            Some(true),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            8001,
            1228,
            true,
            false,
        ));

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
}
