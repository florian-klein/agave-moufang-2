//! Shred arrival tracing for debugging and analysis.
//!
//! When enabled via `--enable-shred-arrival-tracing`, this module captures
//! per-shred arrival metadata (timestamp, source IP, source type) and writes
//! to rotating Parquet files when CompletedDataSetInfo events are produced.

use {
    crate::parquet_writer::IntoRecordBatch,
    arrow::{
        array::*,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    crossbeam_channel::{Receiver, Sender},
    parquet::arrow::ArrowWriter,
    parquet::basic::Compression,
    parquet::file::properties::WriterProperties,
    solana_clock::Slot,
    std::{
        collections::HashMap,
        fs,
        net::IpAddr,
        ops::Range,
        path::PathBuf,
        sync::{Arc, Mutex},
        thread::{self, JoinHandle},
        time::Instant,
    },
};

/// Source type for shred arrival
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShredArrivalSource {
    TurbineUdp,
    TurbineQuic,
    RepairUdp,
    RepairQuic,
}

impl ShredArrivalSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TurbineUdp => "turbine_udp",
            Self::TurbineQuic => "turbine_quic",
            Self::RepairUdp => "repair_udp",
            Self::RepairQuic => "repair_quic",
        }
    }
}

/// Metadata captured at packet arrival (before payload extraction)
#[derive(Debug, Clone)]
pub struct ShredArrivalMeta {
    pub arrival_time_ms: u64, // Monotonic timestamp in milliseconds (CLOCK_MONOTONIC_RAW)
    pub arrival_time_us: u32, // Microseconds component (0-999)
    pub source_ip: IpAddr,
    pub source: ShredArrivalSource,
}

/// Key for looking up arrival data: (slot, shred_index, is_data)
pub type ShredKey = (Slot, u32, bool);

/// Data to write for one completed data set
#[derive(Debug)]
pub struct CompletedDataSetWriteRequest {
    pub slot: Slot,
    pub indices: Range<u32>,
    pub arrivals: HashMap<ShredKey, ShredArrivalMeta>,
}

/// Buffers shred arrivals until a CompletedDataSetInfo triggers write
pub struct ShredArrivalBuffer {
    /// Per-slot arrival data: (slot, shred_index, is_data) -> ArrivalMeta
    arrivals: Mutex<HashMap<ShredKey, ShredArrivalMeta>>,
    /// Channel to send write requests to background writer
    write_sender: Sender<CompletedDataSetWriteRequest>,
    /// Maximum arrivals to buffer before dropping oldest
    max_buffer_size: usize,
}

impl ShredArrivalBuffer {
    pub fn new(write_sender: Sender<CompletedDataSetWriteRequest>) -> Self {
        Self {
            arrivals: Mutex::new(HashMap::with_capacity(100_000)),
            write_sender,
            max_buffer_size: 500_000,
        }
    }

    /// Record a shred arrival
    pub fn record_arrival(
        &self,
        slot: Slot,
        shred_index: u32,
        is_data: bool,
        meta: ShredArrivalMeta,
    ) {
        let mut arrivals = self.arrivals.lock().unwrap();
        let key = (slot, shred_index, is_data);
        arrivals.entry(key).or_insert(meta);

        if arrivals.len() > self.max_buffer_size {
            Self::evict_oldest_slot(&mut arrivals);
        }
    }

    /// Called when CompletedDataSetInfo is produced - extract arrivals and send to writer
    pub fn on_completed_data_set(&self, slot: Slot, indices: Range<u32>) {
        let mut arrivals_lock = self.arrivals.lock().unwrap();

        let mut dataset_arrivals = HashMap::new();
        for idx in indices.clone() {
            let data_key = (slot, idx, true);
            if let Some(meta) = arrivals_lock.remove(&data_key) {
                dataset_arrivals.insert(data_key, meta);
            }
            let coding_key = (slot, idx, false);
            if let Some(meta) = arrivals_lock.remove(&coding_key) {
                dataset_arrivals.insert(coding_key, meta);
            }
        }
        drop(arrivals_lock);

        if !dataset_arrivals.is_empty() {
            let request = CompletedDataSetWriteRequest {
                slot,
                indices,
                arrivals: dataset_arrivals,
            };
            let _ = self.write_sender.try_send(request);
        }
    }

    fn evict_oldest_slot(arrivals: &mut HashMap<ShredKey, ShredArrivalMeta>) {
        if let Some(min_slot) = arrivals.keys().map(|(s, _, _)| *s).min() {
            arrivals.retain(|(s, _, _), _| *s != min_slot);
        }
    }
}

/// Flat row for shred arrival parquet output
pub struct ShredArrivalRow {
    pub dataset_id: String,
    pub slot: u64,
    pub shred_index: u32,
    pub is_data: bool,
    pub arrival_time_ms: u64,
    pub arrival_time_us: u32,
    pub source_ip: String,
    pub source_type: String,
}

impl IntoRecordBatch for ShredArrivalRow {
    fn arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("dataset_id", DataType::Utf8, false),
            Field::new("slot", DataType::UInt64, false),
            Field::new("shred_index", DataType::UInt32, false),
            Field::new("is_data", DataType::Boolean, false),
            Field::new("arrival_time_ms", DataType::UInt64, false),
            Field::new("arrival_time_us", DataType::UInt32, false),
            Field::new("source_ip", DataType::Utf8, false),
            Field::new("source_type", DataType::Utf8, false),
        ]))
    }

    fn to_record_batch(records: &[Self]) -> RecordBatch {
        let dataset_ids: Vec<&str> = records.iter().map(|r| r.dataset_id.as_str()).collect();
        let slots: Vec<u64> = records.iter().map(|r| r.slot).collect();
        let indices: Vec<u32> = records.iter().map(|r| r.shred_index).collect();
        let is_data: Vec<bool> = records.iter().map(|r| r.is_data).collect();
        let arrival_ms: Vec<u64> = records.iter().map(|r| r.arrival_time_ms).collect();
        let arrival_us: Vec<u32> = records.iter().map(|r| r.arrival_time_us).collect();
        let ips: Vec<&str> = records.iter().map(|r| r.source_ip.as_str()).collect();
        let sources: Vec<&str> = records.iter().map(|r| r.source_type.as_str()).collect();

        RecordBatch::try_new(
            Self::arrow_schema(),
            vec![
                Arc::new(StringArray::from(dataset_ids)),
                Arc::new(UInt64Array::from(slots)),
                Arc::new(UInt32Array::from(indices)),
                Arc::new(BooleanArray::from(is_data)),
                Arc::new(UInt64Array::from(arrival_ms)),
                Arc::new(UInt32Array::from(arrival_us)),
                Arc::new(StringArray::from(ips)),
                Arc::new(StringArray::from(sources)),
            ],
        )
        .expect("Failed to create ShredArrival RecordBatch")
    }
}

const SHRED_ARRIVAL_FLUSH_THRESHOLD: usize = 100_000;
const SHRED_ARRIVAL_FLUSH_INTERVAL_SECS: u64 = 60;

/// Background Parquet writer for shred arrival records.
/// Receives CompletedDataSetWriteRequest, flattens to ShredArrivalRow, writes parquet.
pub struct ShredArrivalCsvWriter {
    thread_handle: Option<JoinHandle<()>>,
}

impl ShredArrivalCsvWriter {
    pub fn spawn(output_dir: PathBuf, receiver: Receiver<CompletedDataSetWriteRequest>) -> Self {
        let handle = thread::Builder::new()
            .name("solShredArPqt".to_string())
            .spawn(move || {
                Self::writer_loop(output_dir, receiver);
            })
            .expect("Failed to spawn shred arrival parquet writer thread");

        Self {
            thread_handle: Some(handle),
        }
    }

    fn writer_loop(output_dir: PathBuf, receiver: Receiver<CompletedDataSetWriteRequest>) {
        fs::create_dir_all(&output_dir).expect("Failed to create output dir");

        let prefix = "shred_arrivals";
        let mut sequence = Self::find_next_sequence(&output_dir, prefix);
        let mut buffer: Vec<ShredArrivalRow> =
            Vec::with_capacity(SHRED_ARRIVAL_FLUSH_THRESHOLD);
        let mut last_flush = Instant::now();

        let flush =
            |output_dir: &PathBuf, sequence: &mut u64, buffer: &mut Vec<ShredArrivalRow>| {
                if buffer.is_empty() {
                    return;
                }
                let path = output_dir.join(format!("{}_{:06}.parquet", prefix, sequence));
                *sequence += 1;

                let schema = ShredArrivalRow::arrow_schema();
                let batch = ShredArrivalRow::to_record_batch(buffer);
                buffer.clear();

                let props = WriterProperties::builder()
                    .set_compression(Compression::SNAPPY)
                    .build();

                match fs::File::create(&path) {
                    Ok(file) => match ArrowWriter::try_new(file, schema, Some(props)) {
                        Ok(mut writer) => {
                            if let Err(e) = writer.write(&batch) {
                                log::error!(
                                    "Failed to write parquet batch to {:?}: {}",
                                    path,
                                    e
                                );
                            }
                            if let Err(e) = writer.close() {
                                log::error!("Failed to close parquet file {:?}: {}", path, e);
                            }
                        }
                        Err(e) => {
                            log::error!("Failed to create ArrowWriter for {:?}: {}", path, e)
                        }
                    },
                    Err(e) => log::error!("Failed to create parquet file {:?}: {}", path, e),
                }
            };

        loop {
            match receiver.recv_timeout(std::time::Duration::from_secs(1)) {
                Ok(request) => {
                    let dataset_id = format!(
                        "{}_{}_{}",
                        request.slot, request.indices.start, request.indices.end
                    );

                    // Sort entries: data first, then by index
                    let mut entries: Vec<_> = request.arrivals.into_iter().collect();
                    entries.sort_by_key(|((_, idx, is_data), _)| (!is_data, *idx));

                    for ((slot, shred_index, is_data), meta) in entries {
                        buffer.push(ShredArrivalRow {
                            dataset_id: dataset_id.clone(),
                            slot,
                            shred_index,
                            is_data,
                            arrival_time_ms: meta.arrival_time_ms,
                            arrival_time_us: meta.arrival_time_us,
                            source_ip: meta.source_ip.to_string(),
                            source_type: meta.source.as_str().to_string(),
                        });
                    }

                    if buffer.len() >= SHRED_ARRIVAL_FLUSH_THRESHOLD
                        || last_flush.elapsed().as_secs() >= SHRED_ARRIVAL_FLUSH_INTERVAL_SECS
                    {
                        flush(&output_dir, &mut sequence, &mut buffer);
                        last_flush = Instant::now();
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if !buffer.is_empty()
                        && last_flush.elapsed().as_secs() >= SHRED_ARRIVAL_FLUSH_INTERVAL_SECS
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

impl Drop for ShredArrivalCsvWriter {
    fn drop(&mut self) {
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_shred_arrival_buffer() {
        let (tx, rx) = crossbeam_channel::bounded(10);
        let buffer = ShredArrivalBuffer::new(tx);

        // Record some arrivals (both data and coding shreds)
        let meta1 = ShredArrivalMeta {
            arrival_time_ms: 1000,
            arrival_time_us: 500,
            source_ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
            source: ShredArrivalSource::TurbineUdp,
        };
        let meta2 = ShredArrivalMeta {
            arrival_time_ms: 2000,
            arrival_time_us: 750,
            source_ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 2)),
            source: ShredArrivalSource::TurbineQuic,
        };
        let meta3 = ShredArrivalMeta {
            arrival_time_ms: 1500,
            arrival_time_us: 250,
            source_ip: IpAddr::V4(Ipv4Addr::new(192, 168, 1, 3)),
            source: ShredArrivalSource::TurbineUdp,
        };

        buffer.record_arrival(100, 0, true, meta1.clone()); // data shred
        buffer.record_arrival(100, 1, true, meta2.clone()); // data shred
        buffer.record_arrival(100, 0, false, meta3.clone()); // coding shred

        // Trigger completed data set
        buffer.on_completed_data_set(100, 0..2);

        // Check that request was sent with both data and coding shreds
        let request = rx.try_recv().unwrap();
        assert_eq!(request.slot, 100);
        assert_eq!(request.indices, 0..2);
        assert_eq!(request.arrivals.len(), 3); // 2 data + 1 coding
        assert!(request.arrivals.contains_key(&(100, 0, true)));
        assert!(request.arrivals.contains_key(&(100, 1, true)));
        assert!(request.arrivals.contains_key(&(100, 0, false)));
    }

    #[test]
    fn test_shred_arrival_source_as_str() {
        assert_eq!(ShredArrivalSource::TurbineUdp.as_str(), "turbine_udp");
        assert_eq!(ShredArrivalSource::TurbineQuic.as_str(), "turbine_quic");
        assert_eq!(ShredArrivalSource::RepairUdp.as_str(), "repair_udp");
        assert_eq!(ShredArrivalSource::RepairQuic.as_str(), "repair_quic");
    }
}
