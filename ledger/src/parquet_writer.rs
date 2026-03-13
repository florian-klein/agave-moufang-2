use {
    arrow::datatypes::Schema,
    arrow::record_batch::RecordBatch,
    crossbeam_channel::Receiver,
    parquet::arrow::ArrowWriter,
    parquet::basic::Compression,
    parquet::file::properties::WriterProperties,
    std::{
        fs::{self, File},
        path::PathBuf,
        sync::Arc,
        thread::{self, JoinHandle},
        time::Instant,
    },
};

/// Trait for record types that can be converted to Arrow RecordBatches
pub trait IntoRecordBatch: Send + 'static {
    fn arrow_schema() -> Arc<Schema>;
    fn to_record_batch(records: &[Self]) -> RecordBatch
    where
        Self: Sized;
}

/// Generic rotating Parquet writer. Buffers records and writes to new parquet files periodically.
pub struct RotatingParquetWriter<T: IntoRecordBatch> {
    thread_handle: Option<JoinHandle<()>>,
    _phantom: std::marker::PhantomData<T>,
}

const FLUSH_RECORD_THRESHOLD: usize = 100_000;
const FLUSH_INTERVAL_SECS: u64 = 60;

impl<T: IntoRecordBatch> RotatingParquetWriter<T> {
    pub fn spawn(output_dir: PathBuf, prefix: String, receiver: Receiver<T>) -> Self {
        let handle = thread::Builder::new()
            .name(format!("sol{}Pqt", &prefix[..prefix.len().min(8)]))
            .spawn(move || {
                Self::writer_loop(output_dir, prefix, receiver);
            })
            .expect("Failed to spawn parquet writer thread");
        Self {
            thread_handle: Some(handle),
            _phantom: std::marker::PhantomData,
        }
    }

    fn writer_loop(output_dir: PathBuf, prefix: String, receiver: Receiver<T>) {
        fs::create_dir_all(&output_dir).expect("Failed to create output dir");

        let mut sequence = Self::find_next_sequence(&output_dir, &prefix);
        let mut buffer: Vec<T> = Vec::with_capacity(FLUSH_RECORD_THRESHOLD);
        let mut last_flush = Instant::now();

        loop {
            match receiver.recv_timeout(std::time::Duration::from_secs(1)) {
                Ok(record) => {
                    buffer.push(record);
                    if buffer.len() >= FLUSH_RECORD_THRESHOLD
                        || last_flush.elapsed().as_secs() >= FLUSH_INTERVAL_SECS
                    {
                        Self::flush_buffer(&output_dir, &prefix, &mut sequence, &mut buffer);
                        last_flush = Instant::now();
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if !buffer.is_empty() && last_flush.elapsed().as_secs() >= FLUSH_INTERVAL_SECS {
                        Self::flush_buffer(&output_dir, &prefix, &mut sequence, &mut buffer);
                        last_flush = Instant::now();
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    if !buffer.is_empty() {
                        Self::flush_buffer(&output_dir, &prefix, &mut sequence, &mut buffer);
                    }
                    break;
                }
            }
        }
    }

    fn flush_buffer(
        output_dir: &PathBuf,
        prefix: &str,
        sequence: &mut u64,
        buffer: &mut Vec<T>,
    ) {
        if buffer.is_empty() {
            return;
        }
        let path = output_dir.join(format!("{}_{:06}.parquet", prefix, sequence));
        *sequence += 1;

        let schema = T::arrow_schema();
        let batch = T::to_record_batch(buffer);
        buffer.clear();

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        match File::create(&path) {
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

impl<T: IntoRecordBatch> Drop for RotatingParquetWriter<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.thread_handle.take() {
            let _ = handle.join();
        }
    }
}
