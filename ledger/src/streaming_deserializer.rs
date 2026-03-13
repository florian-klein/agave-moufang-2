//! Streaming entry deserializer for low-latency replay.
//!
//! This module provides [`StreamingEntryDeserializer`] which enables parsing entries
//! incrementally as shreds arrive, without waiting for the `DATA_COMPLETE_SHRED` flag.
//! This reduces latency by allowing transaction execution to begin as soon as entries
//! can be deserialized from available contiguous bytes.
//!
//! ## Key Features
//! - Out-of-order shred buffering with automatic reordering
//! - Incremental entry parsing as bytes become available
//! - Metrics for latency measurement and debugging
//!
//! ## Serialization Format
//! `Vec<Entry>` uses bincode/wincode format:
//! - 8 bytes: entry count (u64 little-endian)
//! - Followed by serialized entries: Entry₀, Entry₁, ..., Entryₙ₋₁

use {
    crate::shred::{self, ShredFlags},
    solana_clock::Slot,
    solana_entry::entry::Entry,
    solana_metrics::datapoint_info,
    std::{
        collections::BTreeMap,
        time::Instant,
    },
};

/// Error types for streaming deserialization
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamingDeserializeError {
    /// Need more bytes to continue parsing
    NeedMoreData,
    /// Invalid data format
    InvalidFormat(String),
    /// Shred extraction error
    ShredError(String),
    /// Entry deserialization error
    DeserializeError(String),
    /// Maximum pending shreds limit exceeded
    TooManyPendingShreds,
}

impl std::fmt::Display for StreamingDeserializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NeedMoreData => write!(f, "Need more data to continue parsing"),
            Self::InvalidFormat(msg) => write!(f, "Invalid format: {}", msg),
            Self::ShredError(msg) => write!(f, "Shred error: {}", msg),
            Self::DeserializeError(msg) => write!(f, "Deserialize error: {}", msg),
            Self::TooManyPendingShreds => write!(f, "Too many pending shreds"),
        }
    }
}

impl std::error::Error for StreamingDeserializeError {}

/// Timestamp information for latency measurement
#[derive(Debug, Clone)]
pub struct EntryTimestamp {
    /// When the first shred containing this entry's bytes arrived
    pub shred_arrival: Instant,
    /// When the entry was fully parsed
    pub parsed: Instant,
    /// When the entry was executed (set externally)
    pub executed: Option<Instant>,
}

/// Streaming deserializer for entries from shreds.
///
/// Buffers out-of-order shreds and parses entries incrementally as contiguous
/// bytes become available.
#[derive(Debug)]
pub struct StreamingEntryDeserializer {
    /// Slot being deserialized
    slot: Slot,

    /// Buffered shreds waiting for gaps to fill (index -> payload)
    pending_shreds: BTreeMap<u32, Vec<u8>>,

    /// Contiguous byte buffer (from shreds 0..N with no gaps)
    buffer: Vec<u8>,

    /// Highest contiguous shred index in buffer (None if no shreds yet)
    contiguous_through: Option<u32>,

    /// Entry count from 8-byte prefix (None until first 8 bytes available)
    expected_count: Option<u64>,

    /// Successfully parsed entry count
    entries_parsed: u64,

    /// Byte offset for next parse attempt
    parse_offset: usize,

    /// Whether DATA_COMPLETE_SHRED has been seen
    data_complete_seen: bool,

    /// Maximum pending shreds to buffer before erroring
    max_pending_shreds: usize,

    /// Arrival time of the first shred
    first_shred_arrival: Option<Instant>,

    /// Arrival times per shred index (for entry latency tracking)
    shred_arrival_times: BTreeMap<u32, Instant>,

    /// Timestamps for parsed entries
    entry_timestamps: Vec<EntryTimestamp>,

    /// When DATA_COMPLETE_SHRED arrived (for comparison)
    data_complete_arrival: Option<Instant>,
}

impl StreamingEntryDeserializer {
    /// Default maximum number of pending (out-of-order) shreds to buffer
    pub const DEFAULT_MAX_PENDING_SHREDS: usize = 1024;

    /// Create a new streaming deserializer for a slot
    pub fn new(slot: Slot) -> Self {
        Self::with_max_pending(slot, Self::DEFAULT_MAX_PENDING_SHREDS)
    }

    /// Create a new streaming deserializer with custom pending limit
    pub fn with_max_pending(slot: Slot, max_pending_shreds: usize) -> Self {
        Self {
            slot,
            pending_shreds: BTreeMap::new(),
            buffer: Vec::new(),
            contiguous_through: None,
            expected_count: None,
            entries_parsed: 0,
            parse_offset: 0,
            data_complete_seen: false,
            max_pending_shreds,
            first_shred_arrival: None,
            shred_arrival_times: BTreeMap::new(),
            entry_timestamps: Vec::new(),
            data_complete_arrival: None,
        }
    }

    /// Get the slot being deserialized
    pub fn slot(&self) -> Slot {
        self.slot
    }

    /// Add a shred payload. Buffers if out of order, appends to buffer if fills gap.
    ///
    /// Returns Ok(true) if new contiguous data was added to buffer.
    pub fn add_shred(
        &mut self,
        index: u32,
        payload: &[u8],
    ) -> Result<bool, StreamingDeserializeError> {
        let now = Instant::now();

        // Record arrival time
        if self.first_shred_arrival.is_none() {
            self.first_shred_arrival = Some(now);
        }
        self.shred_arrival_times.entry(index).or_insert(now);

        // Check for DATA_COMPLETE_SHRED flag
        if let Ok(flags) = shred::layout::get_flags(payload) {
            if flags.contains(ShredFlags::DATA_COMPLETE_SHRED) {
                self.data_complete_seen = true;
                self.data_complete_arrival = Some(now);
            }
        }

        // Determine expected next index
        let expected_next = self.contiguous_through.map(|i| i + 1).unwrap_or(0);

        if index == expected_next {
            // This shred extends our contiguous range
            self.append_shred_data(index, payload)?;
            self.flush_pending()?;
            Ok(true)
        } else if index > expected_next {
            // Out of order - buffer it
            if self.pending_shreds.len() >= self.max_pending_shreds {
                return Err(StreamingDeserializeError::TooManyPendingShreds);
            }
            self.pending_shreds.insert(index, payload.to_vec());
            Ok(false)
        } else {
            // Duplicate or old shred, ignore
            Ok(false)
        }
    }

    /// Add a shred using raw shred object (extracts data internally)
    pub fn add_shred_raw(
        &mut self,
        index: u32,
        shred_payload: &[u8],
    ) -> Result<bool, StreamingDeserializeError> {
        self.add_shred(index, shred_payload)
    }

    /// Append shred data to the contiguous buffer
    fn append_shred_data(
        &mut self,
        index: u32,
        payload: &[u8],
    ) -> Result<(), StreamingDeserializeError> {
        // Extract the data portion from the shred
        let data = shred::layout::get_data(payload)
            .map_err(|e| StreamingDeserializeError::ShredError(format!("{:?}", e)))?;

        self.buffer.extend_from_slice(data);
        self.contiguous_through = Some(index);
        Ok(())
    }

    /// Flush pending shreds into buffer when gaps fill
    fn flush_pending(&mut self) -> Result<(), StreamingDeserializeError> {
        loop {
            let expected_next = self.contiguous_through.map(|i| i + 1).unwrap_or(0);
            if let Some(payload) = self.pending_shreds.remove(&expected_next) {
                self.append_shred_data(expected_next, &payload)?;
            } else {
                break;
            }
        }
        Ok(())
    }

    /// Try to parse the entry count from the length prefix
    fn try_parse_count(&mut self) -> Result<bool, StreamingDeserializeError> {
        if self.expected_count.is_some() {
            return Ok(true);
        }

        // Need at least 8 bytes for the length prefix
        if self.buffer.len() < 8 {
            return Ok(false);
        }

        let count_bytes: [u8; 8] = self.buffer[0..8]
            .try_into()
            .map_err(|_| StreamingDeserializeError::InvalidFormat("Invalid length prefix".into()))?;

        self.expected_count = Some(u64::from_le_bytes(count_bytes));
        self.parse_offset = 8;
        Ok(true)
    }

    /// Try to parse the next entry from the buffer.
    ///
    /// Returns `Ok(Some(entry))` if an entry was successfully parsed,
    /// `Ok(None)` if more bytes are needed, or an error if parsing failed.
    pub fn try_parse_next(&mut self) -> Result<Option<Entry>, StreamingDeserializeError> {
        // First, ensure we have the count
        if !self.try_parse_count()? {
            return Ok(None);
        }

        let expected_count = self.expected_count.unwrap();

        // Check if we've already parsed all expected entries
        if self.entries_parsed >= expected_count {
            return Ok(None);
        }

        // Try to deserialize an entry starting at parse_offset
        let remaining = &self.buffer[self.parse_offset..];
        if remaining.is_empty() {
            return Ok(None);
        }

        // Use a cursor to track how many bytes were consumed
        let mut cursor = wincode::io::Cursor::new(remaining);

        match wincode::deserialize_from::<Entry>(&mut cursor) {
            Ok(entry) => {
                let bytes_consumed = cursor.position();
                self.parse_offset += bytes_consumed;
                self.entries_parsed += 1;

                // Record timestamp for this entry
                let now = Instant::now();
                // Find the earliest shred arrival time that contributed to this entry
                let shred_arrival = self.first_shred_arrival.unwrap_or(now);
                self.entry_timestamps.push(EntryTimestamp {
                    shred_arrival,
                    parsed: now,
                    executed: None,
                });

                // Emit metric
                datapoint_info!(
                    "streaming_entry_parsed",
                    ("slot", self.slot, i64),
                    ("entry_index", self.entries_parsed - 1, i64),
                    ("buffer_bytes", self.buffer.len(), i64),
                    ("pending_shreds", self.pending_shreds.len(), i64),
                    ("parse_latency_us", now.duration_since(shred_arrival).as_micros() as i64, i64),
                );

                Ok(Some(entry))
            }
            Err(wincode::ReadError::Io(wincode::io::ReadError::ReadSizeLimit(_))) => {
                // Need more data
                Ok(None)
            }
            Err(e) => {
                Err(StreamingDeserializeError::DeserializeError(format!("{:?}", e)))
            }
        }
    }

    /// Parse all currently available entries.
    ///
    /// Returns a vector of entries that could be parsed from available data.
    pub fn parse_available(&mut self) -> Result<Vec<Entry>, StreamingDeserializeError> {
        let mut entries = Vec::new();

        loop {
            match self.try_parse_next()? {
                Some(entry) => entries.push(entry),
                None => break,
            }
        }

        Ok(entries)
    }

    /// Check if all expected entries have been parsed
    pub fn is_complete(&self) -> bool {
        if let Some(expected) = self.expected_count {
            self.entries_parsed >= expected
        } else {
            false
        }
    }

    /// Check if DATA_COMPLETE_SHRED has been seen
    pub fn has_data_complete(&self) -> bool {
        self.data_complete_seen
    }

    /// Get the number of entries parsed so far
    pub fn entries_parsed(&self) -> u64 {
        self.entries_parsed
    }

    /// Get the expected entry count (if known)
    pub fn expected_count(&self) -> Option<u64> {
        self.expected_count
    }

    /// Get the number of contiguous bytes buffered
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    /// Get the number of pending (out-of-order) shreds
    pub fn pending_shreds_count(&self) -> usize {
        self.pending_shreds.len()
    }

    /// Get the highest contiguous shred index
    pub fn contiguous_through(&self) -> Option<u32> {
        self.contiguous_through
    }

    /// Get entry timestamps for latency analysis
    pub fn entry_timestamps(&self) -> &[EntryTimestamp] {
        &self.entry_timestamps
    }

    /// Mark an entry as executed (for latency tracking)
    pub fn mark_entry_executed(&mut self, entry_index: usize, timestamp: Instant) {
        if let Some(ts) = self.entry_timestamps.get_mut(entry_index) {
            ts.executed = Some(timestamp);
        }
    }

    /// Get the first shred arrival time
    pub fn first_shred_arrival(&self) -> Option<Instant> {
        self.first_shred_arrival
    }

    /// Get the DATA_COMPLETE_SHRED arrival time
    pub fn data_complete_arrival(&self) -> Option<Instant> {
        self.data_complete_arrival
    }

    /// Emit summary metrics when slot completes
    pub fn emit_slot_summary(&self) {
        let entries_streamed = self.entries_parsed;
        let total_entries = self.expected_count.unwrap_or(0);

        let avg_latency_us = if !self.entry_timestamps.is_empty() {
            let total: u128 = self.entry_timestamps.iter()
                .map(|ts| ts.parsed.duration_since(ts.shred_arrival).as_micros())
                .sum();
            (total / self.entry_timestamps.len() as u128) as i64
        } else {
            0
        };

        // Calculate latency savings if we have both timestamps
        let latency_savings_us = match (self.first_shred_arrival, self.data_complete_arrival) {
            (Some(first), Some(complete)) => {
                complete.duration_since(first).as_micros() as i64
            }
            _ => 0,
        };

        // Calculate average streaming gain: how much earlier each entry was available
        // compared to waiting for DATA_COMPLETE. This is the real improvement metric.
        let avg_streaming_gain_us = if let Some(complete) = self.data_complete_arrival {
            if !self.entry_timestamps.is_empty() {
                let total: u128 = self.entry_timestamps.iter()
                    .filter_map(|ts| complete.checked_duration_since(ts.parsed).map(|d| d.as_micros()))
                    .sum();
                (total / self.entry_timestamps.len() as u128) as i64
            } else {
                0
            }
        } else {
            0
        };

        datapoint_info!(
            "streaming_slot_summary",
            ("slot", self.slot, i64),
            ("total_entries", total_entries, i64),
            ("entries_streamed", entries_streamed, i64),
            ("avg_parse_latency_us", avg_latency_us, i64),
            ("data_complete_latency_us", latency_savings_us, i64),
            ("avg_streaming_gain_us", avg_streaming_gain_us, i64),
            ("pending_shreds_peak", self.pending_shreds.len(), i64),
            ("buffer_bytes_total", self.buffer.len(), i64),
        );
    }

    /// Reset the deserializer for reuse with a new slot
    pub fn reset(&mut self, slot: Slot) {
        self.slot = slot;
        self.pending_shreds.clear();
        self.buffer.clear();
        self.contiguous_through = None;
        self.expected_count = None;
        self.entries_parsed = 0;
        self.parse_offset = 0;
        self.data_complete_seen = false;
        self.first_shred_arrival = None;
        self.shred_arrival_times.clear();
        self.entry_timestamps.clear();
        self.data_complete_arrival = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_hash::Hash;
    use solana_transaction::versioned::VersionedTransaction;

    fn create_test_entry(num_hashes: u64, hash: Hash, num_txs: usize) -> Entry {
        Entry {
            num_hashes,
            hash,
            transactions: vec![VersionedTransaction::default(); num_txs],
        }
    }

    fn serialize_entries(entries: &[Entry]) -> Vec<u8> {
        wincode::serialize(entries).unwrap()
    }

    #[test]
    fn test_parse_single_entry() {
        let entries = vec![create_test_entry(1, Hash::new_unique(), 0)];
        let data = serialize_entries(&entries);

        let mut deser = StreamingEntryDeserializer::new(0);

        // Simulate single shred containing all data
        // For testing, we'll pretend the whole serialized data is in one "shred"
        // In reality, add_shred extracts data using shred::layout::get_data
        // For unit tests, we'll directly manipulate the buffer

        deser.buffer = data;
        deser.contiguous_through = Some(0);

        let parsed = deser.parse_available().unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].num_hashes, 1);
        assert!(deser.is_complete());
    }

    #[test]
    fn test_parse_multiple_entries() {
        let entries = vec![
            create_test_entry(1, Hash::new_unique(), 0),
            create_test_entry(2, Hash::new_unique(), 0),
            create_test_entry(3, Hash::new_unique(), 0),
        ];
        let data = serialize_entries(&entries);

        let mut deser = StreamingEntryDeserializer::new(0);
        deser.buffer = data;
        deser.contiguous_through = Some(0);

        let parsed = deser.parse_available().unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].num_hashes, 1);
        assert_eq!(parsed[1].num_hashes, 2);
        assert_eq!(parsed[2].num_hashes, 3);
        assert!(deser.is_complete());
    }

    #[test]
    fn test_incremental_parsing() {
        let entries = vec![
            create_test_entry(1, Hash::new_unique(), 0),
            create_test_entry(2, Hash::new_unique(), 0),
        ];
        let data = serialize_entries(&entries);

        let mut deser = StreamingEntryDeserializer::new(0);

        // Add bytes incrementally
        // First, add just the length prefix (8 bytes)
        deser.buffer.extend_from_slice(&data[0..8]);
        deser.contiguous_through = Some(0);

        let parsed = deser.parse_available().unwrap();
        assert_eq!(parsed.len(), 0);
        assert!(!deser.is_complete());
        assert_eq!(deser.expected_count(), Some(2));

        // Add rest of data
        deser.buffer.extend_from_slice(&data[8..]);

        let parsed = deser.parse_available().unwrap();
        assert_eq!(parsed.len(), 2);
        assert!(deser.is_complete());
    }

    #[test]
    fn test_empty_entries() {
        let entries: Vec<Entry> = vec![];
        let data = serialize_entries(&entries);

        let mut deser = StreamingEntryDeserializer::new(0);
        deser.buffer = data;
        deser.contiguous_through = Some(0);

        let parsed = deser.parse_available().unwrap();
        assert_eq!(parsed.len(), 0);
        assert!(deser.is_complete());
        assert_eq!(deser.expected_count(), Some(0));
    }

    #[test]
    fn test_need_more_data() {
        let entries = vec![create_test_entry(1, Hash::new_unique(), 0)];
        let data = serialize_entries(&entries);

        let mut deser = StreamingEntryDeserializer::new(0);

        // Add incomplete data (just part of the entry)
        let half = data.len() / 2;
        deser.buffer.extend_from_slice(&data[0..half]);
        deser.contiguous_through = Some(0);

        let parsed = deser.parse_available().unwrap();
        // Should parse nothing since entry is incomplete
        assert!(parsed.is_empty() || parsed.len() < 1);
    }

    #[test]
    fn test_reset() {
        let mut deser = StreamingEntryDeserializer::new(0);
        deser.buffer = vec![1, 2, 3];
        deser.entries_parsed = 5;

        deser.reset(1);

        assert_eq!(deser.slot(), 1);
        assert!(deser.buffer.is_empty());
        assert_eq!(deser.entries_parsed(), 0);
    }
}
