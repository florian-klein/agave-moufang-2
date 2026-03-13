//! Entry cache for low-latency entry access during replay.
//!
//! This cache stores recently completed entries in memory, allowing replay to
//! read entries without waiting for blockstore I/O. Entries are added when
//! shreds complete a data set, and blockstore writes happen asynchronously.

use {
    solana_clock::Slot,
    solana_entry::entry::Entry,
    std::{
        collections::{BTreeMap, HashMap},
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, RwLock,
        },
    },
};

/// A batch of entries from a contiguous range of shreds.
#[derive(Debug, Clone)]
pub struct CachedEntryBatch {
    /// The entries in this batch
    pub entries: Vec<Entry>,
    /// Number of shreds that produced these entries
    pub num_shreds: u64,
    /// The next expected shred index after this batch (shred_start_index + num_shreds)
    pub next_shred_index: u64,
}

/// Information about cached entries for a slot
#[derive(Debug, Clone)]
pub struct CachedSlotEntries {
    /// Entry batches keyed by their starting shred index
    pub batches: BTreeMap<u64, CachedEntryBatch>,
    /// First missing shred index - mirrors blockstore's consumed tracking.
    /// All shreds from 0 to consumed-1 have been received contiguously.
    pub consumed: u64,
    /// The last shred index in the slot (when LAST_SHRED_IN_SLOT flag is set).
    /// Used to determine if the slot is complete.
    pub last_index: Option<u64>,
    /// Monotonic counter value when this slot was last accessed/updated.
    /// Used for LRU eviction.
    pub last_accessed: u64,
}

impl CachedSlotEntries {
    pub fn new() -> Self {
        Self {
            batches: BTreeMap::new(),
            consumed: 0,
            last_index: None,
            last_accessed: 0,
        }
    }

    pub fn with_access_time(access_time: u64) -> Self {
        Self {
            batches: BTreeMap::new(),
            consumed: 0,
            last_index: None,
            last_accessed: access_time,
        }
    }

    /// Check if the slot is complete (all shreds received contiguously up to last_index).
    /// This is computed fresh each time, not cached, to avoid staleness issues.
    /// Uses strict equality to match blockstore's SlotMeta::is_full() semantics:
    /// consumed must equal exactly last_index + 1.
    pub fn is_full(&self) -> bool {
        match self.last_index {
            Some(last_idx) => self.consumed == last_idx + 1,
            None => false,
        }
    }

    /// Update the consumed index by walking through contiguous batches.
    /// This mirrors the blockstore's approach to tracking contiguity.
    fn update_consumed(&mut self) {
        let mut current = self.consumed;
        while let Some(batch) = self.batches.get(&current) {
            // Guard against zero-shred batches causing infinite loops
            if batch.next_shred_index <= current {
                break;
            }
            current = batch.next_shred_index;
        }
        self.consumed = current;
    }

    /// Get contiguous entries starting from a given shred index.
    /// Returns (entries, total_shreds, next_expected_index) or None if there's a gap.
    /// Handles mid-batch queries by finding the containing batch and returning from that point.
    pub fn get_contiguous_entries_from(
        &self,
        start_index: u64,
    ) -> Option<(Vec<Entry>, u64, u64)> {
        // Can't return data if requesting beyond what's contiguously available
        if start_index >= self.consumed {
            return None;
        }

        let mut entries = Vec::new();
        let mut total_shreds = 0u64;
        let mut expected_next = start_index;

        // Find the batch containing start_index (may be mid-batch query)
        // Use range(..=start_index) to find batches at or before start_index
        let containing_batch = self.batches.range(..=start_index).next_back();

        if let Some((&batch_start, first_batch)) = containing_batch {
            // Check if start_index falls within this batch
            if start_index < first_batch.next_shred_index {
                // Mid-batch query: start_index is within [batch_start, next_shred_index)
                // We can return entries from this batch, but we need to account for
                // the shreds we're skipping
                let skipped_shreds = start_index - batch_start;
                let remaining_shreds = first_batch.num_shreds - skipped_shreds;

                // For mid-batch, we return all entries (can't split entries mid-batch)
                // but adjust num_shreds to reflect what remains
                entries.extend(first_batch.entries.iter().cloned());
                total_shreds += remaining_shreds;
                expected_next = first_batch.next_shred_index;

                // Continue with subsequent batches
                for (&shred_idx, batch) in self.batches.range(expected_next..) {
                    if shred_idx != expected_next {
                        // Gap detected - stop here
                        break;
                    }
                    entries.extend(batch.entries.iter().cloned());
                    total_shreds += batch.num_shreds;
                    expected_next = batch.next_shred_index;
                }

                return Some((entries, total_shreds, expected_next));
            }
        }

        // Standard case: start_index should align with a batch start
        for (&shred_idx, batch) in self.batches.range(start_index..) {
            if shred_idx != expected_next {
                // Gap detected - stop here
                break;
            }
            entries.extend(batch.entries.iter().cloned());
            total_shreds += batch.num_shreds;
            expected_next = batch.next_shred_index;
        }

        if entries.is_empty() {
            None
        } else {
            Some((entries, total_shreds, expected_next))
        }
    }

    /// Get entries starting from index without gap validation (legacy behavior).
    /// Use get_contiguous_entries_from for correct gap handling.
    #[allow(dead_code)]
    pub fn get_entries_from(&self, start_index: u64) -> (Vec<Entry>, u64) {
        let mut entries = Vec::new();
        let mut total_shreds = 0u64;

        for (_, batch) in self.batches.range(start_index..) {
            entries.extend(batch.entries.iter().cloned());
            total_shreds += batch.num_shreds;
        }

        (entries, total_shreds)
    }
}

impl Default for CachedSlotEntries {
    fn default() -> Self {
        Self::new()
    }
}

/// A memory cache for entries to reduce blockstore read latency.
///
/// The cache holds entries for recent slots, allowing replay to read
/// entries without waiting for RocksDB I/O.
pub struct EntryCache {
    /// Cached entries by slot
    slots: RwLock<HashMap<Slot, CachedSlotEntries>>,
    /// Maximum number of slots to cache
    max_slots: usize,
    /// Monotonic counter for LRU tracking (incremented on each access/insert)
    access_counter: AtomicU64,
    /// Stats
    hits: AtomicU64,
    misses: AtomicU64,
}

impl EntryCache {
    /// Create a new entry cache
    pub fn new(max_slots: usize) -> Self {
        Self {
            slots: RwLock::new(HashMap::with_capacity(max_slots)),
            max_slots,
            access_counter: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Get the next access time for LRU tracking
    fn next_access_time(&self) -> u64 {
        self.access_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Add entries for a slot at the given shred index.
    /// `last_index` is the final shred index in the slot (from LAST_SHRED_IN_SLOT flag).
    pub fn insert_entries(
        &self,
        slot: Slot,
        shred_start_index: u64,
        entries: Vec<Entry>,
        num_shreds: u64,
        last_index: Option<u64>,
    ) {
        let access_time = self.next_access_time();
        let mut slots = self.slots.write().unwrap();

        // Evict least recently used slot if at capacity
        if slots.len() >= self.max_slots && !slots.contains_key(&slot) {
            // Find the slot with the oldest (lowest) last_accessed value
            if let Some(&lru_slot) = slots
                .iter()
                .min_by_key(|(_, entries)| entries.last_accessed)
                .map(|(slot, _)| slot)
            {
                slots.remove(&lru_slot);
            }
        }

        let slot_entries = slots
            .entry(slot)
            .or_insert_with(|| CachedSlotEntries::with_access_time(access_time));

        // Update access time on each insert
        slot_entries.last_accessed = access_time;

        let new_next_shred_index = shred_start_index + num_shreds;

        // Check if this is an overwrite that changes the chain structure
        // If so, we need to reset consumed to rescan from the affected point
        if let Some(old_batch) = slot_entries.batches.get(&shred_start_index) {
            if old_batch.next_shred_index != new_next_shred_index {
                // Overwrite with different num_shreds - must rescan from this index
                slot_entries.consumed = slot_entries.consumed.min(shred_start_index);
            }
        }

        // Create the batch with proper metadata
        let batch = CachedEntryBatch {
            entries,
            num_shreds,
            next_shred_index: new_next_shred_index,
        };

        slot_entries.batches.insert(shred_start_index, batch);

        // Recalculate contiguous consumed index
        slot_entries.update_consumed();

        // Update last_index if provided (from LAST_SHRED_IN_SLOT flag)
        if let Some(idx) = last_index {
            slot_entries.last_index = Some(
                slot_entries.last_index.map_or(idx, |existing| existing.max(idx))
            );
        }
    }

    /// Set the last_index for a slot (the final shred index).
    /// This is called when the LAST_SHRED_IN_SLOT flag is seen.
    pub fn set_last_index(&self, slot: Slot, last_index: u64) {
        let mut slots = self.slots.write().unwrap();
        if let Some(slot_entries) = slots.get_mut(&slot) {
            slot_entries.last_index = Some(
                slot_entries.last_index.map_or(last_index, |existing| existing.max(last_index))
            );
        }
    }

    /// Get entries for a slot starting from a given shred index.
    /// Returns (entries, num_shreds, is_full) or None if not in cache or gap exists.
    /// Note: `is_full` is computed fresh based on consumed vs last_index, not cached.
    pub fn get_slot_entries(
        &self,
        slot: Slot,
        start_index: u64,
    ) -> Option<(Vec<Entry>, u64, bool)> {
        let slots = self.slots.read().unwrap();

        if let Some(slot_entries) = slots.get(&slot) {
            // Check if start_index is at or beyond contiguous data boundary
            // (consumed is exclusive - data exists for indices [0..consumed))
            if start_index >= slot_entries.consumed {
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }

            if let Some((entries, num_shreds, _next_index)) =
                slot_entries.get_contiguous_entries_from(start_index)
            {
                self.hits.fetch_add(1, Ordering::Relaxed);
                // Compute is_full fresh based on consumed vs last_index
                let is_full = slot_entries.is_full();
                return Some((entries, num_shreds, is_full));
            }
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Check if a slot has any cached entries
    pub fn has_slot(&self, slot: Slot) -> bool {
        self.slots.read().unwrap().contains_key(&slot)
    }

    /// Remove a slot from the cache (e.g., after it's been rooted)
    pub fn remove_slot(&self, slot: Slot) {
        self.slots.write().unwrap().remove(&slot);
    }

    /// Remove all slots below a given slot (cleanup after rooting)
    pub fn purge_slots_below(&self, min_slot: Slot) {
        let mut slots = self.slots.write().unwrap();
        slots.retain(|&slot, _| slot >= min_slot);
    }

    /// Get cache statistics
    pub fn stats(&self) -> (u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
        )
    }

    /// Get the number of slots currently cached
    pub fn len(&self) -> usize {
        self.slots.read().unwrap().len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.slots.read().unwrap().is_empty()
    }

    /// Get the consumed index for a slot (for debugging/testing)
    pub fn get_consumed(&self, slot: Slot) -> Option<u64> {
        self.slots
            .read()
            .unwrap()
            .get(&slot)
            .map(|s| s.consumed)
    }

    /// Get the last_index for a slot (for debugging/testing)
    pub fn get_last_index(&self, slot: Slot) -> Option<Option<u64>> {
        self.slots
            .read()
            .unwrap()
            .get(&slot)
            .map(|s| s.last_index)
    }

    /// Check if a slot is full based on cached data (for debugging/testing)
    pub fn is_slot_full(&self, slot: Slot) -> Option<bool> {
        self.slots
            .read()
            .unwrap()
            .get(&slot)
            .map(|s| s.is_full())
    }
}

impl Default for EntryCache {
    fn default() -> Self {
        // Default to caching 32 slots (~13 seconds at 400ms/slot)
        Self::new(32)
    }
}

/// Shared entry cache that can be used across threads
pub type SharedEntryCache = Arc<EntryCache>;

/// Create a new shared entry cache
pub fn new_shared_entry_cache(max_slots: usize) -> SharedEntryCache {
    Arc::new(EntryCache::new(max_slots))
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_entry::entry::Entry;
    use solana_hash::Hash;

    fn create_test_entries(num: usize) -> Vec<Entry> {
        (0..num)
            .map(|i| Entry {
                num_hashes: i as u64,
                hash: Hash::new_unique(),
                transactions: vec![],
            })
            .collect()
    }

    // ==================== Basic Tests ====================

    #[test]
    fn test_entry_cache_basic() {
        let cache = EntryCache::new(10);

        // Insert entries for slot 100 (no last_index yet, not full)
        let entries = create_test_entries(5);
        cache.insert_entries(100, 0, entries.clone(), 10, None);

        // Retrieve entries
        let result = cache.get_slot_entries(100, 0);
        assert!(result.is_some());
        let (retrieved, num_shreds, is_full) = result.unwrap();
        assert_eq!(retrieved.len(), 5);
        assert_eq!(num_shreds, 10);
        assert!(!is_full); // No last_index set

        // Set last_index to mark slot as full (consumed=10, last_index=9 means full)
        cache.set_last_index(100, 9);
        let result = cache.get_slot_entries(100, 0);
        assert!(result.unwrap().2); // is_full should be true (10 > 9)
    }

    #[test]
    fn test_entry_cache_eviction() {
        let cache = EntryCache::new(3);

        // Insert 4 slots, should evict the oldest
        // last_index=4 with num_shreds=5 means consumed=5, 5>4 so is_full=true
        for slot in 100..104 {
            let entries = create_test_entries(2);
            cache.insert_entries(slot, 0, entries, 5, Some(4));
        }

        // Slot 100 should be evicted
        assert!(cache.get_slot_entries(100, 0).is_none());
        // Slots 101-103 should exist
        assert!(cache.get_slot_entries(101, 0).is_some());
        assert!(cache.get_slot_entries(102, 0).is_some());
        assert!(cache.get_slot_entries(103, 0).is_some());
    }

    #[test]
    fn test_entry_cache_lru_eviction() {
        // Test that LRU eviction is based on access time, not slot number
        let cache = EntryCache::new(3);

        // Insert slots in order: 200, 201, 202
        cache.insert_entries(200, 0, create_test_entries(1), 5, Some(4));
        cache.insert_entries(201, 0, create_test_entries(1), 5, Some(4));
        cache.insert_entries(202, 0, create_test_entries(1), 5, Some(4));

        // Touch slot 200 again (update its access time) by adding more entries
        cache.insert_entries(200, 5, create_test_entries(1), 5, Some(9));

        // Now insert slot 100 (lower number, but requires eviction)
        // Should evict slot 201 (oldest access time), NOT slot 100 or 200
        cache.insert_entries(100, 0, create_test_entries(1), 5, Some(4));

        // Slot 201 should be evicted (oldest access time)
        assert!(cache.get_slot_entries(201, 0).is_none());

        // These should all exist:
        // - Slot 100: just inserted
        // - Slot 200: recently touched
        // - Slot 202: more recent than 201
        assert!(cache.get_slot_entries(100, 0).is_some());
        assert!(cache.get_slot_entries(200, 0).is_some());
        assert!(cache.get_slot_entries(202, 0).is_some());
    }

    #[test]
    fn test_entry_cache_incremental_insert() {
        let cache = EntryCache::new(10);

        // Insert entries incrementally (simulating shred arrival)
        let entries1 = create_test_entries(3);
        cache.insert_entries(100, 0, entries1, 5, None);

        let entries2 = create_test_entries(2);
        cache.insert_entries(100, 5, entries2, 3, None);

        // Last batch with last_index=9 (consumed will be 10, 10>9 means full)
        let entries3 = create_test_entries(1);
        cache.insert_entries(100, 8, entries3, 2, Some(9));

        // Should get all entries
        let result = cache.get_slot_entries(100, 0);
        assert!(result.is_some());
        let (entries, num_shreds, is_full) = result.unwrap();
        assert_eq!(entries.len(), 6); // 3 + 2 + 1
        assert_eq!(num_shreds, 10); // 5 + 3 + 2
        assert!(is_full);
    }

    #[test]
    fn test_entry_cache_stats() {
        let cache = EntryCache::new(10);

        let entries = create_test_entries(2);
        cache.insert_entries(100, 0, entries, 5, Some(4)); // full

        // Hit
        let _ = cache.get_slot_entries(100, 0);
        // Miss
        let _ = cache.get_slot_entries(999, 0);

        let (hits, misses) = cache.stats();
        assert_eq!(hits, 1);
        assert_eq!(misses, 1);
    }

    #[test]
    fn test_purge_slots_below() {
        let cache = EntryCache::new(10);

        for slot in 100..110 {
            let entries = create_test_entries(1);
            cache.insert_entries(slot, 0, entries, 1, Some(0)); // full
        }

        cache.purge_slots_below(105);

        // Slots 100-104 should be purged
        for slot in 100..105 {
            assert!(cache.get_slot_entries(slot, 0).is_none());
        }
        // Slots 105-109 should remain
        for slot in 105..110 {
            assert!(cache.get_slot_entries(slot, 0).is_some());
        }
    }

    // ==================== Gap Detection Tests ====================

    #[test]
    fn test_gap_detection_query_from_gap() {
        let cache = EntryCache::new(10);

        // Insert batches with a gap: [0-5) and [10-15), missing [5-10)
        let entries1 = create_test_entries(3);
        cache.insert_entries(100, 0, entries1, 5, None);

        let entries2 = create_test_entries(2);
        cache.insert_entries(100, 10, entries2, 5, None);

        // Consumed should be 5 (first gap)
        assert_eq!(cache.get_consumed(100), Some(5));

        // Query from index 0 should work (within contiguous region)
        let result = cache.get_slot_entries(100, 0);
        assert!(result.is_some());
        let (entries, num_shreds, _) = result.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(num_shreds, 5);

        // Query from index 5 (gap) should fail
        let result = cache.get_slot_entries(100, 5);
        assert!(result.is_none());

        // Query from index 10 should fail (beyond consumed)
        let result = cache.get_slot_entries(100, 10);
        assert!(result.is_none());
    }

    #[test]
    fn test_gap_detection_stops_at_gap() {
        let cache = EntryCache::new(10);

        // Insert batches: [0-5), [5-10), [15-20) - gap at [10-15)
        cache.insert_entries(100, 0, create_test_entries(2), 5, None);
        cache.insert_entries(100, 5, create_test_entries(2), 5, None);
        cache.insert_entries(100, 15, create_test_entries(2), 5, None);

        // Consumed should be 10
        assert_eq!(cache.get_consumed(100), Some(10));

        // Query from 0 should return first two batches only
        let result = cache.get_slot_entries(100, 0);
        assert!(result.is_some());
        let (entries, num_shreds, _) = result.unwrap();
        assert_eq!(entries.len(), 4); // 2 + 2, not including the third batch
        assert_eq!(num_shreds, 10); // 5 + 5
    }

    #[test]
    fn test_contiguous_entries_returned() {
        let cache = EntryCache::new(10);

        // Insert fully contiguous batches with last_index=14 (consumed=15, 15>14 means full)
        cache.insert_entries(100, 0, create_test_entries(2), 5, None);
        cache.insert_entries(100, 5, create_test_entries(3), 5, None);
        cache.insert_entries(100, 10, create_test_entries(4), 5, Some(14));

        // Consumed should be 15 (all contiguous)
        assert_eq!(cache.get_consumed(100), Some(15));

        // Query from 0 should return all entries
        let result = cache.get_slot_entries(100, 0);
        assert!(result.is_some());
        let (entries, num_shreds, is_full) = result.unwrap();
        assert_eq!(entries.len(), 9); // 2 + 3 + 4
        assert_eq!(num_shreds, 15); // 5 + 5 + 5
        assert!(is_full);
    }

    // ==================== num_shreds Calculation Tests ====================

    #[test]
    fn test_num_shreds_single_batch() {
        let cache = EntryCache::new(10);

        cache.insert_entries(100, 0, create_test_entries(5), 10, None);

        let result = cache.get_slot_entries(100, 0);
        assert!(result.is_some());
        let (_, num_shreds, _) = result.unwrap();
        assert_eq!(num_shreds, 10); // Actual shred count, not span
    }

    #[test]
    fn test_num_shreds_accumulation() {
        let cache = EntryCache::new(10);

        // Three batches with different shred counts
        cache.insert_entries(100, 0, create_test_entries(1), 3, None);
        cache.insert_entries(100, 3, create_test_entries(1), 7, None);
        cache.insert_entries(100, 10, create_test_entries(1), 2, None);

        let result = cache.get_slot_entries(100, 0);
        assert!(result.is_some());
        let (_, num_shreds, _) = result.unwrap();
        assert_eq!(num_shreds, 12); // 3 + 7 + 2, not span-based
    }

    #[test]
    fn test_num_shreds_from_middle() {
        let cache = EntryCache::new(10);

        cache.insert_entries(100, 0, create_test_entries(2), 5, None);
        cache.insert_entries(100, 5, create_test_entries(3), 10, None);
        cache.insert_entries(100, 15, create_test_entries(1), 5, None);

        // Query from index 5 - should get batches starting at 5 and 15
        let result = cache.get_slot_entries(100, 5);
        assert!(result.is_some());
        let (entries, num_shreds, _) = result.unwrap();
        assert_eq!(entries.len(), 4); // 3 + 1
        assert_eq!(num_shreds, 15); // 10 + 5
    }

    // ==================== Index Mismatch Tests ====================

    #[test]
    fn test_query_beyond_data() {
        let cache = EntryCache::new(10);

        cache.insert_entries(100, 0, create_test_entries(2), 5, None);

        // Query at index 10 when only data at 0-4
        let result = cache.get_slot_entries(100, 10);
        assert!(result.is_none());

        // Query at index 5 (consumed boundary)
        let result = cache.get_slot_entries(100, 5);
        assert!(result.is_none());
    }

    #[test]
    fn test_non_contiguous_indices() {
        let cache = EntryCache::new(10);

        // Batches at [0, 10, 20] with gaps
        cache.insert_entries(100, 0, create_test_entries(1), 5, None);
        cache.insert_entries(100, 10, create_test_entries(1), 5, None);
        cache.insert_entries(100, 20, create_test_entries(1), 5, None);

        // Consumed should only be 5 (first gap at 5)
        assert_eq!(cache.get_consumed(100), Some(5));

        // Only first batch accessible
        let result = cache.get_slot_entries(100, 0);
        assert!(result.is_some());
        let (entries, num_shreds, _) = result.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(num_shreds, 5);
    }

    #[test]
    fn test_query_between_batches() {
        let cache = EntryCache::new(10);

        // Data at indices 0 and 10, query at 5
        cache.insert_entries(100, 0, create_test_entries(2), 5, None);
        cache.insert_entries(100, 10, create_test_entries(2), 5, None);

        // Query at index 5 (in the gap)
        let result = cache.get_slot_entries(100, 5);
        assert!(result.is_none());
    }

    // ==================== Integration Tests ====================

    #[test]
    fn test_producer_consumer_sync() {
        let cache = EntryCache::new(10);

        // Simulate window_service producing batches as shreds arrive
        // Producer inserts at shred indices as batches complete

        // First batch completes
        cache.insert_entries(100, 0, create_test_entries(2), 5, None);

        // Consumer (replay) queries from progress.num_shreds = 0
        let result = cache.get_slot_entries(100, 0);
        assert!(result.is_some());
        let (entries, num_shreds, _) = result.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(num_shreds, 5);

        // Simulate consumer advancing progress.num_shreds to 5
        // Second batch arrives
        cache.insert_entries(100, 5, create_test_entries(3), 10, None);

        // Consumer queries from 5
        let result = cache.get_slot_entries(100, 5);
        assert!(result.is_some());
        let (entries, num_shreds, _) = result.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(num_shreds, 10);
    }

    #[test]
    fn test_incremental_consumption() {
        let cache = EntryCache::new(10);

        // Insert all batches, last one with last_index=14 (consumed=15, 15>14 means full)
        cache.insert_entries(100, 0, create_test_entries(2), 5, None);
        cache.insert_entries(100, 5, create_test_entries(3), 5, None);
        cache.insert_entries(100, 10, create_test_entries(4), 5, Some(14));

        // First read from 0
        let result = cache.get_slot_entries(100, 0).unwrap();
        assert_eq!(result.0.len(), 9); // All entries
        assert_eq!(result.1, 15); // All shreds

        // Simulate consumer advancing to 5, then read remaining
        let result = cache.get_slot_entries(100, 5).unwrap();
        assert_eq!(result.0.len(), 7); // 3 + 4
        assert_eq!(result.1, 10); // 5 + 5

        // Consumer advances to 10
        let result = cache.get_slot_entries(100, 10).unwrap();
        assert_eq!(result.0.len(), 4);
        assert_eq!(result.1, 5);
    }

    #[test]
    fn test_out_of_order_insertion() {
        let cache = EntryCache::new(10);

        // Batches arrive out of order
        cache.insert_entries(100, 10, create_test_entries(3), 5, None);
        // Consumed should still be 0
        assert_eq!(cache.get_consumed(100), Some(0));

        cache.insert_entries(100, 5, create_test_entries(2), 5, None);
        // Still 0 (gap at 0-4)
        assert_eq!(cache.get_consumed(100), Some(0));

        cache.insert_entries(100, 0, create_test_entries(1), 5, None);
        // Now should jump to 15 (all contiguous)
        assert_eq!(cache.get_consumed(100), Some(15));

        // All entries now accessible
        let result = cache.get_slot_entries(100, 0);
        assert!(result.is_some());
        let (entries, num_shreds, _) = result.unwrap();
        assert_eq!(entries.len(), 6); // 1 + 2 + 3
        assert_eq!(num_shreds, 15);
    }

    #[test]
    fn test_partial_gap_fill() {
        let cache = EntryCache::new(10);

        // Large gap initially
        cache.insert_entries(100, 0, create_test_entries(1), 5, None);
        cache.insert_entries(100, 20, create_test_entries(1), 5, None);

        assert_eq!(cache.get_consumed(100), Some(5));

        // Partially fill the gap
        cache.insert_entries(100, 5, create_test_entries(1), 5, None);
        assert_eq!(cache.get_consumed(100), Some(10));

        // Fill more
        cache.insert_entries(100, 10, create_test_entries(1), 5, None);
        assert_eq!(cache.get_consumed(100), Some(15));

        // Still gap at 15-19
        let result = cache.get_slot_entries(100, 0).unwrap();
        assert_eq!(result.0.len(), 3); // Only first 3 batches
    }

    // ==================== Performance Test ====================

    #[test]
    fn test_entry_cache_performance() {
        use std::time::Instant;

        let cache = EntryCache::new(100);

        // Insert entries with varying sizes (last_index=19 makes consumed=20 > 19, so is_full=true)
        for slot in 0..50 {
            let entries = create_test_entries(10); // 10 entries per slot
            cache.insert_entries(slot, 0, entries, 20, Some(19));
        }

        // Benchmark cache hits
        let iterations = 10_000;
        let start = Instant::now();
        for i in 0..iterations {
            let slot = (i % 50) as Slot;
            let _ = cache.get_slot_entries(slot, 0);
        }
        let cache_hit_duration = start.elapsed();

        // Benchmark cache misses (non-existent slots)
        let start = Instant::now();
        for i in 0..iterations {
            let slot = 100 + (i % 50) as Slot; // Non-existent slots
            let _ = cache.get_slot_entries(slot, 0);
        }
        let cache_miss_duration = start.elapsed();

        let cache_hit_ns = cache_hit_duration.as_nanos() as u64 / iterations;
        let cache_miss_ns = cache_miss_duration.as_nanos() as u64 / iterations;

        println!("Entry cache performance test:");
        println!("  Cache hit average: {} ns per access", cache_hit_ns);
        println!("  Cache miss average: {} ns per access", cache_miss_ns);

        // Verify hits were counted
        let (hits, misses) = cache.stats();
        assert_eq!(hits, iterations);
        assert_eq!(misses, iterations);

        // Cache hits should be fast (< 10µs typical, we allow generous margin)
        assert!(cache_hit_ns < 100_000, "Cache hit too slow: {} ns", cache_hit_ns);
    }

    // ==================== Edge Case Tests ====================

    #[test]
    fn test_empty_slot() {
        let cache = EntryCache::new(10);

        // Query non-existent slot
        assert!(cache.get_slot_entries(100, 0).is_none());
        assert_eq!(cache.get_consumed(100), None);
    }

    #[test]
    fn test_zero_shred_batch() {
        let cache = EntryCache::new(10);

        // Edge case: batch with 0 shreds (shouldn't happen in practice)
        cache.insert_entries(100, 0, create_test_entries(1), 0, None);

        // next_shred_index would be 0+0=0, so consumed stays 0
        // This is technically a degenerate case
        assert_eq!(cache.get_consumed(100), Some(0));
    }

    #[test]
    fn test_duplicate_batch_insert() {
        let cache = EntryCache::new(10);

        // Insert same batch twice
        cache.insert_entries(100, 0, create_test_entries(2), 5, None);
        cache.insert_entries(100, 0, create_test_entries(3), 5, None); // Overwrites

        let result = cache.get_slot_entries(100, 0).unwrap();
        assert_eq!(result.0.len(), 3); // Second insert wins
    }

    #[test]
    fn test_overwrite_with_different_num_shreds() {
        // This test catches a bug where overwriting a batch with different num_shreds
        // would leave consumed pointing to stale data (not rescanning the chain)
        let cache = EntryCache::new(10);

        // Build a chain: [0-5) -> [5-10) -> [10-15)
        cache.insert_entries(100, 0, create_test_entries(1), 5, None);
        cache.insert_entries(100, 5, create_test_entries(1), 5, None);
        cache.insert_entries(100, 10, create_test_entries(1), 5, None);

        // consumed should be 15 (all contiguous)
        assert_eq!(cache.get_consumed(100), Some(15));

        // Now overwrite batch at 0 with DIFFERENT num_shreds (3 instead of 5)
        // This creates a gap at [3-5)
        cache.insert_entries(100, 0, create_test_entries(2), 3, None);

        // consumed should be 3 (gap at [3-5))
        // If this is 15, the bug was not fixed
        assert_eq!(cache.get_consumed(100), Some(3));

        // Query from 0 should only return the first batch
        let result = cache.get_slot_entries(100, 0).unwrap();
        assert_eq!(result.0.len(), 2);
        assert_eq!(result.1, 3);

        // Query from 3 should fail (gap)
        assert!(cache.get_slot_entries(100, 3).is_none());

        // Query from 5 should fail (beyond consumed)
        assert!(cache.get_slot_entries(100, 5).is_none());
    }

    #[test]
    fn test_overwrite_fills_gap() {
        let cache = EntryCache::new(10);

        // Build a chain with a gap: [0-3) and [5-10)
        cache.insert_entries(100, 0, create_test_entries(1), 3, None);
        cache.insert_entries(100, 5, create_test_entries(1), 5, None);

        // consumed should be 3
        assert_eq!(cache.get_consumed(100), Some(3));

        // Now overwrite batch at 0 with larger num_shreds to close the gap
        cache.insert_entries(100, 0, create_test_entries(2), 5, None);

        // consumed should now be 10 (gap closed)
        assert_eq!(cache.get_consumed(100), Some(10));

        // Query from 0 should return both batches
        let result = cache.get_slot_entries(100, 0).unwrap();
        assert_eq!(result.0.len(), 3); // 2 + 1
        assert_eq!(result.1, 10); // 5 + 5
    }

    #[test]
    fn test_last_index_with_gap() {
        let cache = EntryCache::new(10);

        // Insert with gaps but set last_index (simulating LAST_SHRED_IN_SLOT before all data arrives)
        cache.insert_entries(100, 0, create_test_entries(1), 5, None);
        // Second batch at index 10 with last_index=14 (last shred in slot)
        cache.insert_entries(100, 10, create_test_entries(1), 5, Some(14));

        // consumed is still only 5 due to gap
        assert_eq!(cache.get_consumed(100), Some(5));
        // last_index is set
        assert_eq!(cache.get_last_index(100), Some(Some(14)));
        // is_full should be false (consumed=5 is not > last_index=14)
        assert_eq!(cache.is_slot_full(100), Some(false));

        let result = cache.get_slot_entries(100, 0).unwrap();
        assert_eq!(result.0.len(), 1);
        assert!(!result.2); // is_full false (gap exists)
    }

    #[test]
    fn test_is_full_computation() {
        let cache = EntryCache::new(10);

        // Insert entries with last_index set to make it full
        // consumed will be 10, last_index=9, so 10 > 9 means is_full=true
        cache.insert_entries(100, 0, create_test_entries(2), 5, None);
        cache.insert_entries(100, 5, create_test_entries(2), 5, Some(9));

        assert_eq!(cache.get_consumed(100), Some(10));
        assert_eq!(cache.get_last_index(100), Some(Some(9)));
        assert_eq!(cache.is_slot_full(100), Some(true));

        let result = cache.get_slot_entries(100, 0).unwrap();
        assert!(result.2); // is_full true
    }

    // ==================== Mid-Batch Query Tests ====================

    #[test]
    fn test_mid_batch_query() {
        let cache = EntryCache::new(10);

        // Insert a batch covering [0-10)
        cache.insert_entries(100, 0, create_test_entries(5), 10, None);

        // Query from middle of batch at index 5
        let result = cache.get_slot_entries(100, 5);
        assert!(result.is_some());
        let (entries, num_shreds, _) = result.unwrap();
        // Returns all entries from the batch (can't split entries)
        assert_eq!(entries.len(), 5);
        // num_shreds accounts for the skipped shreds (10 - 5 = 5 remaining)
        assert_eq!(num_shreds, 5);
    }

    #[test]
    fn test_mid_batch_query_with_continuation() {
        let cache = EntryCache::new(10);

        // Insert two contiguous batches: [0-10) and [10-20)
        cache.insert_entries(100, 0, create_test_entries(3), 10, None);
        cache.insert_entries(100, 10, create_test_entries(2), 10, None);

        // Query from middle of first batch at index 5
        let result = cache.get_slot_entries(100, 5);
        assert!(result.is_some());
        let (entries, num_shreds, _) = result.unwrap();
        // Returns entries from both batches
        assert_eq!(entries.len(), 5); // 3 + 2
        // num_shreds: 5 (remaining from first) + 10 (second) = 15
        assert_eq!(num_shreds, 15);
    }
}
