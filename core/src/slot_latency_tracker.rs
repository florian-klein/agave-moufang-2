//! Slot pipeline latency tracking for measuring end-to-end latency from shred
//! reception to bank completion.
//!
//! This module provides a channel-based, low-overhead mechanism to track timestamps
//! at key pipeline stages per slot, enabling measurement of optimization effectiveness.

use {
    crossbeam_channel::{Receiver, Sender, TryRecvError},
    solana_clock::Slot,
    solana_metrics::datapoint_info,
    std::{collections::HashMap, time::Instant},
};

/// Source of replay stage wakeup
#[derive(Clone, Copy, Debug)]
pub enum WakeupSource {
    /// Woken by blockstore signal (after insertion, requires is_parent_connected)
    BlockstoreSignal,
    /// Woken by timeout (no signal received)
    Timeout,
}

impl WakeupSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            WakeupSource::BlockstoreSignal => "blockstore_signal",
            WakeupSource::Timeout => "timeout",
        }
    }
}

/// Events sent to the latency aggregator
#[derive(Debug)]
pub enum LatencyEvent {
    /// First shred for a slot received in window_service
    FirstShredReceived { slot: Slot, timestamp: Instant },
    /// Replay stage woke up and will process this slot
    ReplayWakeup {
        slot: Slot,
        timestamp: Instant,
        source: WakeupSource,
    },
    /// Bank created for slot in generate_new_bank_forks
    BankCreated { slot: Slot, timestamp: Instant },
    /// Bank frozen after execution complete
    BankFrozen { slot: Slot, timestamp: Instant },
}

/// Sender for latency events - cloned to each component that records timestamps
pub type LatencyEventSender = Sender<LatencyEvent>;
/// Receiver for latency events - owned by the aggregator
pub type LatencyEventReceiver = Receiver<LatencyEvent>;

/// Create a channel pair for latency tracking.
/// Returns (sender, receiver) - sender is cloned to components, receiver goes to aggregator.
pub fn create_latency_channel() -> (LatencyEventSender, LatencyEventReceiver) {
    crossbeam_channel::unbounded()
}

/// Timestamps collected for a single slot
#[derive(Default)]
struct SlotTimestamps {
    first_shred_received: Option<Instant>,
    replay_wakeup: Option<Instant>,
    wakeup_source: Option<WakeupSource>,
    bank_created: Option<Instant>,
    bank_frozen: Option<Instant>,
}

/// Aggregates latency events and emits metrics.
/// Runs in the replay stage thread, processing events non-blocking.
pub struct SlotLatencyAggregator {
    receiver: LatencyEventReceiver,
    slots: HashMap<Slot, SlotTimestamps>,
}

impl SlotLatencyAggregator {
    pub fn new(receiver: LatencyEventReceiver) -> Self {
        Self {
            receiver,
            slots: HashMap::new(),
        }
    }

    /// Non-blocking: drain all pending events from channel and update state
    pub fn process_pending_events(&mut self) {
        loop {
            match self.receiver.try_recv() {
                Ok(event) => self.handle_event(event),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
    }

    fn handle_event(&mut self, event: LatencyEvent) {
        match event {
            LatencyEvent::FirstShredReceived { slot, timestamp } => {
                let entry = self.slots.entry(slot).or_default();
                // Only record first occurrence
                if entry.first_shred_received.is_none() {
                    entry.first_shred_received = Some(timestamp);
                }
            }
            LatencyEvent::ReplayWakeup {
                slot,
                timestamp,
                source,
            } => {
                let entry = self.slots.entry(slot).or_default();
                // Only record first wakeup for this slot
                if entry.replay_wakeup.is_none() {
                    entry.replay_wakeup = Some(timestamp);
                    entry.wakeup_source = Some(source);
                }
            }
            LatencyEvent::BankCreated { slot, timestamp } => {
                let entry = self.slots.entry(slot).or_default();
                if entry.bank_created.is_none() {
                    entry.bank_created = Some(timestamp);
                }
            }
            LatencyEvent::BankFrozen { slot, timestamp } => {
                let entry = self.slots.entry(slot).or_default();
                if entry.bank_frozen.is_none() {
                    entry.bank_frozen = Some(timestamp);
                }
            }
        }
    }

    /// Emit metrics for slots at or below new_root, then remove them from tracking.
    /// Call this when root advances.
    pub fn report_and_cleanup(&mut self, new_root: Slot) {
        // First, process any pending events
        self.process_pending_events();

        // Collect slots to report (at or below root)
        let slots_to_report: Vec<Slot> = self
            .slots
            .keys()
            .filter(|&&s| s <= new_root)
            .copied()
            .collect();

        for slot in slots_to_report {
            if let Some(ts) = self.slots.remove(&slot) {
                self.emit_metrics(slot, &ts);
            }
        }
    }

    fn emit_metrics(&self, slot: Slot, ts: &SlotTimestamps) {
        let first_shred = match ts.first_shred_received {
            Some(t) => t,
            None => return, // No first shred timestamp, skip this slot
        };

        // Compute deltas from first_shred_received
        let first_shred_to_replay_wakeup = ts
            .replay_wakeup
            .map(|t| t.duration_since(first_shred).as_micros() as i64)
            .unwrap_or(-1);

        let first_shred_to_bank_created = ts
            .bank_created
            .map(|t| t.duration_since(first_shred).as_micros() as i64)
            .unwrap_or(-1);

        let first_shred_to_bank_frozen = ts
            .bank_frozen
            .map(|t| t.duration_since(first_shred).as_micros() as i64)
            .unwrap_or(-1);

        let wakeup_source_str = ts
            .wakeup_source
            .map(|s| s.as_str())
            .unwrap_or("unknown");

        datapoint_info!(
            "slot-pipeline-latency",
            ("slot", slot as i64, i64),
            ("first_shred_to_replay_wakeup_us", first_shred_to_replay_wakeup, i64),
            ("first_shred_to_bank_created_us", first_shred_to_bank_created, i64),
            ("first_shred_to_bank_frozen_us", first_shred_to_bank_frozen, i64),
            ("wakeup_source", wakeup_source_str, String),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_event_handling() {
        let (sender, receiver) = create_latency_channel();
        let mut aggregator = SlotLatencyAggregator::new(receiver);

        let now = Instant::now();
        let slot = 100;

        // Send events
        sender
            .send(LatencyEvent::FirstShredReceived {
                slot,
                timestamp: now,
            })
            .unwrap();
        sender
            .send(LatencyEvent::ReplayWakeup {
                slot,
                timestamp: now,
                source: WakeupSource::BlockstoreSignal,
            })
            .unwrap();
        sender
            .send(LatencyEvent::BankCreated {
                slot,
                timestamp: now,
            })
            .unwrap();
        sender
            .send(LatencyEvent::BankFrozen {
                slot,
                timestamp: now,
            })
            .unwrap();

        // Process events
        aggregator.process_pending_events();

        // Verify state
        assert!(aggregator.slots.contains_key(&slot));
        let ts = aggregator.slots.get(&slot).unwrap();
        assert!(ts.first_shred_received.is_some());
        assert!(ts.replay_wakeup.is_some());
        assert!(ts.bank_created.is_some());
        assert!(ts.bank_frozen.is_some());
        assert!(matches!(ts.wakeup_source, Some(WakeupSource::BlockstoreSignal)));
    }

    #[test]
    fn test_cleanup_removes_old_slots() {
        let (sender, receiver) = create_latency_channel();
        let mut aggregator = SlotLatencyAggregator::new(receiver);

        let now = Instant::now();

        // Add events for slots 100, 101, 102
        for slot in 100..=102 {
            sender
                .send(LatencyEvent::FirstShredReceived {
                    slot,
                    timestamp: now,
                })
                .unwrap();
        }

        aggregator.process_pending_events();
        assert_eq!(aggregator.slots.len(), 3);

        // Cleanup with root at 101 should remove 100 and 101
        aggregator.report_and_cleanup(101);
        assert_eq!(aggregator.slots.len(), 1);
        assert!(aggregator.slots.contains_key(&102));
    }

    #[test]
    fn test_only_first_occurrence_recorded() {
        let (sender, receiver) = create_latency_channel();
        let mut aggregator = SlotLatencyAggregator::new(receiver);

        let first_time = Instant::now();
        let slot = 100;

        // Send first event
        sender
            .send(LatencyEvent::FirstShredReceived {
                slot,
                timestamp: first_time,
            })
            .unwrap();

        // Try to send another (should be ignored)
        std::thread::sleep(std::time::Duration::from_millis(1));
        let second_time = Instant::now();
        sender
            .send(LatencyEvent::FirstShredReceived {
                slot,
                timestamp: second_time,
            })
            .unwrap();

        aggregator.process_pending_events();

        let ts = aggregator.slots.get(&slot).unwrap();
        assert_eq!(ts.first_shred_received.unwrap(), first_time);
    }
}
