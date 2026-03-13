#![allow(clippy::missing_safety_doc)]
use dashmap::DashMap;
use gxhash::GxBuildHasher;
use likely_stable::unlikely;
use memmap2::{MmapMut, MmapOptions};
use nix::fcntl::OFlag;
use nix::sys::mman::{shm_open as posix_shm_open, shm_unlink};
use nix::sys::stat::Mode;
use nix::unistd::ftruncate;
use solana_pubkey::Pubkey;
use std::fs::File;
use std::io::{self, Error as IoError, Result as IoResult};
use std::mem::{align_of, size_of};
use std::os::fd::OwnedFd;
use std::ptr::{write};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

#[cfg(target_os = "linux")]
use libc::{MADV_DONTDUMP, MADV_HUGEPAGE, MADV_WILLNEED, madvise};

/// ===== Types =====

pub const MAGIC: u32 = 0x5348_4D41; // 'SHMA'
pub const VERSION: u32 = 5; // bump for header change (added gen field to RecHeader)
pub const IS_COMMITED_MASK: u64 = u64::MAX;
pub const IS_WRAP_MARKER: u64 = 0xf0f0_f0f0_f0f0_f0f0;

/// Sentinel pubkey used as a tx-end marker. The reader checks for this to
/// know that all account updates for a given `trigger_sig` have been written.
pub const TX_END_MARKER_PUBKEY: [u8; 32] = [0xFF; 32];

/// Cache-line size hint
const CL: usize = 64;

#[repr(C, align(64))]
#[derive(Debug)]
pub struct ShmHeader {
    // ---- line 0 ----
    pub magic: u32,
    pub version: u32,
    pub capacity: u64,
    _pad0: [u8; CL - (4 + 4 + 8)],

    // ---- line 1: writers ----
    /// Monotonic linear allocator cursor for MPMC reservation (does not wrap).
    pub alloc_off: AtomicU64,
    _pad1: [u8; CL - 8],

    // ---- line 2: reader-only (DEPRECATED; kept for compat) ----
    pub read_off: AtomicU64,
    _pad2: [u8; CL - 8],

    // ---- line 3: misc/rare ----
    pub flags: AtomicU32,
    _pad3a: u32,
    pub _reserved: [u8; 8],
    /// Kept for compat; unused by the new reader/writer (was last_end_off).
    pub last_end_off: AtomicU64,
    _pad3b: [u8; CL - (4 + 4 + 8 + 8)],
}

unsafe impl Send for ShmHeader {}
unsafe impl Sync for ShmHeader {}

/// Per-record header. Safe to mark Pod/Zeroable: no padding.
use bytemuck::{Pod, Zeroable};
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct RecHeader {
    pub commited: u64,
    pub total_len: u32,  // header+payload; 0 == wrap marker / not-yet-published
    pub data_len: u32,   // payload size
    pub generation: u64, // generation counter: which lap around the ring (cur / cap)
    pub pubkey: [u8; 32],
    pub lamports: u64,
    pub trigger_sig: [u8; 64],
}
impl RecHeader {
    #[inline(always)]
    pub const fn size() -> usize {
        size_of::<Self>()
    }
}

/// ===== Latency Tracing =====
#[cfg(feature = "latency-tracing")]
use std::cell::UnsafeCell;

#[cfg(feature = "latency-tracing")]
#[derive(Clone, Copy, Debug)]
enum TraceOp {
    Entry,
    IdleCheck {
        cur_seq: u64,
        idle_seq: u64,
        matched: bool,
    },
    LoadCommitted {
        offset: usize,
        committed: u64,
    },
    WrapMarker,
    NotCommitted,
    DashInsertNew,
    DashSameLengthUpdated,
    DashSameLengthUnchanged,
    DashDataLengthDecreased,
    DashDataLengthIncreased,
    DashNewInsert(Pubkey),
    GenValidation {
        rec_gen: u64,
        expected_gen: u64,
        matched: bool,
    },
    StaleRecord,
    RecordRead {
        lamports: u64,
        data_len: usize,
    },
    Complete,
}

#[cfg(feature = "latency-tracing")]
#[derive(Clone, Copy, Debug)]
struct TraceEntry {
    timestamp_us: u64,
    op: TraceOp,
}

#[cfg(feature = "latency-tracing")]
struct TraceBufferInner {
    entries: [Option<TraceEntry>; 128],
    idx: usize,
}

#[cfg(feature = "latency-tracing")]
struct TraceBuffer {
    inner: UnsafeCell<TraceBufferInner>,
}

#[cfg(feature = "latency-tracing")]
impl std::fmt::Debug for TraceBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceBuffer").finish_non_exhaustive()
    }
}

#[cfg(feature = "latency-tracing")]
unsafe impl Send for TraceBuffer {}
#[cfg(feature = "latency-tracing")]
unsafe impl Sync for TraceBuffer {}

#[cfg(feature = "latency-tracing")]
impl TraceBuffer {
    fn new() -> Self {
        Self {
            inner: UnsafeCell::new(TraceBufferInner {
                entries: [None; 128],
                idx: 0,
            }),
        }
    }

    fn record(&self, op: TraceOp) {
        let timestamp_us = monotonic_micros();
        unsafe {
            let inner = &mut *self.inner.get();
            inner.entries[inner.idx] = Some(TraceEntry { timestamp_us, op });
            inner.idx = (inner.idx + 1) % inner.entries.len();
        }
    }

    fn dump(&self, start_time: u64) {
        unsafe {
            let inner = &*self.inner.get();
            eprintln!("=== Latency trace (times relative to start) ===");
            let mut sorted_entries = Vec::new();

            // Collect all valid entries
            for i in 0..inner.entries.len() {
                let actual_idx = (inner.idx + i) % inner.entries.len();
                if let Some(entry) = &inner.entries[actual_idx] {
                    sorted_entries.push(*entry);
                }
            }

            for entry in sorted_entries {
                let delta = entry.timestamp_us.saturating_sub(start_time);
                eprintln!("  +{:6}us: {:?}", delta, entry.op);
            }
            eprintln!("=== End trace ===");
        }
    }
}

/// ===== Errors =====

#[derive(thiserror::Error, Debug)]
pub enum ShmError {
    #[error("shared memory io: {0}")]
    Io(#[from] IoError),
    #[error("nix: {0}")]
    Nix(#[from] nix::Error),
    #[error("invalid header")]
    InvalidHeader,
    #[error("insufficient space")]
    NoSpace,
    #[error("invalid shm name: {0}")]
    InvalidShmName(String),
}

#[inline(always)]
pub fn monotonic_micros() -> u64 {
    use libc::{CLOCK_MONOTONIC_RAW, clock_gettime, timespec};
    let mut ts = timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { clock_gettime(CLOCK_MONOTONIC_RAW, &mut ts) };
    (ts.tv_sec as u64) * 1_000_000 + (ts.tv_nsec as u64) / 1_000
}

/// ===== Utils =====

#[inline(always)]
fn align_up(x: usize, a: usize) -> usize {
    debug_assert!(a.is_power_of_two());
    (x + (a - 1)) & !(a - 1)
}

#[inline(always)]
fn is_valid_posix_shm_name(s: &str) -> bool {
    // POSIX shm name must be exactly "/name" (one leading slash, no others)
    s.starts_with('/') && !s[1..].contains('/')
}

#[cfg(target_os = "linux")]
fn prefault_and_mlock(ptr: *mut u8, len: usize) {
    unsafe {
        // Fault pages in via volatile reads (no writes!)
        for off in (0..len).step_by(4096) {
            core::ptr::read_volatile(ptr.add(off));
        }
        let _ = libc::mlock(ptr as *const _, len);
    }
}

/// Open/create a POSIX shm object (name like "foo" or "/foo") and size it on create.
/// No reliance on nix Error internals: try EXCL first, then fallback without EXCL.
fn create_or_open_shm_posix(
    name_or_short: &str,
    total_len: usize,
    create: bool,
) -> Result<(File, bool), ShmError> {
    // Normalize to "/name"
    let normalized = if name_or_short.starts_with('/') {
        name_or_short.to_string()
    } else {
        format!("/{}", name_or_short)
    };
    if !is_valid_posix_shm_name(&normalized) {
        return Err(ShmError::InvalidShmName(normalized));
    }

    let mode = Mode::from_bits_truncate(0o600);

    // Try exclusive create first if requested
    let ofd_excl: Result<OwnedFd, nix::Error> = if create {
        posix_shm_open(
            normalized.as_str(),
            OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_EXCL,
            mode,
        )
    } else {
        posix_shm_open(normalized.as_str(), OFlag::O_RDWR, mode)
    };

    // Fall back to non-exclusive open (or create without EXCL)
    let (ofd, created) = match ofd_excl {
        Ok(fd) => (fd, true),
        Err(_) => posix_shm_open(
            normalized.as_str(),
            if create {
                OFlag::O_RDWR | OFlag::O_CREAT
            } else {
                OFlag::O_RDWR
            },
            mode,
        )
        .map(|fd| (fd, false))?,
    };

    // OwnedFd -> File
    let file: File = ofd.into();

    if create {
        if created {
            // We truly created it; size it to the requested length.
            ftruncate(&file, total_len as i64)?;
        } else {
            // Existing object: verify size instead of clobbering.
            let cur = file.metadata()?.len() as usize;
            if cur != total_len {
                return Err(ShmError::Io(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("existing shm has size {}, expected {}", cur, total_len),
                )));
            }
        }
    }

    Ok((file, created))
}

fn map_mut(file: &File, size: usize) -> IoResult<MmapMut> {
    // SAFETY: file valid; keep mmap in struct to extend lifetime
    let mmap = unsafe { MmapOptions::new().len(size).map_mut(file) }?;
    #[cfg(target_os = "linux")]
    unsafe {
        let ptr = mmap.as_ptr() as *mut libc::c_void;
        let _ = madvise(ptr, size, MADV_HUGEPAGE);
        let _ = madvise(ptr, size, MADV_WILLNEED);
        let _ = madvise(ptr, size, MADV_DONTDUMP);
    }
    Ok(mmap)
}

/// ===== Writer / Reader =====

pub struct ShmWriter {
    hdr: *mut ShmHeader,
    buf: *mut u8,
    cap: usize,
    mmap: MmapMut,
    _file: File, // Keep FD alive
}
unsafe impl Send for ShmWriter {}
unsafe impl Sync for ShmWriter {}

#[derive(Debug)]
pub struct ShmReader {
    hdr: *mut ShmHeader,
    buf: *mut u8,
    cap: usize,
    mmap: MmapMut,
    scratch: Vec<u8>,
    /// Per-reader cursor (multi-reader safe, private to this reader)
    r_off: AtomicU64,
    /// Reader's current expected generation (which lap we're on)
    read_gen: AtomicU64,
    #[cfg(feature = "latency-tracing")]
    trace_buffer: TraceBuffer,
    _file: File, // Keep FD alive
}

unsafe impl Send for ShmReader {}
unsafe impl Sync for ShmReader {}

impl ShmWriter {
    /// Create a new shared segment. `path_or_name` must be "name" or "/name".
    pub fn create(path_or_name: &str, capacity_bytes: usize) -> Result<Self, ShmError> {
        if capacity_bytes == 0 {
            return Err(ShmError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "capacity_bytes=0",
            )));
        }
        let total = align_up(size_of::<ShmHeader>() + capacity_bytes, CL.max(4096));
        if total < size_of::<ShmHeader>() + RecHeader::size() {
            return Err(ShmError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "capacity too small",
            )));
        }

        let (file, _created) = create_or_open_shm_posix(path_or_name, total, true)?;
        let mut mmap = map_mut(&file, total)?;
        #[cfg(target_os = "linux")]
        unsafe {
            let ring_ptr = mmap.as_mut_ptr().add(size_of::<ShmHeader>());
            prefault_and_mlock(ring_ptr, capacity_bytes);
        }

        let hdr_ptr = mmap.as_mut_ptr() as *mut ShmHeader;
        let buf_ptr = unsafe { mmap.as_mut_ptr().add(size_of::<ShmHeader>()) } as *mut u8;

        // Init header
        let hdr = ShmHeader {
            magic: MAGIC,
            version: VERSION,
            capacity: capacity_bytes as u64,
            _pad0: [0; CL - (4 + 4 + 8)],

            alloc_off: AtomicU64::new(0),
            _pad1: [0; CL - 8],

            // Kept for compat; not used by multi-reader code.
            read_off: AtomicU64::new(0),
            _pad2: [0; CL - 8],

            flags: AtomicU32::new(0),
            _pad3a: 0,
            _reserved: [0; 8],
            last_end_off: AtomicU64::new(0),
            _pad3b: [0; CL - (4 + 4 + 8 + 8)],
        };

        unsafe { write(hdr_ptr, hdr) };
        debug_assert_eq!((hdr_ptr as usize) % align_of::<ShmHeader>(), 0);

        Ok(Self {
            hdr: hdr_ptr,
            buf: buf_ptr,
            cap: capacity_bytes,
            mmap,
            _file: file,
        })
    }

    #[inline(always)]
    pub fn push(
        &self,
        pubkey: &[u8; 32],
        data: &[u8],
        lamports: u64,
        trigger_sig: &[u8; 64],
    ) -> Result<(u64, usize), ShmError> {
        let hdr = unsafe { &*self.hdr };

        let data_len = data.len();
        if data_len == 0 {
            return Ok((1, 0));
        }
        let pad = (8 - (data_len & 7)) & 7; // 0..=7
        let padded_len = data_len + pad;
        // FIXED: Include trailer size (4 bytes) in need calculation, then round up to 8-byte alignment
        // Without the trailer, it overlaps with the last 4 bytes of payload!
        let need_unaligned = RecHeader::size() + padded_len + core::mem::size_of::<u32>();
        let need = (need_unaligned + 7) & !7; // Round up to 8-byte boundary
        let cap = self.cap;

        if unlikely(need > cap) {
            return Ok((2, 0));
        }
        if unlikely(need > u32::MAX as usize) {
            return Ok((3, 0));
        }

        // === Reserve space (MPMC) and derive generation ===
        // Generation is computed from the reserved linear offset: gen = cur / cap
        // This ensures each record gets a consistent generation number based on which
        // lap around the ring the reservation occurred, regardless of write order.
        let (rec_start, generation) = loop {
            let cur = hdr.alloc_off.load(Ordering::Relaxed);
            let start = (cur as usize) % cap;
            // we need the 8 here for the wrap marker (always needs to fit)
            let fits = start + need + core::mem::size_of::<u64>() <= cap;
            let add = if fits { need } else { (cap - start) + need };
            let next = cur.wrapping_add(add as u64);

            if hdr
                .alloc_off
                .compare_exchange_weak(cur, next, Ordering::AcqRel, Ordering::SeqCst)
                .is_ok()
            {
                // Derive generation from the reserved offset (before wrapping).
                // Every full pass over the ring bumps the generation.
                let rec_linear = if fits { cur } else { next - need as u64 };
                let generation_val = rec_linear / cap as u64;

                if fits {
                    break (start, generation_val as u64);
                } else {
                    // Write wrap marker
                    unsafe {
                        let wrap_ptr = self.buf.add(start) as *mut AtomicU64;
                        (*wrap_ptr).store(IS_WRAP_MARKER, Ordering::Relaxed);
                    }
                    break (0usize, generation_val as u64);
                }
            }
            core::hint::spin_loop();
        };

        let rh = RecHeader {
            commited: 0,
            total_len: need as u32,
            data_len: data_len as u32,
            generation: 0, // placeholder
            pubkey: *pubkey,
            lamports,
            trigger_sig: *trigger_sig,
        };

        let hdr_sz = RecHeader::size();

        unsafe {
            // 0) mark slot as uncommitted
            let commit_ptr = self.buf.add(rec_start) as *const AtomicU64;
            (*commit_ptr).store(0, Ordering::Relaxed);

            // 1a) write total_len + data_len
            let src_len = (&rh as *const RecHeader as *const u8).add(8);
            let dst_len = self.buf.add(rec_start + 8);
            core::ptr::copy_nonoverlapping(src_len, dst_len, 8);
            // this writes bytes 8..16 (tl + dl)

            // 1b) write rest of header AFTER generation
            let src_rest = (&rh as *const RecHeader as *const u8).add(24);
            let dst_rest = self.buf.add(rec_start + 24);
            core::ptr::copy_nonoverlapping(src_rest, dst_rest, hdr_sz - 24);

            // 2) write payload
            let payload_dst = self.buf.add(rec_start + hdr_sz);
            core::ptr::copy_nonoverlapping(data.as_ptr(), payload_dst, data_len);
            if pad != 0 {
                core::ptr::write_bytes(payload_dst.add(data_len), 0, pad);
            }

            // 3) publish generation atomically
            let generation_ptr = self.buf.add(rec_start + 16) as *const AtomicU64;
            (*generation_ptr).store(generation, Ordering::Release);

            // 4) publish commit last
            (*commit_ptr).store(IS_COMMITED_MASK, Ordering::Release);
        }

        Ok((0, rec_start))
    }

    /// Write a lightweight tx-end marker carrying only the trigger signature.
    /// Uses `TX_END_MARKER_PUBKEY` as the sentinel pubkey and a 1-byte
    /// payload (push requires data_len > 0).
    #[inline(always)]
    pub fn push_tx_end_marker(
        &self,
        trigger_sig: &[u8; 64],
    ) -> Result<(u64, usize), ShmError> {
        self.push(&TX_END_MARKER_PUBKEY, &[0u8; 1], 0, trigger_sig)
    }
}

impl Drop for ShmWriter {
    fn drop(&mut self) {}
}

impl ShmReader {
    /// Open an existing shm by name ("foo" or "/foo").
    pub fn open(path_or_name: &str) -> Result<Self, ShmError> {
        let (file, _created) = create_or_open_shm_posix(path_or_name, 0, false)?;
        let len = file.metadata()?.len() as usize;
        if len < size_of::<ShmHeader>() {
            return Err(ShmError::InvalidHeader);
        }
        let mmap = map_mut(&file, len)?;
        let hdr_ptr = mmap.as_ptr() as *mut ShmHeader;
        let hdr = unsafe { &*hdr_ptr };
        if hdr.magic != MAGIC || hdr.version != VERSION {
            return Err(ShmError::InvalidHeader);
        }
        let ring_len = hdr.capacity as usize;
        #[cfg(feature = "debug-prints")]
        println!("ring len {:?}", ring_len);
        let avail = len.saturating_sub(size_of::<ShmHeader>());
        if ring_len == 0 || ring_len > avail {
            return Err(ShmError::InvalidHeader);
        }
        #[cfg(target_os = "linux")]
        unsafe {
            let ring_ptr = (mmap.as_ptr() as *mut u8).add(size_of::<ShmHeader>());
            prefault_and_mlock(ring_ptr, ring_len);
        }
        let buf_ptr = unsafe { (mmap.as_ptr() as *mut u8).add(size_of::<ShmHeader>()) };

        // FIXED: Use alloc_off to find the tip position.
        // With multiple producers, alloc_off represents the "reservation boundary" -
        // everything before it has been reserved (though not necessarily published yet).
        // The reader will skip unpublished records (total_len == 0) automatically.
        // This ensures late readers start near the end and only see new records.
        let alloc = hdr.alloc_off.load(Ordering::Acquire);
        let tip = (alloc as usize % ring_len) as u64;
        let start = tip;

        // Initialize read_gen by computing from alloc_off.
        // This allows readers that attach later to start reading at the current lap.
        // Generation = alloc_off / capacity gives us which lap we're on.
        let cur_gen = (alloc as usize) / ring_len;

        Ok(Self {
            hdr: hdr_ptr,
            buf: buf_ptr,
            cap: ring_len,
            mmap,
            scratch: Vec::with_capacity(64 * 1024),
            r_off: AtomicU64::new(start),
            read_gen: AtomicU64::new(cur_gen as u64),
            #[cfg(feature = "latency-tracing")]
            trace_buffer: TraceBuffer::new(),
            _file: file,
        })
    }

    pub fn seek_to_start(&mut self) {
        self.r_off.store(0, Ordering::Relaxed);
        self.read_gen.store(0, Ordering::Relaxed);
    }

    #[cfg(feature = "latency-tracing")]
    pub fn dump_trace(&self) {
        self.trace_buffer.dump(monotonic_micros());
    }

    /// Same as poll_next, but writes into a DashMap and keeps &self by using the atomic local cursor.
    #[inline(always)]
    pub fn poll_next_into_dashmap(
        &self,
        map: &DashMap<Pubkey, Vec<u8>, GxBuildHasher>,
    ) -> Option<([u8; 32], u64, bool, bool, u64)> {
        // Spin loop - never return None, always wait for data
        loop {
            // #[cfg(feature = "latency-tracing")]
            // self.trace_buffer.record(TraceOp::Entry);

            #[cfg(feature = "debug-prints")]
            let hdr_ref = unsafe { &*self.hdr };
            let hdr_sz = RecHeader::size();

            let raw = self.r_off.load(Ordering::Relaxed);
            let r = (raw as usize) % self.cap;

            #[cfg(feature = "debug-prints")]
            let cur_alloc = hdr_ref.alloc_off.load(Ordering::Relaxed);
            #[cfg(feature = "debug-prints")]
            println!("alloc off {:?}", cur_alloc);
            #[cfg(feature = "debug-prints")]
            println!("alloc off mod cap {:?}", cur_alloc % self.cap as u64);
            #[cfg(feature = "debug-prints")]
            println!("ccap off {:?}", self.cap);
            #[cfg(feature = "debug-prints")]
            println!("raw {:?}", raw);
            #[cfg(feature = "debug-prints")]
            println!("r {:?}", r);

            let commited =
                unsafe { (*(self.buf.add(r) as *const AtomicU64)).load(Ordering::Acquire) };

            // #[cfg(feature = "latency-tracing")]
            // self.trace_buffer.record(TraceOp::LoadCommitted {
            //     offset: r,
            //     committed: commited
            // });
            //
            #[cfg(feature = "debug-prints")]
            println!("commited: {:?}", commited);

            if unlikely(commited == IS_WRAP_MARKER) {
                eprintln!("[reader] WRAP MARKER at offset={r}, resetting to 0");
                #[cfg(feature = "latency-tracing")]
                self.trace_buffer.record(TraceOp::WrapMarker);
                self.r_off.store(0, Ordering::Relaxed);
                continue;
            }

            if commited != IS_COMMITED_MASK {
                // not committed yet — spin
                continue;
            }

            // ---------- Generation validation ----------

            let gen_ptr = unsafe { self.buf.add(r + 16) as *const core::sync::atomic::AtomicU64 };
            let rec_gen = unsafe { (*gen_ptr).load(Ordering::Acquire) };
            let reader_gen = self.read_gen.load(Ordering::Relaxed);

            #[cfg(feature = "latency-tracing")]
            self.trace_buffer.record(TraceOp::GenValidation {
                rec_gen,
                expected_gen: reader_gen,
                matched: rec_gen == reader_gen,
            });

            #[cfg(feature = "debug-prints")]
            println!("rec_gen: {:?}, reader_gen: {:?}", rec_gen, reader_gen);

            if rec_gen == reader_gen + 1 {
                eprintln!("[reader] GEN WRAP rec_gen={rec_gen} reader_gen={reader_gen} at offset={r}, resetting to 0");
                self.r_off.store(0, Ordering::Relaxed);
                self.read_gen.store(rec_gen, Ordering::Relaxed);
                continue;
            } else if rec_gen != reader_gen {
                eprintln!("[reader] STALE rec_gen={rec_gen} reader_gen={reader_gen} at offset={r}, skipping");
                #[cfg(feature = "latency-tracing")]
                self.trace_buffer.record(TraceOp::StaleRecord);
                continue;
            }

            let rh_ptr = (unsafe { self.buf.add(r) }) as *const RecHeader;
            let need = unsafe { (*rh_ptr).total_len as usize };

            let rec_start = r;
            let rec_end = r + need;

            #[cfg(feature = "debug-prints")]
            {
                println!("rec start {}", rec_start);
                println!("rec end {}", rec_end);
            }

            let data_len = unsafe { (*rh_ptr).data_len as usize };

            let mut updated = true;

            let data_off = rec_start + hdr_sz;
            let src_ptr = unsafe { self.buf.add(data_off) };

            // Read header fields
            let lamports = unsafe { (*rh_ptr).lamports };
            let pubkey = unsafe { (*rh_ptr).pubkey };

            #[cfg(feature = "latency-tracing")]
            self.trace_buffer
                .record(TraceOp::RecordRead { lamports, data_len });

            let receive_ts = monotonic_micros();

            let is_tx_end = pubkey == TX_END_MARKER_PUBKEY;

            if is_tx_end {
                eprintln!(
                    "[reader] TX_END_MARKER at offset={rec_start} data_len={data_len} lamports={lamports}"
                );
            } else {
                eprintln!(
                    "[reader] ACCOUNT key={} offset={rec_start} data_len={data_len} lamports={lamports}",
                    Pubkey::new_from_array(pubkey)
                );
            }

            // Only insert real accounts into the DashMap, skip end markers
            if !is_tx_end {
                map.entry(pubkey.into())
                    .and_modify(|v| {
                        if v.len() == data_len {
                            // Fast path: check if data unchanged
                            let src_slice =
                                unsafe { core::slice::from_raw_parts(src_ptr, data_len) };
                            if v.as_slice() == src_slice {
                                updated = false;
                                return;
                            }
                            // Copy directly from SHM
                            unsafe {
                                core::ptr::copy_nonoverlapping(
                                    src_ptr,
                                    v.as_mut_ptr(),
                                    data_len,
                                );
                            }

                            #[cfg(feature = "latency-tracing")]
                            self.trace_buffer.record(TraceOp::DashSameLengthUpdated);
                        } else if v.capacity() >= data_len {
                            unsafe {
                                v.set_len(data_len);
                                core::ptr::copy_nonoverlapping(
                                    src_ptr,
                                    v.as_mut_ptr(),
                                    data_len,
                                );
                            }
                            #[cfg(feature = "latency-tracing")]
                            self.trace_buffer.record(TraceOp::DashDataLengthDecreased);
                        } else {
                            // Reallocate
                            let mut new_vec = Vec::with_capacity(data_len);
                            unsafe {
                                new_vec.set_len(data_len);
                                core::ptr::copy_nonoverlapping(
                                    src_ptr,
                                    new_vec.as_mut_ptr(),
                                    data_len,
                                );
                            }
                            *v = new_vec;
                            #[cfg(feature = "latency-tracing")]
                            self.trace_buffer.record(TraceOp::DashDataLengthIncreased);
                        }
                    })
                    .or_insert_with(|| {
                        let mut new_vec = Vec::with_capacity(data_len);
                        unsafe {
                            new_vec.set_len(data_len);
                            core::ptr::copy_nonoverlapping(
                                src_ptr,
                                new_vec.as_mut_ptr(),
                                data_len,
                            );
                        }
                        #[cfg(feature = "latency-tracing")]
                        self.trace_buffer
                            .record(TraceOp::DashNewInsert(Pubkey::new_from_array(pubkey)));
                        new_vec
                    });
            }

            self.r_off.store(rec_end as u64, Ordering::Relaxed);

            #[cfg(feature = "latency-tracing")]
            {
                self.trace_buffer.record(TraceOp::Complete);
                let elapsed = monotonic_micros().saturating_sub(lamports);
                if elapsed > 1000 {
                    eprintln!(
                        "\n!!! High latency detected: {}us in poll_next_into_dashmap !!!",
                        elapsed
                    );
                    self.trace_buffer.dump(lamports);
                }
            }

            return Some((pubkey, lamports, updated, !is_tx_end, receive_ts));
        } // end of loop
    }
}

/// Optional helper if you want to clean up stuck segments (e.g., after a sudo run).
pub fn shm_unlink_if_exists(name_or_short: &str) -> Result<(), ShmError> {
    let normalized = if name_or_short.starts_with('/') {
        name_or_short.to_string()
    } else {
        format!("/{}", name_or_short)
    };
    if !is_valid_posix_shm_name(&normalized) {
        return Err(ShmError::InvalidShmName(normalized));
    }
    let _ = shm_unlink(normalized.as_str()); // ignore ENOENT
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dashmap::DashMap;
    use gxhash::GxBuildHasher;
    use std::any::type_name;
    use std::mem::size_of;
    use std::ptr;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    fn new_writer_reader(cap: usize, name: &str) -> (ShmWriter, ShmReader) {
        let _ = shm_unlink_if_exists(name);
        let w = ShmWriter::create(name, cap).expect("create shm");
        let r = ShmReader::open(name).expect("open shm");
        (w, r)
    }

    /// Helper: write a single record "by hand" into the ring buffer.
    ///
    /// `publish_total_len` is what will be written into total_len and the trailer;
    /// it can be made inconsistent on purpose to test corruption paths.
    unsafe fn write_record_raw(
        buf: *mut u8,
        cap: usize,
        start: usize,
        data_len: usize,
        generation: u64,
        pubkey: [u8; 32],
        lamports: u64,
        publish_total_len: u32,
    ) {
        let hdr_sz = RecHeader::size();
        let pad = (8 - (data_len & 7)) & 7; // 0..=7
        let need = hdr_sz + data_len + pad;
        debug_assert_eq!(need & 7, 0);

        let rec_start = start;

        let rh = RecHeader {
            commited: IS_COMMITED_MASK,
            total_len: publish_total_len,
            data_len: data_len as u32,
            generation,
            pubkey,
            lamports,
            is_end: 0,
        };

        // 1) header (with total_len = 0)
        ptr::copy_nonoverlapping(
            &rh as *const RecHeader as *const u8,
            buf.add(rec_start),
            hdr_sz,
        );

        // 2) payload, with same physical layout the reader assumes
        let payload_start = rec_start + hdr_sz;

        if payload_start + data_len <= cap {
            // Non-wrapped record: one contiguous block.
            for i in 0..data_len.min(4096) {
                *buf.add(payload_start + i) = (i & 0xFF) as u8;
            }
        } else {
            // Wrapped record: first chunk at the end, second chunk at the beginning.
            let first = cap - rec_start - hdr_sz;
            let second = data_len.saturating_sub(first);

            for i in 0..first.min(data_len).min(4096) {
                *buf.add(payload_start + i) = (i & 0xFF) as u8;
            }

            for i in 0..second.min(4096) {
                *buf.add(i) = ((first + i) & 0xFF) as u8;
            }
        }

        // Padding contents don’t matter for our tests.

        // 3) trailer at end-of-record (aligned, like real writer)
        let tptr = buf.add(rec_start + need - core::mem::size_of::<u32>()) as *mut u32;
        *tptr = publish_total_len;

        // 4) publish: write total_len as AtomicU32
        let pub_ptr = buf.add(rec_start) as *const AtomicU64;
        (*(pub_ptr)).store(IS_COMMITED_MASK, Ordering::Relaxed);
    }

    #[test]
    fn header_offsets_are_atomic() {
        let dummy = ShmHeader {
            magic: 0,
            version: 0,
            capacity: 0,
            _pad0: [0; CL - (4 + 4 + 8)],
            alloc_off: AtomicU64::new(0),

            _pad1: [0; CL - 8],

            read_off: AtomicU64::new(0), // deprecated but present
            _pad2: [0; CL - 8],

            flags: AtomicU32::new(0),
            _pad3a: 0,
            _reserved: [0; 8],
            last_end_off: AtomicU64::new(0),
            _pad3b: [0; CL - (4 + 4 + 8 + 8)],
        };

        fn ty_of<T>(_: &T) -> &'static str {
            type_name::<T>()
        }

        assert!(ty_of(&dummy.read_off).contains("AtomicU64"));
        assert!(ty_of(&dummy.last_end_off).contains("AtomicU64"));
        assert!(ty_of(&dummy.flags).contains("AtomicU32"));
    }

    #[test]
    fn publish_flag_is_4byte_aligned_after_each_push() {
        let (w, _r) = new_writer_reader(8192, "/t_pubalign_ok");
        let pubkey = [0u8; 32];

        for payload_len in 0..16 {
            let (_seq, start) = w
                .push(&pubkey, &vec![0xAB; payload_len], 42, 1)
                .expect("push");
            let need = RecHeader::size() + ((payload_len + 7) & !7);
            let next_start = (start + need) % w.cap;
            assert_eq!(next_start % 4, 0);
        }
    }

    #[test]
    fn reader_prefault_does_not_modify_memory() {
        #[cfg(target_os = "linux")]
        unsafe {
            let len = 8192;
            let mut v = vec![0xCCu8; len];
            let ptr = v.as_mut_ptr();
            super::prefault_and_mlock(ptr, len);
            assert_eq!(v[0], 0xCC);
            assert_eq!(v[4096], 0xCC);
        }
    }

    // ========== basic helper tests ==========

    #[test]
    fn align_up_behaves_as_expected() {
        assert_eq!(align_up(0, 8), 0);
        assert_eq!(align_up(1, 8), 8);
        assert_eq!(align_up(7, 8), 8);
        assert_eq!(align_up(8, 8), 8);
        assert_eq!(align_up(9, 8), 16);

        assert_eq!(align_up(15, 16), 16);
        assert_eq!(align_up(16, 16), 16);
        assert_eq!(align_up(17, 16), 32);
    }

    #[test]
    fn posix_shm_name_validation() {
        assert!(is_valid_posix_shm_name("/foo"));
        assert!(is_valid_posix_shm_name("/x"));
        assert!(!is_valid_posix_shm_name("foo")); // missing leading slash
        assert!(!is_valid_posix_shm_name("/foo/bar")); // extra slash
        assert!(!is_valid_posix_shm_name("")); // empty
    }

    #[test]
    fn monotonic_micros_is_monotonic() {
        let t1 = monotonic_micros();
        let t2 = monotonic_micros();
        assert!(t2 >= t1);
    }

    #[test]
    fn create_or_open_existing_size_mismatch_errors() {
        let name = "/t_size_mismatch";
        let _ = shm_unlink_if_exists(name);

        // Choose capacities so that the aligned total_len values are different.
        let total1 = align_up(size_of::<ShmHeader>() + 1024, CL.max(4096));
        let total2 = align_up(size_of::<ShmHeader>() + 8192, CL.max(4096));

        assert_ne!(
            total1, total2,
            "totals must differ for this test to be meaningful"
        );

        {
            let (_file, created) = create_or_open_shm_posix(name, total1, true).unwrap();
            assert!(created);
        }

        let err = create_or_open_shm_posix(name, total2, true).unwrap_err();
        match err {
            ShmError::Io(e) => assert_eq!(e.kind(), io::ErrorKind::InvalidInput),
            other => panic!("expected Io(InvalidInput), got {other:?}"),
        }

        let _ = shm_unlink_if_exists(name);
    }

    #[test]
    fn open_fails_on_invalid_header_magic() {
        let name = "/t_invalid_header_magic";
        let _ = shm_unlink_if_exists(name);

        let capacity = 4096usize;
        let total = align_up(size_of::<ShmHeader>() + capacity, CL.max(4096));
        let (file, _created) = create_or_open_shm_posix(name, total, true).unwrap();

        let mut mmap = map_mut(&file, total).unwrap();
        unsafe {
            let hdr_ptr = mmap.as_mut_ptr() as *mut ShmHeader;
            (*hdr_ptr).magic = MAGIC.wrapping_add(1); // corrupt magic
            (*hdr_ptr).version = VERSION;
            (*hdr_ptr).capacity = capacity as u64;
        }
        drop(mmap);
        drop(file);

        let err = ShmReader::open(name).unwrap_err();
        assert!(matches!(err, ShmError::InvalidHeader));
    }

    #[test]
    fn shm_unlink_removes_segment() {
        let name = "/t_shm_unlink";
        let _ = shm_unlink_if_exists(name);

        // create and drop once
        {
            let (_w, _r) = new_writer_reader(4096, name);
        }

        // unlink
        shm_unlink_if_exists(name).unwrap();

        // opening must now fail with a Nix error
        let err = ShmReader::open(name).unwrap_err();
        assert!(matches!(err, ShmError::Nix(_)));
    }

    // ========== basic writer/reader semantics ==========

    #[test]
    fn single_writer_single_reader_roundtrip() {
        let (w, r) = new_writer_reader(64 * 1024, "/t_roundtrip");
        let map: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
            DashMap::with_hasher(GxBuildHasher::default());

        let pk = [1u8; 32];
        let payload = b"hello world".to_vec();

        let (seq, _start) = w.push(&pk, &payload, 123, 0).unwrap();
        assert_eq!(seq, 0);

        let (out_pk, lamports, updated, is_end, _ts) =
            r.poll_next_into_dashmap(&map).expect("record");
        assert_eq!(out_pk, pk);
        assert_eq!(lamports, 123);
        assert!(updated);
        assert!(!is_end);

        let key: Pubkey = pk.into();
        let v = map.get(&key).unwrap();
        assert_eq!(v.as_slice(), payload.as_slice());

        // ring is empty now
        assert!(r.poll_next_into_dashmap(&map).is_none());
    }

    #[test]
    fn at_tip_reader_only_sees_new_records() {
        let (w, r) = new_writer_reader(4096, "/t_at_tip");
        let map: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
            DashMap::with_hasher(GxBuildHasher::default());

        // At creation, reader starts at tip; with no writes, it should see nothing.
        assert!(r.poll_next_into_dashmap(&map).is_none());

        let pk = [7u8; 32];
        // Use a payload length where the trailer only overwrites padding, not data.
        let payload = b"tip-tests".to_vec(); // len = 9, 9 % 8 = 1 → safe
        w.push(&pk, &payload, 99, 1).unwrap();

        let (got_pk, lamports, updated, is_end, _ts) =
            r.poll_next_into_dashmap(&map).expect("record after tip");
        assert_eq!(got_pk, pk);
        assert_eq!(lamports, 99);
        assert!(updated);
        assert!(is_end);

        let key: Pubkey = pk.into();
        let v = map.get(&key).unwrap();
        assert_eq!(v.as_slice(), payload.as_slice());

        // Ring is empty after consuming that single record
        assert!(r.poll_next_into_dashmap(&map).is_none());
    }

    #[test]
    fn seek_to_start_reads_from_beginning_for_new_reader() {
        let name = "/t_seek_to_start";
        let _ = shm_unlink_if_exists(name);
        let w = ShmWriter::create(name, 16 * 1024).unwrap();

        // write some records before opening the reader
        let base_pk = [0u8; 32];
        let num = 10u8;
        for i in 0..num {
            let mut pk = base_pk;
            pk[0] = i;
            let payload = vec![i; 4];
            w.push(&pk, &payload, i as u64, 0).unwrap();
        }

        let mut r = ShmReader::open(name).unwrap();
        let map: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
            DashMap::with_hasher(GxBuildHasher::default());

        // Reader starts at tip → sees no historical records.
        assert!(r.poll_next_into_dashmap(&map).is_none());

        // Move to logical start and read everything.
        r.seek_to_start();

        let mut seen = 0;
        while let Some((pk, lamports, updated, is_end, _)) = r.poll_next_into_dashmap(&map) {
            seen += 1;
            assert!(updated);
            assert!(!is_end);
            let key: Pubkey = pk.into();
            let v = map.get(&key).unwrap();
            assert_eq!(v.len(), 4);
            assert_eq!(v[0], lamports as u8);
        }
        assert_eq!(seen, num as usize);
    }

    #[test]
    fn updated_flag_reflects_payload_changes() {
        let (w, r) = new_writer_reader(4096, "/t_updated_flag");
        let map: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
            DashMap::with_hasher(GxBuildHasher::default());

        let pk = [5u8; 32];

        // 1) first write → updated = true
        let p1 = vec![1u8, 2, 3];
        w.push(&pk, &p1, 1, 0).unwrap();
        let (_, _, updated1, _, _) = r.poll_next_into_dashmap(&map).unwrap();
        assert!(updated1);

        // 2) write identical payload → updated = false
        let p1b = vec![1u8, 2, 3];
        w.push(&pk, &p1b, 2, 0).unwrap();
        let (_, _, updated2, _, _) = r.poll_next_into_dashmap(&map).unwrap();
        assert!(!updated2);

        // 3) write different payload (same len) → updated = true
        let p2 = vec![4u8, 5, 6];
        w.push(&pk, &p2, 3, 0).unwrap();
        let (_, _, updated3, _, _) = r.poll_next_into_dashmap(&map).unwrap();
        assert!(updated3);

        let key: Pubkey = pk.into();
        let v = map.get(&key).unwrap();
        assert_eq!(v.as_slice(), p2.as_slice());
    }

    // ========== concurrency / race-condition coverage WITH TIMEOUTS ==========

    #[test]
    fn multi_writer_single_reader_stress() {
        const NUM_WRITERS: usize = 8;
        const PER_WRITER: usize = 64;
        const TOTAL: usize = NUM_WRITERS * PER_WRITER;
        const TIMEOUT: Duration = Duration::from_secs(5);

        let (w, r) = new_writer_reader(128 * 1024, "/t_multiwriter_single_reader");
        let w = Arc::new(w);
        let map: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
            DashMap::with_hasher(GxBuildHasher::default());

        // Writers send a signal when done; we don't join them directly.
        let (tx, rx) = mpsc::channel();

        for t in 0..NUM_WRITERS {
            let w_cl = Arc::clone(&w);
            let tx_cl = tx.clone();
            thread::spawn(move || {
                for i in 0..PER_WRITER {
                    let mut pk = [0u8; 32];
                    let id: u64 = ((t as u64) << 32) | (i as u64);
                    pk[..8].copy_from_slice(&id.to_le_bytes());
                    let payload = format!("writer-{t}-record-{i}").into_bytes();
                    w_cl.push(&pk, &payload, i as u64, 0).unwrap();
                }
                let _ = tx_cl.send(());
            });
        }
        drop(tx); // close original sender

        // Main thread acts as reader with timeout.
        let start = Instant::now();
        while map.len() < TOTAL {
            while let Some((_pk, _lamports, _updated, _is_end, _ts)) =
                r.poll_next_into_dashmap(&map)
            {}
            if map.len() >= TOTAL {
                break;
            }
            if start.elapsed() > TIMEOUT {
                panic!(
                    "timeout waiting for reader to consume all records; got {} / {}",
                    map.len(),
                    TOTAL
                );
            }
            thread::yield_now();
        }

        // Make sure all writers signaled completion (or we time out).
        let start_w = Instant::now();
        let mut completed = 0;
        while completed < NUM_WRITERS {
            let remaining = TIMEOUT
                .checked_sub(start_w.elapsed())
                .unwrap_or_else(|| Duration::from_millis(1));
            match rx.recv_timeout(remaining) {
                Ok(()) => completed += 1,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    panic!("timeout waiting for writer threads to finish");
                }
                Err(e) => {
                    panic!("channel error waiting for writer threads: {e:?}");
                }
            }
        }

        assert_eq!(map.len(), TOTAL);

        // Verify payloads
        for t in 0..NUM_WRITERS {
            for i in 0..PER_WRITER {
                let id: u64 = ((t as u64) << 32) | (i as u64);
                let mut pk = [0u8; 32];
                pk[..8].copy_from_slice(&id.to_le_bytes());
                let key: Pubkey = pk.into();
                let v = map.get(&key).unwrap();
                assert_eq!(v.as_slice(), format!("writer-{t}-record-{i}").as_bytes());
            }
        }
    }

    #[test]
    fn multi_reader_independent_cursors() {
        const N: usize = 16;
        const TIMEOUT: Duration = Duration::from_secs(5);

        let name = "/t_multireader_single_writer";
        let _ = shm_unlink_if_exists(name);

        let w = ShmWriter::create(name, 16 * 1024).unwrap();

        // write some records before opening any reader
        let base_pk = [0u8; 32];
        for i in 0..N {
            let mut pk = base_pk;
            pk[0] = i as u8;
            let payload = vec![i as u8; 8];
            w.push(&pk, &payload, i as u64, 0).unwrap();
        }

        // Open readers at tip; they shouldn't see historical data yet
        let r1 = ShmReader::open(name).unwrap();
        let r2 = ShmReader::open(name).unwrap();

        let map1: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
            DashMap::with_hasher(GxBuildHasher::default());
        let map2: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
            DashMap::with_hasher(GxBuildHasher::default());

        assert!(r1.poll_next_into_dashmap(&map1).is_none());
        assert!(r2.poll_next_into_dashmap(&map2).is_none());

        // Now write a fresh batch; both readers should see all of those.
        for i in 0..N {
            let mut pk = base_pk;
            pk[0] = (i + N) as u8;
            let payload = vec![(i + N) as u8; 8];
            w.push(&pk, &payload, (i + N) as u64, 0).unwrap();
        }

        let start = Instant::now();
        while map1.len() < N || map2.len() < N {
            while let Some((_pk, _lamports, _updated, _is_end, _ts)) =
                r1.poll_next_into_dashmap(&map1)
            {}
            while let Some((_pk, _lamports, _updated, _is_end, _ts)) =
                r2.poll_next_into_dashmap(&map2)
            {}

            if start.elapsed() > TIMEOUT {
                panic!(
                    "timeout waiting for both readers: map1={} map2={} (expected {})",
                    map1.len(),
                    map2.len(),
                    N
                );
            }
            thread::yield_now();
        }

        assert_eq!(map1.len(), N);
        assert_eq!(map2.len(), N);
    }

    #[test]
    fn reader_never_sees_decreasing_lamports_under_pressure() {
        const TIMEOUT: Duration = Duration::from_secs(5);

        let (w, r) = new_writer_reader(80 * 4096, "/t_no_rewind");
        let w = Arc::new(w);
        let r = Arc::new(r);
        let map: Arc<DashMap<Pubkey, Vec<u8>, GxBuildHasher>> =
            Arc::new(DashMap::with_hasher(GxBuildHasher::default()));

        let pk = [42u8; 32];
        let writer_done = Arc::new(AtomicBool::new(false));

        {
            let w_cl = Arc::clone(&w);
            let writer_done_cl = Arc::clone(&writer_done);
            thread::spawn(move || {
                let payload = [0xAAu8; 32];
                for i in 1u64..10_000 {
                    w_cl.push(&pk, &payload, i, 0).unwrap();
                }
                writer_done_cl.store(true, Ordering::Release);
            });
        }

        let mut last = 0u64;
        let start = Instant::now();

        loop {
            if start.elapsed() > TIMEOUT {
                panic!(
                    "timeout in monotonic lamports test; last seen lamports = {}",
                    last
                );
            }

            let mut progressed = false;
            while let Some((_pk, lamports, _updated, _is_end, _ts)) = r.poll_next_into_dashmap(&map)
            {
                assert!(
                    lamports > last,
                    "lamports not monotonic: {lamports} <= {last}"
                );
                last = lamports;
                progressed = true;
            }

            if !progressed && writer_done.load(Ordering::Acquire) {
                break;
            }

            thread::yield_now();
        }

        assert!(last > 0);
    }
    // ========== compatibility: wrapped payload support ==========

    #[test]
    fn reader_never_sees_per_writer_reordering_with_multiple_writers() {
        const TIMEOUT: Duration = Duration::from_secs(10);
        const NUM_WRITERS: usize = 16;
        const STEPS_PER_WRITER: u64 = 1_000;

        let (w, r) = new_writer_reader(10 * 4096, "/t_multi_writer_order");
        let w = Arc::new(w);
        let r = Arc::new(r);
        let map: Arc<DashMap<Pubkey, Vec<u8>, GxBuildHasher>> =
            Arc::new(DashMap::with_hasher(GxBuildHasher::default()));

        let pk = [42u8; 32];

        // Track when all writers are done.
        let done_writers = Arc::new(AtomicUsize::new(0));

        // Spawn NUM_WRITERS parallel writers.
        for writer_id in 0..NUM_WRITERS {
            let w_cl = Arc::clone(&w);
            let done_cl = Arc::clone(&done_writers);
            let pk = pk; // copy the array into the closure

            thread::spawn(move || {
                let payload = [0xAAu8; 32];

                // Writer k writes lamports = k + n * NUM_WRITERS for n in 0..=STEPS_PER_WRITER
                for step in 0..=STEPS_PER_WRITER {
                    let lamports = writer_id as u64 + step * NUM_WRITERS as u64;
                    w_cl.push(&pk, &payload, lamports, 0).unwrap();
                }

                done_cl.fetch_add(1, Ordering::AcqRel);
            });
        }

        // Per-writer last-seen lamports.
        let mut last_by_writer = vec![0u64; NUM_WRITERS];
        let mut seen_any_for_writer = vec![false; NUM_WRITERS];

        let start = Instant::now();

        loop {
            if start.elapsed() > TIMEOUT {
                panic!(
                    "timeout in multi-writer monotonic test; last_by_writer = {:?}",
                    last_by_writer
                );
            }

            let mut progressed = false;

            while let Some((_pk, lamports, _updated, _is_end, _ts)) = r.poll_next_into_dashmap(&map)
            {
                // Recover writer id from lamports pattern: lamports = k + n * NUM_WRITERS
                let writer_id = (lamports % NUM_WRITERS as u64) as usize;

                if seen_any_for_writer[writer_id] {
                    let last = last_by_writer[writer_id];
                    assert!(
                        lamports > last,
                        "writer {writer_id}: lamports not monotonic: {lamports} <= {last}"
                    );
                } else {
                    seen_any_for_writer[writer_id] = true;
                }

                last_by_writer[writer_id] = lamports;
                progressed = true;
            }

            if !progressed && done_writers.load(Ordering::Acquire) == NUM_WRITERS {
                break;
            }

            thread::yield_now();
        }

        // Sanity check that we at least saw something from some writer.
        assert!(
            seen_any_for_writer.iter().any(|&x| x),
            "reader did not observe any entries from any writer"
        );
    }

    #[test]
    fn reader_does_not_rewind_when_idle_after_drain() {
        const TIMEOUT: Duration = Duration::from_secs(5);
        const NUM_RECORDS: u64 = 5_000;

        let (w, r) = new_writer_reader(100 * 4096, "/t_no_rewind_idle");
        let w = Arc::new(w);
        let r = Arc::new(r);
        let map: Arc<DashMap<Pubkey, Vec<u8>, GxBuildHasher>> =
            Arc::new(DashMap::with_hasher(GxBuildHasher::default()));

        let pk = [7u8; 32];

        let writer_done = Arc::new(AtomicBool::new(false));
        {
            let w_cl = Arc::clone(&w);
            let writer_done_cl = Arc::clone(&writer_done);
            thread::spawn(move || {
                let payload = [0xBBu8; 32];
                for i in 0..NUM_RECORDS {
                    w_cl.push(&pk, &payload, i, 0).unwrap();
                }
                writer_done_cl.store(true, Ordering::Release);
            });
        }

        let start = Instant::now();

        let mut count = 0u64;
        let mut last_lamports = None;

        // Drain once
        loop {
            if start.elapsed() > TIMEOUT {
                panic!(
                    "timeout while draining; count = {}, last_lamports = {:?}",
                    count, last_lamports
                );
            }

            let mut progressed = false;
            while let Some((_pk, lamports, _updated, _is_end, _ts)) = r.poll_next_into_dashmap(&map)
            {
                if let Some(prev) = last_lamports {
                    assert!(
                        lamports > prev,
                        "lamports not monotonic: {lamports} <= {prev}"
                    );
                }
                last_lamports = Some(lamports);
                count += 1;
                progressed = true;
            }

            if !progressed && writer_done.load(Ordering::Acquire) {
                break;
            }

            thread::yield_now();
        }

        // We must have seen at least the final write.
        assert_eq!(last_lamports, Some(NUM_RECORDS - 1));

        // After drain, we must NOT see any more entries (no rewind / duplicates).
        let idle_start = Instant::now();
        while idle_start.elapsed() < Duration::from_millis(200) {
            if let Some((_pk, lamports, _updated, _is_end, _ts)) = r.poll_next_into_dashmap(&map) {
                panic!(
                    "reader rewound or produced duplicates after drain; saw lamports {} again",
                    lamports
                );
            }
            thread::yield_now();
        }
    }

    #[test]
    fn reader_sees_monotonic_lamports_across_wraparound() {
        const TIMEOUT: Duration = Duration::from_secs(10);
        const NUM_RECORDS: u64 = 50_000;

        // Smallish buffer to force wrap-around
        let (w, r) = new_writer_reader(80 * 1024, "/t_wrap_monotonic");
        let w = Arc::new(w);
        let r = Arc::new(r);
        let map: Arc<DashMap<Pubkey, Vec<u8>, GxBuildHasher>> =
            Arc::new(DashMap::with_hasher(GxBuildHasher::default()));

        let pk = [99u8; 32];
        let writer_done = Arc::new(AtomicBool::new(false));

        {
            let w_cl = Arc::clone(&w);
            let writer_done_cl = Arc::clone(&writer_done);
            thread::spawn(move || {
                let payload = [0xCCu8; 32];
                for i in 0..NUM_RECORDS {
                    w_cl.push(&pk, &payload, i, 0).unwrap();
                }
                writer_done_cl.store(true, Ordering::Release);
            });
        }

        let start = Instant::now();
        let mut last = None::<u64>;
        let mut count = 0u64;

        loop {
            if start.elapsed() > TIMEOUT {
                panic!(
                    "timeout in wraparound test; count = {}, last = {:?}",
                    count, last
                );
            }

            let mut progressed = false;
            while let Some((_pk, lamports, _updated, _is_end, _ts)) = r.poll_next_into_dashmap(&map)
            {
                if let Some(prev) = last {
                    assert!(
                        lamports > prev,
                        "lamports not monotonic: {lamports} <= {prev}"
                    );
                }
                last = Some(lamports);
                count += 1;
                progressed = true;
            }

            if !progressed && writer_done.load(Ordering::Acquire) {
                break;
            }

            thread::yield_now();
        }

        // We might not see ALL records (old ones can be overwritten),
        // but we must see the final lamports value written.
        assert!(count > 0, "reader did not observe any entries at all");
        assert_eq!(last, Some(NUM_RECORDS - 1));
    }

    #[test]
    fn reader_started_late_skips_old_entries_and_starts_at_tail() {
        const TIMEOUT: Duration = Duration::from_secs(5);
        const WARMUP_RECORDS: u64 = 5_000;
        const EXTRA_RECORDS: u64 = 5_000;

        let name = "/t_late_reader";
        let _ = shm_unlink_if_exists(name);

        // Create writer on a fresh segment.
        let w = ShmWriter::create(name, 4096).unwrap();
        let w = Arc::new(w);
        let pk = [13u8; 32];

        let writer_ready_for_reader = Arc::new(AtomicBool::new(false));
        let writer_done = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(2));

        {
            let w_cl = Arc::clone(&w);
            let ready_cl = Arc::clone(&writer_ready_for_reader);
            let done_cl = Arc::clone(&writer_done);
            let barrier_cl = Arc::clone(&barrier);

            thread::spawn(move || {
                let payload = [0xDDu8; 32];

                // Phase 1: warmup before reader exists.
                for i in 0..WARMUP_RECORDS {
                    w_cl.push(&pk, &payload, i, 0).unwrap();
                }

                // Signal that warmup is done.
                ready_cl.store(true, Ordering::Release);

                // Wait until the main thread has created the late reader.
                barrier_cl.wait();

                // Phase 2: extra records that the late reader should see.
                for i in WARMUP_RECORDS..(WARMUP_RECORDS + EXTRA_RECORDS) {
                    w_cl.push(&pk, &payload, i, 0).unwrap();
                }

                done_cl.store(true, Ordering::Release);
            });
        }

        // Wait until warmup is done.
        while !writer_ready_for_reader.load(Ordering::Acquire) {
            thread::yield_now();
        }

        // Late reader: attach to the SAME shm segment, at tail.
        let r = ShmReader::open(name).unwrap();
        let r = Arc::new(r);
        let map: Arc<DashMap<Pubkey, Vec<u8>, GxBuildHasher>> =
            Arc::new(DashMap::with_hasher(GxBuildHasher::default()));

        // Synchronize so writer only starts EXTRA phase once reader is ready.
        barrier.wait();

        let start = Instant::now();
        let mut count = 0u64;

        loop {
            if start.elapsed() > TIMEOUT {
                panic!("timeout in late reader test; count = {}", count);
            }

            let mut progressed = false;
            while let Some((_pk, lamports, _updated, _is_end, _ts)) = r.poll_next_into_dashmap(&map)
            {
                // Late reader must never see warmup entries.
                assert!(
                    lamports >= WARMUP_RECORDS,
                    "late reader saw old entry: lamports = {lamports} < WARMUP_RECORDS={WARMUP_RECORDS}"
                );

                count += 1;
                progressed = true;
            }

            if !progressed && writer_done.load(Ordering::Acquire) {
                break;
            }

            thread::yield_now();
        }

        // We might not see ALL EXTRA_RECORDS (if overwritten), but we must see something.
        assert!(
            count > 0,
            "late reader did not observe any post-warmup entries"
        );
    }

    #[test]
    fn reader_does_not_rescan_ring_on_idle_after_drain() {
        const TIMEOUT: Duration = Duration::from_secs(5);
        const NUM_RECORDS: u64 = 5_000;

        let (w, r) = new_writer_reader(80 * 4096, "/t_idle_no_rescan");
        let w = Arc::new(w);
        let r = Arc::new(r);
        let map: Arc<DashMap<Pubkey, Vec<u8>, GxBuildHasher>> =
            Arc::new(DashMap::with_hasher(GxBuildHasher::default()));

        let pk = [123u8; 32];
        let writer_done = Arc::new(AtomicBool::new(false));

        {
            let w_cl = Arc::clone(&w);
            let done_cl = Arc::clone(&writer_done);
            thread::spawn(move || {
                let payload = [0xEEu8; 32];
                for i in 0..NUM_RECORDS {
                    w_cl.push(&pk, &payload, i, 0).unwrap();
                }
                done_cl.store(true, Ordering::Release);
            });
        }

        let start = Instant::now();
        let mut last = None::<u64>;

        // Drain everything once.
        loop {
            if start.elapsed() > TIMEOUT {
                panic!(
                    "timeout while draining in idle-no-rescan test; last = {:?}",
                    last
                );
            }

            let mut progressed = false;
            while let Some((_pk, lamports, _updated, _is_end, _ts)) = r.poll_next_into_dashmap(&map)
            {
                if let Some(prev) = last {
                    assert!(
                        lamports > prev,
                        "lamports not monotonic: {lamports} <= {prev}"
                    );
                }
                last = Some(lamports);
                progressed = true;
            }

            if !progressed && writer_done.load(Ordering::Acquire) {
                break;
            }

            thread::yield_now();
        }

        assert_eq!(last, Some(NUM_RECORDS - 1));

        // Snapshot the cursor after full drain.
        let off_after_drain = r.r_off.load(Ordering::Relaxed);

        // Now repeatedly poll while writer is idle; cursor must not move and we must not
        // accidentally "rewind" and rescan the ring.
        for _ in 0..1000 {
            assert!(r.poll_next_into_dashmap(&map).is_none());
            let cur_off = r.r_off.load(Ordering::Relaxed);
            assert_eq!(
                cur_off, off_after_drain,
                "reader cursor moved during idle: {} -> {}",
                off_after_drain, cur_off
            );
        }
    }

    #[test]
    fn reader_hits_corrupt_header_path_twice_in_a_row() {
        // We want a tiny ring so we can reason about offsets easily.
        let (w, r) = new_writer_reader(512, "/t_two_corrupt_headers");
        let r = Arc::new(r);
        let map: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
            DashMap::with_hasher(GxBuildHasher::default());

        let buf_ptr = w.buf;
        let cap = w.cap;
        let hdr_sz = RecHeader::size();

        // We want the same "next" offset that the error path will compute:
        // next = (r + align8(need.max(hdr_sz))) % cap
        //
        // We'll choose need to be smaller than hdr_sz, so need.max(hdr_sz) == hdr_sz
        // and align8(hdr_sz) is exactly what we'll use for the second header.
        fn align8(x: usize) -> usize {
            (x + 7) & !7
        }
        let second_off = align8(hdr_sz);
        assert!(second_off + 4 <= cap); // enough room for another u32 header

        unsafe {
            // Header 1 at offset 0: total_len = 13 (invalid)
            //
            // This satisfies:
            //   need < hdr_sz (since hdr_sz > 13)
            //   (need & 7) != 0 (13 is not 8-aligned)
            //
            // So the "corrupt/partial record" path must be taken.
            *(buf_ptr as *mut u32) = 13;

            // Header 2 at offset "second_off": same invalid length value.
            *(buf_ptr.add(second_off) as *mut u32) = 13;
        }

        // Put the reader exactly at the first bad header.
        r.r_off.store(0, Ordering::Relaxed);

        // First poll:
        //  - reads total_len = 13 at offset 0,
        //  - hits "corrupt header" path (need < hdr_sz, misaligned),
        //  - stores r_off = second_off,
        //  - returns None.
        assert!(r.poll_next_into_dashmap(&map).is_none());

        // Second poll:
        //  - now at offset second_off,
        //  - again reads total_len = 13,
        //  - again hits the same corrupt-header path.
        //
        // Previously, hitting this branch multiple times in a row sometimes
        // led to a segfault in production; this test forces that situation.
        assert!(r.poll_next_into_dashmap(&map).is_none());

        // If there is a segfault due to this logic, the test process will crash.
        // If not, we at least know we can hit the error-handling path twice
        // in a row without UB.
    }

    // ========== Additional Comprehensive Tests ==========

    #[test]
    fn writer_handles_exact_capacity_boundary() {
        let cap = 1024;
        let (w, r) = new_writer_reader(cap, "/t_exact_capacity");
        let map: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
            DashMap::with_hasher(GxBuildHasher::default());

        let pk = [1u8; 32];

        // Calculate payload size that exactly fills the buffer
        let hdr_sz = RecHeader::size();
        let payload_size = cap - hdr_sz - 8; // -8 for padding alignment
        let payload = vec![0xAB; payload_size];

        // This should succeed
        assert!(w.push(&pk, &payload, 1, 0).is_ok());

        // Verify we can read it back
        let result = r.poll_next_into_dashmap(&map);
        assert!(result.is_some());
    }

    #[test]
    fn writer_rejects_oversized_payload() {
        let cap = 1024;
        let (w, _r) = new_writer_reader(cap, "/t_oversized");

        let pk = [1u8; 32];
        let oversized_payload = vec![0xAB; cap + 100];

        // This should fail with NoSpace
        let result = w.push(&pk, &oversized_payload, 1, 0);
        assert!(matches!(result, Err(ShmError::NoSpace)));
    }

    #[test]
    fn reader_handles_rapid_wrapping() {
        const TIMEOUT: Duration = Duration::from_secs(10);
        // Small buffer to force frequent wrapping
        let (w, r) = new_writer_reader(50 * 512, "/t_rapid_wrap");
        let w = Arc::new(w);
        let r = Arc::new(r);
        let map: Arc<DashMap<Pubkey, Vec<u8>, GxBuildHasher>> =
            Arc::new(DashMap::with_hasher(GxBuildHasher::default()));

        let writer_done = Arc::new(AtomicBool::new(false));
        let pk = [99u8; 32];

        {
            let w_cl = Arc::clone(&w);
            let done_cl = Arc::clone(&writer_done);
            thread::spawn(move || {
                // Write many small records to force wrapping
                for i in 0..100_000u64 {
                    let payload = vec![(i & 0xFF) as u8; 8];
                    w_cl.push(&pk, &payload, i, 0).unwrap();
                }
                done_cl.store(true, Ordering::Release);
            });
        }

        let start = Instant::now();
        let mut last_lamports = None;
        let mut count = 0;

        loop {
            if start.elapsed() > TIMEOUT {
                panic!("timeout in rapid wrapping test");
            }

            let mut progressed = false;
            while let Some((_pk, lamports, _updated, _is_end, _ts)) = r.poll_next_into_dashmap(&map)
            {
                if let Some(prev) = last_lamports {
                    assert!(lamports > prev, "non-monotonic lamports");
                }
                last_lamports = Some(lamports);
                count += 1;
                progressed = true;
            }

            if !progressed && writer_done.load(Ordering::Acquire) {
                break;
            }

            thread::yield_now();
        }

        assert!(count > 0);
    }

    #[test]
    fn multiple_readers_at_different_speeds() {
        const TIMEOUT: Duration = Duration::from_secs(10);
        const NUM_RECORDS: usize = 1000;

        let name = "/t_multi_speed_readers";
        let _ = shm_unlink_if_exists(name);

        let w = ShmWriter::create(name, 16 * 1024).unwrap();
        let w = Arc::new(w);
        let pk = [77u8; 32];

        // Write all records first
        for i in 0..NUM_RECORDS {
            let payload = vec![(i & 0xFF) as u8; 16];
            w.push(&pk, &payload, i as u64, 0).unwrap();
        }

        // Open 3 readers: one from start, two from tip
        let mut r1 = ShmReader::open(name).unwrap();
        let r2 = ShmReader::open(name).unwrap();
        let r3 = ShmReader::open(name).unwrap();

        r1.seek_to_start(); // Read from beginning

        let map1: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
            DashMap::with_hasher(GxBuildHasher::default());
        let map2: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
            DashMap::with_hasher(GxBuildHasher::default());
        let map3: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
            DashMap::with_hasher(GxBuildHasher::default());

        // r1 should see all historical records
        let mut count1 = 0;
        while let Some(_) = r1.poll_next_into_dashmap(&map1) {
            count1 += 1;
        }
        assert!(count1 > 0, "r1 should see historical records");

        // r2 and r3 should see no historical records (started at tip)
        assert!(r2.poll_next_into_dashmap(&map2).is_none());
        assert!(r3.poll_next_into_dashmap(&map3).is_none());

        // Write new records; r2 and r3 should see them (using different pubkey to force update)
        for i in NUM_RECORDS..(NUM_RECORDS + 100) {
            let mut pk_new = pk;
            pk_new[1] = ((i - NUM_RECORDS) & 0xFF) as u8; // Different pubkey for each record
            let payload = vec![(i & 0xFF) as u8; 16];
            w.push(&pk_new, &payload, i as u64, 0).unwrap();
        }

        let start = Instant::now();
        let mut count2 = 0;
        let mut count3 = 0;

        while (count2 < 100 || count3 < 100) && start.elapsed() < TIMEOUT {
            while let Some(_) = r1.poll_next_into_dashmap(&map1) {}
            while let Some(_) = r2.poll_next_into_dashmap(&map2) {
                count2 += 1;
            }
            while let Some(_) = r3.poll_next_into_dashmap(&map3) {
                count3 += 1;
            }
            thread::yield_now();
        }

        assert!(count2 >= 100, "r2 should see new records, got {}", count2);
        assert!(count3 >= 100, "r3 should see new records, got {}", count3);
    }

    #[test]
    fn writer_handles_various_alignment_sizes() {
        let (w, r) = new_writer_reader(8192, "/t_alignment");
        let map: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
            DashMap::with_hasher(GxBuildHasher::default());

        let pk = [55u8; 32];

        // Test various payload sizes to exercise padding logic
        for size in [
            0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129,
        ] {
            let payload = vec![(size & 0xFF) as u8; size];
            w.push(&pk, &payload, size as u64, 0).unwrap();
        }

        // Read all back and verify sizes
        let mut count = 0;
        while let Some((_pk, lamports, _updated, _is_end, _ts)) = r.poll_next_into_dashmap(&map) {
            let key: Pubkey = pk.into();
            let v = map.get(&key).unwrap();
            assert_eq!(v.len(), lamports as usize);
            count += 1;
        }

        assert_eq!(count, 17);
    }

    #[test]
    fn concurrent_writers_with_slow_reader() {
        const TIMEOUT: Duration = Duration::from_secs(15);
        const NUM_WRITERS: usize = 8;
        const PER_WRITER: usize = 500;

        let (w, r) = new_writer_reader(8192, "/t_slow_reader");
        let w = Arc::new(w);
        let r = Arc::new(r);
        let map: Arc<DashMap<Pubkey, Vec<u8>, GxBuildHasher>> =
            Arc::new(DashMap::with_hasher(GxBuildHasher::default()));

        let done_count = Arc::new(AtomicUsize::new(0));

        // Start multiple writers
        for writer_id in 0..NUM_WRITERS {
            let w_cl = Arc::clone(&w);
            let done_cl = Arc::clone(&done_count);

            thread::spawn(move || {
                for i in 0..PER_WRITER {
                    let mut pk = [0u8; 32];
                    pk[0] = writer_id as u8;
                    pk[1] = (i & 0xFF) as u8;
                    let payload = vec![writer_id as u8; 32];
                    w_cl.push(&pk, &payload, i as u64, 0).unwrap();
                }
                done_cl.fetch_add(1, Ordering::Release);
            });
        }

        // Slow reader: add delays between polls
        let start = Instant::now();
        let mut total_read = 0;

        loop {
            if start.elapsed() > TIMEOUT {
                panic!("timeout with slow reader; read {} entries", total_read);
            }

            let mut batch_count = 0;
            while let Some(_) = r.poll_next_into_dashmap(&map) {
                batch_count += 1;
                total_read += 1;
            }

            if batch_count == 0 && done_count.load(Ordering::Acquire) == NUM_WRITERS {
                break;
            }

            // Simulate slow reader
            thread::sleep(Duration::from_micros(100));
        }

        assert!(total_read > 0);
    }

    #[test]
    fn zero_length_payload() {
        let (w, r) = new_writer_reader(1024, "/t_zero_len");
        let map: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
            DashMap::with_hasher(GxBuildHasher::default());

        let pk = [22u8; 32];
        let empty_payload: Vec<u8> = vec![];

        w.push(&pk, &empty_payload, 42, 0).unwrap();

        let (out_pk, lamports, _updated, _is_end, _ts) =
            r.poll_next_into_dashmap(&map).expect("zero-length record");

        assert_eq!(out_pk, pk);
        assert_eq!(lamports, 42);

        let key: Pubkey = pk.into();
        let v = map.get(&key).unwrap();
        assert_eq!(v.len(), 0);
    }

    #[test]
    fn reader_can_replay_history_with_seek_to_start() {
        // Test that seek_to_start allows replaying historical records
        const TIMEOUT: Duration = Duration::from_secs(5);
        let name = "/t_reader_replay";
        let _ = shm_unlink_if_exists(name);

        let w = ShmWriter::create(name, 4096).unwrap();
        let pk = [11u8; 32];

        // Write some records before opening reader
        for i in 0..10u64 {
            w.push(&pk, &vec![1u8; 8], i, 0).unwrap();
        }

        // Open reader AFTER writes - it starts at tip (end of existing data)
        let mut r = ShmReader::open(name).unwrap();
        let map: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
            DashMap::with_hasher(GxBuildHasher::default());

        // Reader at tip should not see historical records
        let mut historical_count = 0;
        for _ in 0..5 {
            if r.poll_next_into_dashmap(&map).is_some() {
                historical_count += 1;
            }
        }
        // It's OK if we see 0 or a few records due to timing, but not all 10
        assert!(
            historical_count < 10,
            "reader at tip should not see all historical records"
        );

        // Seek to start and read all
        r.seek_to_start();
        let mut count = 0;
        while let Some(_) = r.poll_next_into_dashmap(&map) {
            count += 1;
        }
        assert_eq!(
            count, 10,
            "should read all 10 historical records after seek_to_start"
        );

        // Seek to start again should allow replaying the same records
        r.seek_to_start();
        count = 0;
        while let Some(_) = r.poll_next_into_dashmap(&map) {
            count += 1;
        }
        assert_eq!(count, 10, "should replay all 10 records after second seek");

        // Write new records with different pubkeys to ensure they're seen
        for i in 10..20u64 {
            let mut pk_new = pk;
            pk_new[0] = i as u8;
            w.push(&pk_new, &vec![2u8; 8], i, 0).unwrap();
        }

        // Should see new records
        count = 0;
        let start = Instant::now();
        while count < 10 && start.elapsed() < TIMEOUT {
            if let Some(_) = r.poll_next_into_dashmap(&map) {
                count += 1;
            }
            thread::yield_now();
        }

        assert_eq!(count, 10, "should see all 10 new records");
    }

    #[test]
    fn stress_test_single_writer_single_reader_sustained_load() {
        const TIMEOUT: Duration = Duration::from_secs(20);
        const NUM_RECORDS: u64 = 100_000;

        let (w, r) = new_writer_reader(64 * 1024, "/t_sustained_load");
        let w = Arc::new(w);
        let r = Arc::new(r);
        let map: Arc<DashMap<Pubkey, Vec<u8>, GxBuildHasher>> =
            Arc::new(DashMap::with_hasher(GxBuildHasher::default()));

        let writer_done = Arc::new(AtomicBool::new(false));
        let pk = [33u8; 32];

        {
            let w_cl = Arc::clone(&w);
            let done_cl = Arc::clone(&writer_done);
            thread::spawn(move || {
                for i in 0..NUM_RECORDS {
                    let payload = vec![(i & 0xFF) as u8; 64];
                    w_cl.push(&pk, &payload, i, 0).unwrap();
                }
                done_cl.store(true, Ordering::Release);
            });
        }

        let start = Instant::now();
        let mut last_lamports = None;

        loop {
            if start.elapsed() > TIMEOUT {
                panic!(
                    "timeout in sustained load test at lamports {:?}",
                    last_lamports
                );
            }

            let mut progressed = false;
            while let Some((_pk, lamports, _updated, _is_end, _ts)) = r.poll_next_into_dashmap(&map)
            {
                if let Some(prev) = last_lamports {
                    assert!(lamports > prev);
                }
                last_lamports = Some(lamports);
                progressed = true;
            }

            if !progressed && writer_done.load(Ordering::Acquire) {
                break;
            }

            thread::yield_now();
        }

        assert_eq!(last_lamports, Some(NUM_RECORDS - 1));
    }
}

#[cfg(test)]
mod benches {
    use super::*;
    use dashmap::DashMap;
    use gxhash::GxBuildHasher;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    #[ignore] // Run with: cargo test --release bench_ -- --ignored --nocapture
    fn bench_single_writer_single_reader_latency() {
        const WARMUP: usize = 10_000;
        const SAMPLES: usize = 100_000;

        let name = "/bench_latency";
        let _ = shm_unlink_if_exists(name);

        let w = ShmWriter::create(name, 1024 * 1024).unwrap();
        let w = Arc::new(w);
        let r = ShmReader::open(name).unwrap();
        let r = Arc::new(r);
        let map: Arc<DashMap<Pubkey, Vec<u8>, GxBuildHasher>> =
            Arc::new(DashMap::with_hasher(GxBuildHasher::default()));

        let pk = [1u8; 32];
        let payload = vec![0xAB; 64];

        let latencies = Arc::new(DashMap::<u64, u64, GxBuildHasher>::with_hasher(
            GxBuildHasher::default(),
        ));
        let latencies_clone = Arc::clone(&latencies);

        let writer_done = Arc::new(AtomicBool::new(false));
        let writer_done_clone = Arc::clone(&writer_done);

        // Reader thread
        let reader_handle = thread::spawn(move || {
            loop {
                while let Some((_pk, seq, _updated, _is_end, receive_ts)) =
                    r.poll_next_into_dashmap(&map)
                {
                    latencies_clone.insert(seq, receive_ts);
                }

                if writer_done_clone.load(Ordering::Relaxed) {
                    // Final drain
                    while let Some((_pk, seq, _updated, _is_end, receive_ts)) =
                        r.poll_next_into_dashmap(&map)
                    {
                        latencies_clone.insert(seq, receive_ts);
                    }
                    break;
                }

                std::hint::spin_loop();
            }
        });

        // Warmup
        for i in 0..WARMUP {
            w.push(&pk, &payload, i as u64, 0).unwrap();
        }

        thread::sleep(Duration::from_millis(100));

        // Benchmark: record send timestamps
        let mut send_times = Vec::with_capacity(SAMPLES);
        for i in WARMUP..(WARMUP + SAMPLES) {
            let send_ts = monotonic_micros();
            w.push(&pk, &payload, i as u64, 0).unwrap();
            send_times.push((i as u64, send_ts));
        }

        writer_done.store(true, Ordering::Release);
        reader_handle.join().unwrap();

        // Calculate latencies
        let mut latency_samples = Vec::new();
        for (seq, send_ts) in send_times {
            if let Some(entry) = latencies.get(&seq) {
                let receive_ts = *entry;
                if receive_ts >= send_ts {
                    latency_samples.push(receive_ts - send_ts);
                }
            }
        }

        latency_samples.sort();

        if latency_samples.is_empty() {
            println!("WARNING: No latency samples collected!");
            return;
        }

        let len = latency_samples.len();
        let min = latency_samples[0];
        let max = latency_samples[len - 1];
        let median = latency_samples[len / 2];
        let p95 = latency_samples[(len as f64 * 0.95) as usize];
        let p99 = latency_samples[(len as f64 * 0.99) as usize];
        let p999 = latency_samples[(len as f64 * 0.999) as usize];
        let avg: u64 = latency_samples.iter().sum::<u64>() / len as u64;

        println!("\n========== Single Writer/Reader Latency (microseconds) ==========");
        println!("Samples collected: {}/{}", len, SAMPLES);
        println!("Min:    {:7.2} μs", min as f64);
        println!("Avg:    {:7.2} μs", avg as f64);
        println!("Median: {:7.2} μs", median as f64);
        println!("P95:    {:7.2} μs", p95 as f64);
        println!("P99:    {:7.2} μs", p99 as f64);
        println!("P99.9:  {:7.2} μs", p999 as f64);
        println!("Max:    {:7.2} μs", max as f64);
        println!("==================================================================\n");
    }

    #[test]
    #[ignore]
    fn bench_throughput_single_writer() {
        const DURATION_SECS: u64 = 10;
        const PAYLOAD_SIZE: usize = 64;

        let name = "/bench_throughput_sw";
        let _ = shm_unlink_if_exists(name);

        let w = ShmWriter::create(name, 4 * 1024 * 1024).unwrap();
        let pk = [2u8; 32];
        let payload = vec![0xCD; PAYLOAD_SIZE];

        let start = Instant::now();
        let mut count = 0u64;

        while start.elapsed() < Duration::from_secs(DURATION_SECS) {
            w.push(&pk, &payload, count, 0).unwrap();
            count += 1;
        }

        let elapsed = start.elapsed().as_secs_f64();
        let throughput = count as f64 / elapsed;
        let bandwidth_mbps =
            (throughput * (PAYLOAD_SIZE + RecHeader::size()) as f64) / (1024.0 * 1024.0);

        println!("\n========== Single Writer Throughput ==========");
        println!("Duration:   {:.2} seconds", elapsed);
        println!("Messages:   {}", count);
        println!("Throughput: {:.2} msgs/sec", throughput);
        println!("Bandwidth:  {:.2} MB/s", bandwidth_mbps);
        println!("==============================================\n");
    }

    #[test]
    #[ignore]
    fn bench_throughput_multi_writer() {
        const DURATION_SECS: u64 = 10;
        const NUM_WRITERS: usize = 4;
        const PAYLOAD_SIZE: usize = 64;

        let name = "/bench_throughput_mw";
        let _ = shm_unlink_if_exists(name);

        let w = ShmWriter::create(name, 8 * 1024 * 1024).unwrap();
        let w = Arc::new(w);

        let total_count = Arc::new(AtomicU64::new(0));
        let start_barrier = Arc::new(std::sync::Barrier::new(NUM_WRITERS + 1));

        let handles: Vec<_> = (0..NUM_WRITERS)
            .map(|writer_id| {
                let w_clone = Arc::clone(&w);
                let count_clone = Arc::clone(&total_count);
                let barrier_clone = Arc::clone(&start_barrier);

                thread::spawn(move || {
                    let mut pk = [0u8; 32];
                    pk[0] = writer_id as u8;
                    let payload = vec![0xEF; PAYLOAD_SIZE];

                    barrier_clone.wait(); // Synchronize start

                    let start = Instant::now();
                    let mut local_count = 0u64;

                    while start.elapsed() < Duration::from_secs(DURATION_SECS) {
                        w_clone.push(&pk, &payload, local_count, 0).unwrap();
                        local_count += 1;
                    }

                    count_clone.fetch_add(local_count, Ordering::Relaxed);
                })
            })
            .collect();

        let start = Instant::now();
        start_barrier.wait(); // Start all writers

        for handle in handles {
            handle.join().unwrap();
        }

        let elapsed = start.elapsed().as_secs_f64();
        let count = total_count.load(Ordering::Relaxed);
        let throughput = count as f64 / elapsed;
        let bandwidth_mbps =
            (throughput * (PAYLOAD_SIZE + RecHeader::size()) as f64) / (1024.0 * 1024.0);

        println!(
            "\n========== Multi Writer ({} threads) Throughput ==========",
            NUM_WRITERS
        );
        println!("Duration:   {:.2} seconds", elapsed);
        println!("Messages:   {}", count);
        println!("Throughput: {:.2} msgs/sec", throughput);
        println!(
            "Per-thread: {:.2} msgs/sec",
            throughput / NUM_WRITERS as f64
        );
        println!("Bandwidth:  {:.2} MB/s", bandwidth_mbps);
        println!("=========================================================\n");
    }

    #[test]
    #[ignore]
    fn bench_round_trip_latency_distribution() {
        const SAMPLES: usize = 1_000_000;
        const PAYLOAD_SIZE: usize = 64;

        let name = "/bench_rt_latency";
        let _ = shm_unlink_if_exists(name);

        let w = ShmWriter::create(name, 16 * 1024 * 1024).unwrap();
        let w = Arc::new(w);
        let r = ShmReader::open(name).unwrap();
        let r = Arc::new(r);
        let map: Arc<DashMap<Pubkey, Vec<u8>, GxBuildHasher>> =
            Arc::new(DashMap::with_hasher(GxBuildHasher::default()));

        let pk = [3u8; 32];
        let payload = vec![0x12; PAYLOAD_SIZE];

        let latencies = Arc::new(DashMap::<u64, u64, GxBuildHasher>::with_hasher(
            GxBuildHasher::default(),
        ));
        let latencies_clone = Arc::clone(&latencies);

        let writer_done = Arc::new(AtomicBool::new(false));
        let writer_done_clone = Arc::clone(&writer_done);

        // Dedicated reader thread
        let reader_handle = thread::spawn(move || {
            loop {
                while let Some((_pk, seq, _updated, _is_end, receive_ts)) =
                    r.poll_next_into_dashmap(&map)
                {
                    latencies_clone.insert(seq, receive_ts);
                }

                if writer_done_clone.load(Ordering::Acquire) {
                    // Final drain
                    for _ in 0..10 {
                        while let Some((_pk, seq, _updated, _is_end, receive_ts)) =
                            r.poll_next_into_dashmap(&map)
                        {
                            latencies_clone.insert(seq, receive_ts);
                        }
                        thread::sleep(Duration::from_millis(10));
                    }
                    break;
                }

                std::hint::spin_loop();
            }
        });

        // Warmup
        for i in 0..10_000 {
            w.push(&pk, &payload, i, 0).unwrap();
        }

        thread::sleep(Duration::from_millis(200));

        println!("Starting benchmark run...");

        let mut send_times = Vec::with_capacity(SAMPLES);
        let bench_start = Instant::now();

        for i in 0..SAMPLES {
            let send_ts = monotonic_micros();
            w.push(&pk, &payload, i as u64, 0).unwrap();
            send_times.push((i as u64, send_ts));
        }

        println!("All messages sent, waiting for reader to finish...");

        writer_done.store(true, Ordering::Release);
        reader_handle.join().unwrap();

        let bench_elapsed = bench_start.elapsed();

        // Calculate latencies and track high-latency sequences
        let mut latency_samples = Vec::new();
        let mut high_latency_seqs = Vec::new();
        for (seq, send_ts) in send_times {
            if let Some(entry) = latencies.get(&seq) {
                let receive_ts = *entry;
                if receive_ts >= send_ts {
                    let lat = receive_ts - send_ts;
                    latency_samples.push(lat);
                    if lat > 20 {
                        high_latency_seqs.push((seq, lat));
                    }
                }
            }
        }

        latency_samples.sort();

        if latency_samples.is_empty() {
            println!("ERROR: No latency samples collected!");
            return;
        }

        let len = latency_samples.len();
        let min = latency_samples[0];
        let max = latency_samples[len - 1];
        let median = latency_samples[len / 2];
        let p50 = median;
        let p90 = latency_samples[(len as f64 * 0.90) as usize];
        let p95 = latency_samples[(len as f64 * 0.95) as usize];
        let p99 = latency_samples[(len as f64 * 0.99) as usize];
        let p999 = latency_samples[(len as f64 * 0.999) as usize];
        let p9999 = latency_samples[(len as f64 * 0.9999) as usize];
        let sum: u64 = latency_samples.iter().sum();
        let avg = sum as f64 / len as f64;

        // Calculate standard deviation
        let variance: f64 = latency_samples
            .iter()
            .map(|&x| {
                let diff = x as f64 - avg;
                diff * diff
            })
            .sum::<f64>()
            / len as f64;
        let stddev = variance.sqrt();

        println!("\n========== Round-Trip Latency Distribution ==========");
        println!(
            "Benchmark duration: {:.2} seconds",
            bench_elapsed.as_secs_f64()
        );
        println!("Total messages:     {}", SAMPLES);
        println!(
            "Samples collected:  {} ({:.2}%)",
            len,
            (len as f64 / SAMPLES as f64) * 100.0
        );
        println!("Payload size:       {} bytes", PAYLOAD_SIZE);
        println!("\nLatency (microseconds):");
        println!("  Min:       {:8.2} μs", min as f64);
        println!("  Avg:       {:8.2} μs", avg);
        println!("  Median:    {:8.2} μs", median as f64);
        println!("  StdDev:    {:8.2} μs", stddev);
        println!("  P50:       {:8.2} μs", p50 as f64);
        println!("  P90:       {:8.2} μs", p90 as f64);
        println!("  P95:       {:8.2} μs", p95 as f64);
        println!("  P99:       {:8.2} μs", p99 as f64);
        println!("  P99.9:     {:8.2} μs", p999 as f64);
        println!("  P99.99:    {:8.2} μs", p9999 as f64);
        println!("  Max:       {:8.2} μs", max as f64);
        println!(
            "\nThroughput: {:.2} msgs/sec",
            SAMPLES as f64 / bench_elapsed.as_secs_f64()
        );
        println!("======================================================\n");

        // Histogram
        println!("Latency Histogram:");
        let buckets = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000];
        let mut prev = 0;
        for &bucket in &buckets {
            let count = latency_samples
                .iter()
                .filter(|&&x| x > prev && x <= bucket)
                .count();
            let pct = (count as f64 / len as f64) * 100.0;
            println!("  {:5} - {:5} μs: {:6} ({:5.2}%)", prev, bucket, count, pct);
            prev = bucket;
        }
        let count = latency_samples.iter().filter(|&&x| x > prev).count();
        let pct = (count as f64 / len as f64) * 100.0;
        println!("  {:5} +      μs: {:6} ({:5.2}%)", prev, count, pct);
    }

    #[test]
    #[ignore]
    fn bench_payload_size_impact() {
        const SAMPLES_PER_SIZE: usize = 50_000;
        let payload_sizes = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096];

        println!("\n========== Payload Size Impact on Latency ==========");
        println!(
            "{:>10} | {:>10} | {:>10} | {:>10} | {:>10}",
            "Size (B)", "Avg (μs)", "P50 (μs)", "P95 (μs)", "P99 (μs)"
        );
        println!("{:-<60}", "");

        for &size in &payload_sizes {
            let name = format!("/bench_payload_{}", size);
            let _ = shm_unlink_if_exists(&name);

            let w = ShmWriter::create(&name, 32 * 1024 * 1024).unwrap();
            let w = Arc::new(w);
            let r = ShmReader::open(&name).unwrap();
            let r = Arc::new(r);
            let map: Arc<DashMap<Pubkey, Vec<u8>, GxBuildHasher>> =
                Arc::new(DashMap::with_hasher(GxBuildHasher::default()));

            let pk = [4u8; 32];
            let payload = vec![0x55; size];

            let latencies = Arc::new(DashMap::<u64, u64, GxBuildHasher>::with_hasher(
                GxBuildHasher::default(),
            ));
            let latencies_clone = Arc::clone(&latencies);

            let writer_done = Arc::new(AtomicBool::new(false));
            let writer_done_clone = Arc::clone(&writer_done);

            let reader_handle = thread::spawn(move || {
                loop {
                    while let Some((_pk, seq, _updated, _is_end, receive_ts)) =
                        r.poll_next_into_dashmap(&map)
                    {
                        latencies_clone.insert(seq, receive_ts);
                    }

                    if writer_done_clone.load(Ordering::Acquire) {
                        for _ in 0..5 {
                            while let Some((_pk, seq, _updated, _is_end, receive_ts)) =
                                r.poll_next_into_dashmap(&map)
                            {
                                latencies_clone.insert(seq, receive_ts);
                            }
                            thread::sleep(Duration::from_millis(5));
                        }
                        break;
                    }

                    std::hint::spin_loop();
                }
            });

            // Warmup
            for i in 0..5_000 {
                w.push(&pk, &payload, i, 0).unwrap();
            }

            thread::sleep(Duration::from_millis(100));

            let mut send_times = Vec::with_capacity(SAMPLES_PER_SIZE);
            for i in 0..SAMPLES_PER_SIZE {
                let send_ts = monotonic_micros();
                w.push(&pk, &payload, i as u64, 0).unwrap();
                send_times.push((i as u64, send_ts));
            }

            writer_done.store(true, Ordering::Release);
            reader_handle.join().unwrap();

            let mut latency_samples = Vec::new();
            for (seq, send_ts) in send_times {
                if let Some(entry) = latencies.get(&seq) {
                    let receive_ts = *entry;
                    if receive_ts >= send_ts {
                        latency_samples.push(receive_ts - send_ts);
                    }
                }
            }

            latency_samples.sort();

            if !latency_samples.is_empty() {
                let len = latency_samples.len();
                let avg: u64 = latency_samples.iter().sum::<u64>() / len as u64;
                let p50 = latency_samples[len / 2];
                let p95 = latency_samples[(len as f64 * 0.95) as usize];
                let p99 = latency_samples[(len as f64 * 0.99) as usize];

                println!(
                    "{:10} | {:10.2} | {:10.2} | {:10.2} | {:10.2}",
                    size, avg as f64, p50 as f64, p95 as f64, p99 as f64
                );
            }

            let _ = shm_unlink_if_exists(&name);
        }

        println!("====================================================\n");
    }

    #[test]
    #[ignore]
    fn bench_balanced_writer_reader_latency() {
        // Benchmark where writer is paced to not outpace reader
        // This measures true steady-state latency without queue backlog
        const SAMPLES: usize = 100_000;
        const PAYLOAD_SIZE: usize = 10000;
        const WRITE_DELAY_NS: u64 = 1000; // Small delay between writes to pace

        let name = &format!("/bench_balanced_latency_{}", std::process::id());
        let _ = shm_unlink_if_exists(name);

        let w = ShmWriter::create(name, 512 * 1024 * 1024).unwrap();
        let w = Arc::new(w);
        let r = ShmReader::open(name).unwrap();
        let r = Arc::new(r);
        let map: Arc<DashMap<Pubkey, Vec<u8>, GxBuildHasher>> =
            Arc::new(DashMap::with_hasher(GxBuildHasher::default()));

        let pk = [5u8; 32];
        let payload = vec![0x77; PAYLOAD_SIZE];

        let latencies = Arc::new(DashMap::<u64, u64, GxBuildHasher>::with_hasher(
            GxBuildHasher::default(),
        ));
        let latencies_clone = Arc::clone(&latencies);

        // Tracking reschedule events
        let reschedule_events = Arc::new(
            DashMap::<usize, (u64, u64, i32, i32), GxBuildHasher>::with_hasher(
                GxBuildHasher::default(),
            ),
        );
        let reschedule_events_clone = Arc::clone(&reschedule_events);

        let writer_done = Arc::new(AtomicBool::new(false));
        let writer_done_clone = Arc::clone(&writer_done);

        // Dedicated reader thread (spinning for low latency)
        let reader_handle = thread::spawn(move || {
            // EXPERIMENT: Try realtime priority WITHOUT pinning
            // Hypothesis: Pinning to any single core causes issues due to IRQs
            // Let scheduler choose best core dynamically with realtime guarantee

            println!("Reader: Skipping CPU pinning (let scheduler choose with realtime priority)");

            // Try to set SCHED_FIFO with moderate realtime priority
            // NOTE: Max priority (99) is too high and can interfere with kernel threads
            // Use moderate priority (50) to avoid priority inversion and system conflicts
            unsafe {
                let tid = libc::syscall(libc::SYS_gettid) as libc::pid_t;

                // Use moderate priority: high enough to avoid preemption, low enough to not conflict
                // Can be overridden via READER_RT_PRIORITY env var
                let target_priority = std::env::var("READER_RT_PRIORITY")
                    .ok()
                    .and_then(|s| s.parse::<i32>().ok())
                    .unwrap_or(50); // Default: 50 (middle ground, range is typically 1-99)

                let param = libc::sched_param {
                    sched_priority: target_priority,
                };

                let result = libc::sched_setscheduler(tid, libc::SCHED_FIFO, &param);
                if result == 0 {
                    println!("Reader: SCHED_FIFO priority {}: SUCCESS", target_priority);
                } else {
                    println!(
                        "Reader: SCHED_FIFO: FAILED (errno: {}, likely need CAP_SYS_NICE or sudo)",
                        *libc::__errno_location()
                    );

                    // Fallback: Try increasing nice value (works without root)
                    let result = libc::setpriority(libc::PRIO_PROCESS, tid as u32, -20);
                    if result == 0 {
                        println!("Reader: Nice priority -20: SUCCESS");
                    } else {
                        println!("Reader: Nice priority -20: FAILED");
                    }
                }
            }

            // Helper to read current CPU from /proc
            let get_current_cpu = || -> i32 {
                unsafe {
                    let mut cpu: libc::c_uint = 0;
                    let mut node: libc::c_uint = 0;
                    if libc::syscall(
                        libc::SYS_getcpu,
                        &mut cpu,
                        &mut node,
                        std::ptr::null_mut::<libc::c_void>(),
                    ) == 0
                    {
                        cpu as i32
                    } else {
                        -1
                    }
                }
            };

            let mut last_loop_time = monotonic_micros();
            let mut prev_cpu = get_current_cpu();
            let mut reschedule_count = 0;
            let mut cpu_migration_count = 0;

            loop {
                let loop_start = monotonic_micros();
                let mut processed_any = false;

                while let Some((_pk, seq, _updated, _is_end, receive_ts)) =
                    r.poll_next_into_dashmap(&map)
                {
                    latencies_clone.insert(seq, receive_ts);
                    processed_any = true;
                }

                let loop_end = monotonic_micros();
                let gap = loop_start.saturating_sub(last_loop_time);

                // Check current CPU
                let current_cpu = get_current_cpu();
                let cpu_changed = current_cpu != prev_cpu && prev_cpu != -1;

                // Detect potential reschedule: gap > 10us when we didn't process data
                // (If we processed data, the gap is expected due to processing time)
                if !processed_any && gap > 10 {
                    reschedule_events_clone.insert(
                        reschedule_count,
                        (last_loop_time, gap, prev_cpu, current_cpu),
                    );
                    reschedule_count += 1;
                } else if cpu_changed {
                    // CPU migration without significant time gap
                    cpu_migration_count += 1;
                }

                prev_cpu = current_cpu;
                last_loop_time = loop_end;

                if writer_done_clone.load(Ordering::Relaxed) {
                    break;
                }
            }

            println!("\nReader thread stats:");
            println!(
                "  Total reschedule events (>10us gaps): {}",
                reschedule_count
            );
            println!("  CPU migrations: {}", cpu_migration_count);
        });

        // Also boost writer (main thread) priority to avoid priority inversion
        // Use slightly lower priority than reader to ensure reader gets preference
        unsafe {
            let tid = libc::syscall(libc::SYS_gettid) as libc::pid_t;
            let reader_priority = std::env::var("READER_RT_PRIORITY")
                .ok()
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(50);
            let writer_priority = std::cmp::max(1, reader_priority - 5); // 5 levels lower, min 1

            let param = libc::sched_param {
                sched_priority: writer_priority,
            };

            let result = libc::sched_setscheduler(tid, libc::SCHED_FIFO, &param);
            if result == 0 {
                println!("Writer: SCHED_FIFO priority {}: SUCCESS", writer_priority);
            } else {
                println!("Writer: SCHED_FIFO: FAILED (will use default priority)");
            }
        }

        // Warmup
        for i in 0..1_000 {
            w.push(&pk, &payload, i, 0).unwrap();
            thread::sleep(Duration::from_nanos(WRITE_DELAY_NS));
        }

        thread::sleep(Duration::from_millis(100));

        println!("Starting balanced latency benchmark...");
        println!("Write delay: {} ns", WRITE_DELAY_NS);

        let mut send_times = Vec::with_capacity(SAMPLES);
        let bench_start = Instant::now();

        for i in 0..SAMPLES {
            let send_ts = monotonic_micros();
            w.push(&pk, &payload, i as u64, 0).unwrap();
            send_times.push((i as u64, send_ts));

            // Small delay to pace the writer
            if WRITE_DELAY_NS > 0 {
                thread::sleep(Duration::from_nanos(WRITE_DELAY_NS));
            }
        }

        writer_done.store(true, Ordering::Release);
        reader_handle.join().unwrap();

        let bench_elapsed = bench_start.elapsed();

        // Calculate latencies and track high-latency sequences
        let mut latency_samples = Vec::new();
        let mut high_latency_seqs = Vec::new();
        for (seq, send_ts) in send_times {
            if let Some(entry) = latencies.get(&seq) {
                let receive_ts = *entry;
                if receive_ts >= send_ts {
                    let lat = receive_ts - send_ts;
                    latency_samples.push(lat);
                    if lat > 20 {
                        high_latency_seqs.push((seq, lat));
                    }
                }
            }
        }

        latency_samples.sort();

        if latency_samples.is_empty() {
            println!("ERROR: No latency samples collected!");
            return;
        }

        let len = latency_samples.len();
        let min = latency_samples[0];
        let max = latency_samples[len - 1];
        let median = latency_samples[len / 2];
        let p50 = median;
        let p90 = latency_samples[(len as f64 * 0.90) as usize];
        let p95 = latency_samples[(len as f64 * 0.95) as usize];
        let p99 = latency_samples[(len as f64 * 0.99) as usize];
        let p999 = latency_samples[(len as f64 * 0.999) as usize];
        let p9999 = latency_samples[(len as f64 * 0.9999) as usize];
        let sum: u64 = latency_samples.iter().sum();
        let avg = sum as f64 / len as f64;

        // Calculate standard deviation
        let variance: f64 = latency_samples
            .iter()
            .map(|&x| {
                let diff = x as f64 - avg;
                diff * diff
            })
            .sum::<f64>()
            / len as f64;
        let stddev = variance.sqrt();

        println!("\n========== Balanced Writer/Reader Latency ==========");
        println!(
            "Benchmark duration: {:.2} seconds",
            bench_elapsed.as_secs_f64()
        );
        println!("Total messages:     {}", SAMPLES);
        println!(
            "Samples collected:  {} ({:.2}%)",
            len,
            (len as f64 / SAMPLES as f64) * 100.0
        );
        println!("Payload size:       {} bytes", PAYLOAD_SIZE);
        println!(
            "Write pacing:       {} ns delay between writes",
            WRITE_DELAY_NS
        );
        println!("\nLatency (microseconds):");
        println!("  Min:       {:8.2} μs", min as f64);
        println!("  Avg:       {:8.2} μs", avg);
        println!("  Median:    {:8.2} μs", median as f64);
        println!("  StdDev:    {:8.2} μs", stddev);
        println!("  P50:       {:8.2} μs", p50 as f64);
        println!("  P90:       {:8.2} μs", p90 as f64);
        println!("  P95:       {:8.2} μs", p95 as f64);
        println!("  P99:       {:8.2} μs", p99 as f64);
        println!("  P99.9:     {:8.2} μs", p999 as f64);
        println!("  P99.99:    {:8.2} μs", p9999 as f64);
        println!("  Max:       {:8.2} μs", max as f64);
        println!(
            "\nEffective throughput: {:.2} msgs/sec",
            SAMPLES as f64 / bench_elapsed.as_secs_f64()
        );
        println!("====================================================\n");

        // Histogram
        println!("Latency Histogram:");
        let buckets = [1, 2, 3, 4, 5, 10, 20, 50, 100, 200, 500, 1000];
        let mut prev = 0;
        for &bucket in &buckets {
            let count = latency_samples
                .iter()
                .filter(|&&x| x >= prev && x < bucket)
                .count();
            let pct = (count as f64 / len as f64) * 100.0;
            println!("  {:5} - {:5} μs: {:6} ({:5.2}%)", prev, bucket, count, pct);
            prev = bucket;
        }
        let count = latency_samples.iter().filter(|&&x| x > prev).count();
        let pct = (count as f64 / len as f64) * 100.0;
        println!("  {:5} +      μs: {:6} ({:5.2}%)", prev, count, pct);

        // Calculate percentage under various thresholds
        println!("\nLatency Thresholds:");
        for &threshold in &[1, 2, 5, 10, 20, 50, 100] {
            let under_threshold = latency_samples.iter().filter(|&&x| x <= threshold).count();
            let pct = (under_threshold as f64 / len as f64) * 100.0;
            println!("  <= {:3} μs: {:5.2}% of samples", threshold, pct);
        }

        // Clustering analysis
        println!("\n========== High Latency Clustering ==========");
        println!(
            "Messages >20μs: {} ({:.1}%)",
            high_latency_seqs.len(),
            high_latency_seqs.len() as f64 / SAMPLES as f64 * 100.0
        );

        if high_latency_seqs.len() > 0 {
            high_latency_seqs.sort_by_key(|(seq, _)| *seq);
            let mut clusters = 0;
            let mut cluster_sizes = Vec::new();
            let mut in_cluster = 1;

            for i in 1..high_latency_seqs.len() {
                if high_latency_seqs[i].0 == high_latency_seqs[i - 1].0 + 1 {
                    in_cluster += 1;
                } else {
                    cluster_sizes.push(in_cluster);
                    in_cluster = 1;
                }
            }
            cluster_sizes.push(in_cluster);
            clusters = cluster_sizes.len();

            println!("Clusters: {}", clusters);
            println!(
                "Avg cluster size: {:.1}",
                high_latency_seqs.len() as f64 / clusters as f64
            );
            println!("Max cluster size: {}", cluster_sizes.iter().max().unwrap());

            // Show some examples
            println!("\nSample sequences:");
            for (i, (seq, lat)) in high_latency_seqs.iter().take(10).enumerate() {
                print!("  seq {}: {}μs", seq, lat);
                if i > 0 && seq - 1 == high_latency_seqs[i - 1].0 {
                    print!(" [consecutive]");
                }
                println!();
            }
        }
        println!("=============================================");

        // Analyze reschedule events
        println!("\n========== OS Reschedule Analysis ==========");
        if reschedule_events.is_empty() {
            println!("No reschedule events detected (no gaps >10μs)");
        } else {
            // Collect all events
            let mut events: Vec<(u64, u64, i32, i32)> = reschedule_events
                .iter()
                .map(|entry| *entry.value())
                .collect();
            events.sort_by_key(|(timestamp, _, _, _)| *timestamp);

            println!("Total reschedule events: {}", events.len());

            // Analyze delay distribution
            let mut delays: Vec<u64> = events.iter().map(|(_, delay, _, _)| *delay).collect();
            delays.sort();

            if !delays.is_empty() {
                let delay_min = delays[0];
                let delay_max = delays[delays.len() - 1];
                let delay_median = delays[delays.len() / 2];
                let delay_p90 = delays[(delays.len() as f64 * 0.90) as usize];
                let delay_p99 = delays[(delays.len() as f64 * 0.99) as usize];
                let delay_sum: u64 = delays.iter().sum();
                let delay_avg = delay_sum as f64 / delays.len() as f64;

                println!("\nReschedule delay statistics (μs):");
                println!("  Min:    {:8.2} μs", delay_min as f64);
                println!("  Avg:    {:8.2} μs", delay_avg);
                println!("  Median: {:8.2} μs", delay_median as f64);
                println!("  P90:    {:8.2} μs", delay_p90 as f64);
                println!("  P99:    {:8.2} μs", delay_p99 as f64);
                println!("  Max:    {:8.2} μs", delay_max as f64);

                // Delay histogram
                println!("\nReschedule delay histogram:");
                let delay_buckets = [10, 20, 50, 100, 200, 500, 1000, 2000, 5000];
                let mut prev_bucket = 0;
                for &bucket in &delay_buckets {
                    let count = delays
                        .iter()
                        .filter(|&&d| d >= prev_bucket && d < bucket)
                        .count();
                    let pct = (count as f64 / delays.len() as f64) * 100.0;
                    println!(
                        "  {:5} - {:5} μs: {:6} ({:5.2}%)",
                        prev_bucket, bucket, count, pct
                    );
                    prev_bucket = bucket;
                }
                let count = delays.iter().filter(|&&d| d >= prev_bucket).count();
                let pct = (count as f64 / delays.len() as f64) * 100.0;
                println!("  {:5} +      μs: {:6} ({:5.2}%)", prev_bucket, count, pct);

                // CPU migrations
                let cpu_migrations = events
                    .iter()
                    .filter(|(_, _, prev, curr)| prev != curr && *prev != -1)
                    .count();
                println!(
                    "\nCPU migrations during reschedules: {} ({:.1}%)",
                    cpu_migrations,
                    cpu_migrations as f64 / events.len() as f64 * 100.0
                );

                // Show first 10 reschedule events
                println!("\nFirst 10 reschedule events:");
                for (i, (timestamp, delay, prev_cpu, curr_cpu)) in
                    events.iter().take(10).enumerate()
                {
                    print!("  [{:2}] @{}μs: delay={}μs", i, timestamp, delay);
                    if prev_cpu != curr_cpu && *prev_cpu != -1 {
                        print!(" CPU{}→{}", prev_cpu, curr_cpu);
                    } else {
                        print!(" CPU{}", curr_cpu);
                    }
                    println!();
                }

                // Show largest reschedule events
                let mut sorted_by_delay = events.clone();
                sorted_by_delay.sort_by_key(|(_, delay, _, _)| std::cmp::Reverse(*delay));
                println!("\nTop 10 longest reschedule delays:");
                for (i, (timestamp, delay, prev_cpu, curr_cpu)) in
                    sorted_by_delay.iter().take(10).enumerate()
                {
                    print!("  [{:2}] @{}μs: delay={}μs", i, timestamp, delay);
                    if prev_cpu != curr_cpu && *prev_cpu != -1 {
                        print!(" CPU{}→{}", prev_cpu, curr_cpu);
                    } else {
                        print!(" CPU{}", curr_cpu);
                    }
                    println!();
                }
            }
        }
        println!("============================================");
    }

    #[test]
    fn test_last_end_off_never_goes_backward_with_multiple_writers() {
        // This test verifies that last_end_off uses atomic max and never decreases
        // even when multiple writers complete out of order.
        const NUM_WRITERS: usize = 8;
        const WRITES_PER_THREAD: usize = 1000;
        const CAP: usize = 64 * 1024;
        let name = "/test_last_end_off_monotonic";

        let w = ShmWriter::create(name, CAP).unwrap();
        let w = Arc::new(w);
        let r = ShmReader::open(name).unwrap();

        let barrier = Arc::new(std::sync::Barrier::new(NUM_WRITERS + 1));
        let handles: Vec<_> = (0..NUM_WRITERS)
            .map(|thread_id| {
                let w_clone = Arc::clone(&w);
                let barrier_clone = barrier.clone();
                std::thread::spawn(move || {
                    let pubkey = [thread_id as u8; 32];
                    let data = vec![thread_id as u8; 50];

                    barrier_clone.wait(); // Synchronize start

                    for i in 0..WRITES_PER_THREAD {
                        let lamports = (thread_id * WRITES_PER_THREAD + i) as u64;
                        let _ = w_clone.push(&pubkey, &data, lamports, 0);

                        // Add some randomness to writer completion order
                        if i % 10 == 0 {
                            std::thread::yield_now();
                        }
                    }
                })
            })
            .collect();

        // Monitor thread: watch last_end_off to ensure it never decreases
        let hdr = unsafe { &*r.hdr };
        let monitor_done = Arc::new(AtomicBool::new(false));
        let monitor_done_clone = monitor_done.clone();

        let monitor_handle = std::thread::spawn(move || {
            let mut last_tail = 0u64;
            let mut backwards_count = 0;
            let mut samples = 0;

            while !monitor_done_clone.load(Ordering::Relaxed) {
                let current_tail = hdr.last_end_off.load(Ordering::Acquire);

                // Allow wrapping: consider it "backwards" only if substantially backwards
                // (not just wrapping around the ring)
                if current_tail != 0
                    && current_tail < last_tail
                    && (last_tail - current_tail) < CAP as u64 / 2
                {
                    backwards_count += 1;
                    eprintln!(
                        "WARNING: last_end_off went backwards: {} -> {}",
                        last_tail, current_tail
                    );
                }

                last_tail = current_tail;
                samples += 1;
                std::thread::yield_now();
            }

            (backwards_count, samples)
        });

        barrier.wait(); // Start all writers

        for h in handles {
            h.join().unwrap();
        }

        // Let monitor thread observe final state
        std::thread::sleep(std::time::Duration::from_millis(50));
        monitor_done.store(true, Ordering::Relaxed);

        let (backwards_count, samples) = monitor_handle.join().unwrap();

        println!(
            "Monotonic last_end_off test: {} samples, {} backwards movements",
            samples, backwards_count
        );

        // With the atomic max fix, tail should never go backwards
        assert_eq!(
            backwards_count, 0,
            "last_end_off should never decrease with atomic max"
        );
    }

    #[test]
    fn test_reader_not_stuck_on_stale_seq_at_tail() {
        // This test reproduces the scenario where reader gets stuck in the
        // "seq <= last_seq" branch with "r != cur_tail" due to stale cur_tail snapshot.
        const CAP: usize = 8192;
        let name = "/test_stuck_reader";

        let w = ShmWriter::create(name, CAP).unwrap();
        let r = ShmReader::open(name).unwrap();

        let pubkey = [42u8; 32];
        let data = vec![0xCC; 200];

        // Write enough to fill the ring and wrap around
        let records_to_wrap = (CAP / 256) + 10;

        for i in 0..records_to_wrap {
            w.push(&pubkey, &data, i as u64, 0).unwrap();
        }

        // Reader drains everything
        let map: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
            DashMap::with_hasher(GxBuildHasher::default());
        let mut count = 0;
        for _ in 0..records_to_wrap * 2 {
            if r.poll_next_into_dashmap(&map).is_some() {
                count += 1;
            }
        }

        println!("Reader consumed {} records before wrapping", count);

        // Now write a few more to overwrite old positions
        for i in 0..5 {
            w.push(&pubkey, &data, (records_to_wrap + i) as u64, 0)
                .unwrap();
        }

        // Reader should be able to read these new records without getting stuck
        let mut new_reads = 0;
        let start = std::time::Instant::now();

        while start.elapsed() < std::time::Duration::from_millis(100) {
            if r.poll_next_into_dashmap(&map).is_some() {
                new_reads += 1;
            }

            if new_reads >= 5 {
                break;
            }
        }

        println!("Reader consumed {} new records after wrap", new_reads);

        // With the fix (reloading cur_tail), reader should not get stuck
        assert!(
            new_reads >= 3,
            "Reader should not get stuck and should read most new records"
        );
    }

    // #[test]
    // fn test_concurrent_read_write_no_bad_records() {
    //     // Stress test: ensure reader never sees "bad" records (corrupted data)
    //     // with concurrent writers hammering the ring buffer.
    //     const CAP: usize = 4 * 16 * 1024;
    //     const NUM_WRITERS: usize = 4;
    //     const DURATION_MS: u64 = 1000;
    //     let name = "/test_no_bad_records";
    //
    //     let w = ShmWriter::create(name, CAP).unwrap();
    //     let w = Arc::new(w);
    //     let r = ShmReader::open(name).unwrap();
    //
    //     let done = Arc::new(AtomicBool::new(false));
    //     let total_writes = Arc::new(AtomicU64::new(0));
    //
    //     let handles: Vec<_> = (0..NUM_WRITERS)
    //         .map(|thread_id| {
    //             let w_clone = Arc::clone(&w);
    //             let done_clone = done.clone();
    //             let total_writes_clone = total_writes.clone();
    //
    //             std::thread::spawn(move || {
    //                 let pubkey = [thread_id as u8; 32];
    //                 let mut count = 0u64;
    //
    //                 while !done_clone.load(Ordering::Relaxed) {
    //                     // Create data that's verifiable (all bytes = thread_id)
    //                     let data = vec![thread_id as u8; 128];
    //                     let lamports = (thread_id as u64) << 32 | count;
    //
    //                     let _ = w_clone.push(&pubkey, &data, lamports, 0);
    //                     count += 1;
    //
    //                     std::thread::yield_now();
    //                     thread::sleep(std::time::Duration::from_micros(1));
    //                 }
    //
    //                 total_writes_clone.fetch_add(count, Ordering::Relaxed);
    //             })
    //         })
    //         .collect();
    //
    //     // Reader: verify all records have consistent data
    //     let map: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
    //         DashMap::with_hasher(GxBuildHasher::default());
    //     let mut valid_reads = 0u64;
    //     let mut data_corruption_errors = 0;
    //     let start = std::time::Instant::now();
    //
    //     while start.elapsed() < std::time::Duration::from_millis(DURATION_MS) {
    //         if let Some((pubkey, lamports, _updated, _is_end, _ts)) = r.poll_next_into_dashmap(&map) {
    //             valid_reads += 1;
    //
    //             // Verify data integrity: all bytes in the vec should match the thread_id
    //             // which is encoded in pubkey[0] and the upper 32 bits of lamports
    //             let expected_byte = pubkey[0];
    //             let thread_id_from_lamports = (lamports >> 32) as u8;
    //
    //             if expected_byte != thread_id_from_lamports {
    //                 data_corruption_errors += 1;
    //                 eprintln!("Data corruption detected: pubkey[0]={}, lamports thread_id={}",
    //                          expected_byte, thread_id_from_lamports);
    //             }
    //
    //         }
    //
    //     }
    //
    //     done.store(true, Ordering::Relaxed);
    //
    //     for h in handles {
    //         h.join().unwrap();
    //     }
    //
    //     let total_writes_count = total_writes.load(Ordering::Relaxed);
    //
    //     println!("Concurrent read/write test: {} writes, {} valid reads, {} corruption errors",
    //              total_writes_count, valid_reads, data_corruption_errors);
    //
    //     // With TOCTOU protection, we should see zero data corruption
    //     assert_eq!(data_corruption_errors, 0, "Reader should never see corrupted data");
    //     assert!(valid_reads > 0, "Reader should see some valid reads");
    // }
}
