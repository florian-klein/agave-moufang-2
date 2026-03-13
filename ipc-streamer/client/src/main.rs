use clap::Parser;
use dashmap::DashMap;
use gxhash::GxBuildHasher;
use shm_accounts_shared::{ShmReader, TX_END_MARKER_PUBKEY};
use solana_pubkey::Pubkey;

/// Streams updates from the shared ring and logs every received entry.
#[derive(Parser, Debug)]
struct Args {
    /// Name under /dev/shm (e.g., "moufang_accounts" -> /dev/shm/moufang_accounts)
    #[arg(long, default_value = "moufang_accounts")]
    path: String,
}

#[inline(always)]
fn monotonic_micros() -> u64 {
    use libc::{CLOCK_MONOTONIC_RAW, clock_gettime, timespec};
    let mut ts = timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { clock_gettime(CLOCK_MONOTONIC_RAW, &mut ts) };
    (ts.tv_sec as u64) * 1_000_000 + (ts.tv_nsec as u64) / 1_000
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let r = ShmReader::open(&args.path)?;
    let map: DashMap<Pubkey, Vec<u8>, GxBuildHasher> =
        DashMap::with_hasher(GxBuildHasher::default());

    loop {
        let (pubkey, lamports, updated, is_account, receive_ts) =
            r.poll_next_into_dashmap(&map).expect("poll_next_into_dashmap returned None");

        let now = monotonic_micros();
        let delay = now.wrapping_sub(receive_ts);

        if pubkey == TX_END_MARKER_PUBKEY {
            println!(
                "TX_END_MARKER lamports={} delay_us={} receive_ts={}",
                lamports, delay, receive_ts,
            );
        } else {
            println!(
                "ACCOUNT key={} lamports={} updated={} is_account={} delay_us={} receive_ts={}",
                Pubkey::from(pubkey),
                lamports,
                updated,
                is_account,
                delay,
                receive_ts,
            );
        }
    }
}
