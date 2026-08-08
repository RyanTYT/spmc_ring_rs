//! Benchmark / correctness harness for `SpmcRingBuffer`.
//!
//! Adjust the `use` paths below to match your crate's actual module layout
//! (replace `crate::` below with wherever `SpmcRingBuffer`, `MeasureTool`, and your `SyncRingBuffer` reference impl actually live in rusty_trader).
//!
//! Default behaviour (no flags): runs only the lock-free `SpmcRingBuffer`
//! push/pop loop, with no correctness check and no MeasureTool
//! instrumentation, so the binary is clean to profile with `perf stat
//! ./ring_bench`.
//!
//!   --implementation      which ring buffer implementation to benchmark:
//!                         `lock-free` (SpmcRingBuffer, default) or `locked`
//!                         (SyncRingBuffer, mutex-based).
//!   --check              also run the *other* implementation over the same
//!                         input and verify every consumer received items in
//!                         the exact produced order.
//!   --measure             wrap every push/pop in MeasureTool::start_time /
//!                         end_time and render a latency histogram at the end.
//!   -c, --capacity        ring buffer capacity (power of two).
//!                         supported: 1024, 4096, 16384, 65536, 262144
//!   -n, --num-consumers   number of consumer threads. supported: 1, 2, 4, 8
//!   -i, --num-items       total items the producer pushes
//!   -s, --item-size       payload size in bytes per item
//!       --num-bars        histogram bucket count (only with --measure)

use clap::{Parser, ValueEnum};
use crossterm::{
    event, execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};
use std::io;
use std::thread;
use std::time::{Duration, Instant};

use spmc_ring::ring_buffer::spmc_ring_buffer::SpmcRingBuffer;
use spmc_ring::ring_buffer::sync_spmc_ring_buffer::SyncRingBuffer;

#[path = "./measure.rs"]
mod measure;
use measure::MeasureTool;
// use crate::{measure::MeasureTool, ring_buffer::sync_spmc_ring_buffer::SyncRingBuffer};
// use crate::sync_ring_buffer::SyncRingBuffer; // your reference impl — see note at bottom of file

/// Payload type pushed through the ring buffer. The first 8 bytes encode the
/// item's original index (little-endian u64) so we can verify ordering.
type Item = Vec<u8>;

/// Which ring buffer implementation to drive as the "primary" benchmarked
/// implementation. Both map onto the same underlying types used everywhere
/// else in this file: `LockFree` -> `SpmcRingBuffer`, `Locked` ->
/// `SyncRingBuffer`.
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
enum RingImpl {
    /// Lock-free SpmcRingBuffer. Default.
    LockFree,
    /// Mutex/lock-based SyncRingBuffer.
    Locked,
}

impl RingImpl {
    fn label(self) -> &'static str {
        match self {
            RingImpl::LockFree => "SpmcRingBuffer (lock-free)",
            RingImpl::Locked => "SyncRingBuffer (locked)",
        }
    }

    /// The other implementation, used as the `--check` reference when this
    /// one is the primary.
    fn other(self) -> RingImpl {
        match self {
            RingImpl::LockFree => RingImpl::Locked,
            RingImpl::Locked => RingImpl::LockFree,
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(
    name = "benchmark",
    about = "Benchmark / correctness harness for SpmcRingBuffer"
)]
struct Args {
    /// Ring buffer capacity (must be a power of two).
    /// Supported: 1024, 4096, 16384, 65536, 262144
    #[arg(long, short = 'c', default_value_t = 4096)]
    capacity: usize,

    /// Number of consumer threads. Supported: 1, 2, 4, 8
    #[arg(long, short = 'n', default_value_t = 1)]
    num_consumers: usize,

    /// Total number of items the producer will push
    #[arg(long, short = 'i', default_value_t = 1_000_000)]
    num_items: usize,

    /// Size in bytes of each item's payload (minimum 8, for the index)
    #[arg(long, short = 's', default_value_t = 64)]
    item_size: usize,

    /// Which ring buffer implementation to benchmark: the lock-free
    /// SpmcRingBuffer, or the locked (mutex-based) SyncRingBuffer.
    /// Defaults to the lock-free implementation.
    #[arg(long, short = 'I', value_enum, default_value = "lock-free")]
    implementation: RingImpl,

    /// Verify the selected implementation's output against the *other*
    /// implementation (used as a correctness oracle). Off by default.
    #[arg(long, default_value_t = false)]
    check: bool,

    /// Wrap push/pop calls with MeasureTool and display a latency histogram
    /// at the end. Off by default so the default run is clean for `perf stat`.
    #[arg(long, default_value_t = false)]
    measure: bool,

    /// Number of buckets in the latency histogram (only used with --measure)
    #[arg(long, default_value_t = 20)]
    num_bars: usize,
}

/// Result of running the producer/consumer loop against one ring buffer
/// implementation.
struct RunOutcome {
    producer_elapsed: Duration,
    consumer_elapsed: Vec<Duration>,
    /// Per-consumer sequence of item indices received, in order.
    /// Only populated when `--check` is set.
    consumer_sequences: Option<Vec<Vec<u64>>>,
}

fn make_items(num_items: usize, item_size: usize) -> Vec<Item> {
    let size = item_size.max(8);
    (0..num_items)
        .map(|i| {
            let mut buf = vec![0u8; size];
            buf[0..8].copy_from_slice(&(i as u64).to_le_bytes());
            buf
        })
        .collect()
}

fn item_index(item: &Item) -> u64 {
    u64::from_le_bytes(item[0..8].try_into().expect("item smaller than 8 bytes"))
}

/// Generates a `run_$name` function generic over the ring buffer's const
/// generics, parameterised over which ring buffer type it drives. Both
/// `SpmcRingBuffer` and `SyncRingBuffer` are assumed to expose an identical
/// `new()` / `get_new_producer()` / `get_new_consumer()` / `push()` / `pop()`
/// surface, so the body only needs to be written once.
macro_rules! define_runner {
    ($fn_name:ident, $ring_ty:ident) => {
        // inline(never) matters here: `dispatch!` below expands to ~20 calls
        // to different monomorphizations of this function gated behind a
        // runtime match. Without this, some optimization levels will inline
        // several/all of those monomorphizations into `main`'s single stack
        // frame (even though only one branch ever executes), and the frame
        // size scales with the largest CAPACITY compiled in — easily
        // overflowing the stack before a single line of `main` runs.
        #[inline(never)]
        fn $fn_name<const CAPACITY: usize, const NUM_CONSUMERS: usize>(
            args: &Args,
            items: &[Item],
            measure: Option<&MeasureTool>,
        ) -> RunOutcome {
            let ring = $ring_ty::<Item, CAPACITY, NUM_CONSUMERS>::new();
            let producer = ring
                .get_new_producer()
                .expect("failed to acquire producer slot");
            let consumers: Vec<_> = (0..args.num_consumers)
                .map(|_| {
                    ring.get_new_consumer()
                        .expect("failed to acquire consumer slot")
                })
                .collect();

            let num_items = args.num_items;
            let record_sequences = args.check;

            // Scoped threads let the producer/consumer handles (which hold
            // raw pointers into `ring`) borrow it directly, without needing
            // 'static bounds, Arc, or MeasureTool: Clone.
            thread::scope(|scope| {
                let producer_handle = scope.spawn(move || {
                    let start = Instant::now();
                    if let Some(tool) = measure {
                        for (i, item) in items.iter().cloned().enumerate() {
                            let name = format!("push-{i}");
                            // Name must be unique per call so concurrent
                            // start/end pairs from different threads never
                            // cross-match in MeasureTool's matching maps.
                            tool.start_time(&name);
                            producer.push(item);
                            tool.end_time(&name);
                        }
                    } else {
                        for (i, item) in items.iter().cloned().enumerate() {
                            producer.push(item);
                        }
                    }
                    start.elapsed()
                });

                let consumer_handles: Vec<_> = consumers
                    .into_iter()
                    .enumerate()
                    .map(|(cid, consumer)| {
                        scope.spawn(move || {
                            let mut sequence =
                                record_sequences.then(|| Vec::with_capacity(num_items));
                            let start = Instant::now();
                            if let Some(tool) = measure {
                                for i in 0..num_items {
                                    let name = format!("pop-{cid}-{i}");
                                    tool.start_time(&name);
                                    let item = consumer.pop();
                                    tool.end_time(&name);

                                    if let Some(seq) = sequence.as_mut() {
                                        seq.push(item_index(&item));
                                    }
                                }
                            } else {
                                for i in 0..num_items {
                                    let item = consumer.pop();
                                    if let Some(seq) = sequence.as_mut() {
                                        seq.push(item_index(&item));
                                    }
                                }
                            }
                            (start.elapsed(), sequence)
                        })
                    })
                    .collect();

                let producer_elapsed = producer_handle.join().expect("producer thread panicked");
                let mut consumer_elapsed = Vec::with_capacity(consumer_handles.len());
                let mut consumer_sequences = record_sequences.then(Vec::new);
                for handle in consumer_handles {
                    let (elapsed, seq) = handle.join().expect("consumer thread panicked");
                    consumer_elapsed.push(elapsed);
                    if let (Some(all), Some(one)) = (consumer_sequences.as_mut(), seq) {
                        all.push(one);
                    }
                }

                RunOutcome {
                    producer_elapsed,
                    consumer_elapsed,
                    consumer_sequences,
                }
            })
        }
    };
}

define_runner!(run_spmc, SpmcRingBuffer);
define_runner!(run_sync, SyncRingBuffer);

/// `CAPACITY` and `NUM_CONSUMERS` are compile-time const generics, so a
/// runtime `--capacity`/`--num-consumers` pair has to be mapped onto one of
/// a fixed set of monomorphizations. Extend the match arms (and the
/// supported lists in `Args`' doc comments) if you need other sizes.
macro_rules! dispatch {
    ($func:ident, $capacity:expr, $num_consumers:expr, $($arg:expr),+ $(,)?) => {
        match ($capacity, $num_consumers) {
            (1024, 1) => $func::<1024, 1>($($arg),+),
            (1024, 2) => $func::<1024, 2>($($arg),+),
            (1024, 4) => $func::<1024, 4>($($arg),+),
            (1024, 8) => $func::<1024, 8>($($arg),+),
            (1024, 16) => $func::<1024, 16>($($arg),+),
            (1024, 32) => $func::<1024, 32>($($arg),+),
            (1024, 64) => $func::<1024, 64>($($arg),+),
            (4096, 1) => $func::<4096, 1>($($arg),+),
            (4096, 2) => $func::<4096, 2>($($arg),+),
            (4096, 4) => $func::<4096, 4>($($arg),+),
            (4096, 8) => $func::<4096, 8>($($arg),+),
            (4096, 16) => $func::<1024, 16>($($arg),+),
            (4096, 32) => $func::<1024, 32>($($arg),+),
            (4096, 64) => $func::<1024, 64>($($arg),+),
            (16384, 1) => $func::<16384, 1>($($arg),+),
            (16384, 2) => $func::<16384, 2>($($arg),+),
            (16384, 4) => $func::<16384, 4>($($arg),+),
            (16384, 8) => $func::<16384, 8>($($arg),+),
            (16384, 16) => $func::<1024, 16>($($arg),+),
            (16384, 32) => $func::<1024, 32>($($arg),+),
            (16384, 64) => $func::<1024, 64>($($arg),+),
            (65536, 1) => $func::<65536, 1>($($arg),+),
            (65536, 2) => $func::<65536, 2>($($arg),+),
            (65536, 4) => $func::<65536, 4>($($arg),+),
            (65536, 8) => $func::<65536, 8>($($arg),+),
            (65536, 16) => $func::<1024, 16>($($arg),+),
            (65536, 32) => $func::<1024, 32>($($arg),+),
            (65536, 64) => $func::<1024, 64>($($arg),+),
            (262144, 1) => $func::<262144, 1>($($arg),+),
            (262144, 2) => $func::<262144, 2>($($arg),+),
            (262144, 4) => $func::<262144, 4>($($arg),+),
            (262144, 8) => $func::<262144, 8>($($arg),+),
            (262144, 16) => $func::<1024, 16>($($arg),+),
            (262144, 32) => $func::<1024, 32>($($arg),+),
            (262144, 64) => $func::<1024, 64>($($arg),+),
            (cap, cons) => {
                eprintln!(
                    "Unsupported --capacity/--num-consumers combination: {cap} / {cons}.\n\
                     Supported capacities: 1024, 4096, 16384, 65536, 262144\n\
                     Supported consumer counts: 1, 2, 4, 8, 16, 32, 64"
                );
                std::process::exit(1);
            }
        }
    };
}

/// Runs the given `RingImpl` (dispatching to `run_spmc` or `run_sync` under
/// the hood) for the current `--capacity`/`--num-consumers` combination.
fn run_impl(
    which: RingImpl,
    args: &Args,
    items: &[Item],
    measure: Option<&MeasureTool>,
) -> RunOutcome {
    match which {
        RingImpl::LockFree => dispatch!(run_spmc, args.capacity, args.num_consumers, args, items, measure),
        RingImpl::Locked => dispatch!(run_sync, args.capacity, args.num_consumers, args, items, measure),
    }
}

/// `SpmcRingBuffer::new()` / `SyncRingBuffer::new()` return `Self` by value,
/// so `let ring = ...::new();` places the whole `[UnsafeCell<Option<T>>; CAPACITY]`
/// buffer on the calling thread's stack (e.g. ~6 MB for CAPACITY = 262144,
/// item_size = 64). That comfortably blows a default 8 MB thread stack once
/// combined with everything else on the call chain. Running the dispatch on
/// a dedicated thread with a generous stack sidesteps this regardless of
/// how the optimizer happens to place things.
const DRIVER_STACK_SIZE: usize = 512 * 1024 * 1024;

fn run_with_big_stack<'scope, 'env, R: Send + 'scope>(
    scope: &'scope thread::Scope<'scope, 'env>,
    f: impl FnOnce() -> R + Send + 'scope,
) -> R {
    thread::Builder::new()
        .stack_size(DRIVER_STACK_SIZE)
        .spawn_scoped(scope, f)
        .expect("failed to spawn benchmark driver thread")
        .join()
        .expect("benchmark driver thread panicked")
}

fn print_summary(label: &str, outcome: &RunOutcome, num_items: usize) {
    println!("--- {label} ---");
    let producer_throughput = num_items as f64 / outcome.producer_elapsed.as_secs_f64();
    println!(
        "producer latency: {:?} ({producer_throughput:.0} items/sec)",
        outcome.producer_elapsed.div_f64(num_items as f64)
    );
    for (i, elapsed) in outcome.consumer_elapsed.iter().enumerate() {
        let throughput = num_items as f64 / elapsed.as_secs_f64();
        println!(
            "consumer latency[{i}]: {:?} ({throughput:.0} items/sec)",
            elapsed.div_f64(num_items as f64)
        );
    }
}

fn compare_outcomes(
    items: &[Item],
    primary_label: &str,
    primary: &RunOutcome,
    reference_label: &str,
    reference: &RunOutcome,
) {
    let expected: Vec<u64> = items.iter().map(item_index).collect();
    let primary_seqs = primary
        .consumer_sequences
        .as_ref()
        .expect("--check should record sequences");
    let reference_seqs = reference
        .consumer_sequences
        .as_ref()
        .expect("--check should record sequences");

    let mut all_ok = true;
    for (label, seqs) in [(primary_label, primary_seqs), (reference_label, reference_seqs)] {
        for (cid, seq) in seqs.iter().enumerate() {
            if seq != &expected {
                all_ok = false;
                let mismatch_at = seq
                    .iter()
                    .zip(expected.iter())
                    .position(|(a, b)| a != b)
                    .unwrap_or_else(|| seq.len().min(expected.len()));
                eprintln!(
                    "MISMATCH: {label} consumer[{cid}] diverges from expected order at index \
                     {mismatch_at} (got {:?}, expected {:?})",
                    seq.get(mismatch_at),
                    expected.get(mismatch_at)
                );
            }
        }
    }

    if all_ok {
        println!(
            "check OK: every consumer on both implementations received all items in the exact \
             produced order"
        );
    } else {
        std::process::exit(2);
    }
}

fn show_histogram(tool: &MeasureTool, num_bars: usize) {
    if let Err(e) = enable_raw_mode() {
        eprintln!("failed to enable raw mode, skipping histogram: {e:?}");
        return;
    }
    let mut stdout = io::stdout();
    if let Err(e) = execute!(stdout, EnterAlternateScreen) {
        eprintln!("failed to enter alternate screen: {e:?}");
        let _ = disable_raw_mode();
        return;
    }
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = match Terminal::new(backend) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("failed to create terminal: {e:?}");
            let _ = disable_raw_mode();
            return;
        }
    };

    let _ = terminal.draw(|frame| tool.plot_histogram(frame, num_bars));
    let _ = event::read(); // wait for a keypress, per the widget's own title

    let _ = disable_raw_mode();
    let _ = execute!(terminal.backend_mut(), LeaveAlternateScreen);
}

fn main() {
    let args = Args::parse();

    if !args.capacity.is_power_of_two() {
        eprintln!("--capacity must be a power of two (got {})", args.capacity);
        std::process::exit(1);
    }

    let items = make_items(args.num_items, args.item_size);

    // Only spin up MeasureTool's background thread and its per-call
    // channel sends when explicitly requested — otherwise the default run
    // is just the raw push/pop loop, suitable for `perf stat`.
    let measure_tool = args.measure.then(MeasureTool::new);

    // Bind references up front: the closures below are `move` closures (to
    // satisfy the `'scope` bound on `run_with_big_stack`), and a `move`
    // closure that uses `&args`/`&items` in its body captures the whole
    // owned value, not just a reference to it. Capturing these `&Args`/
    // `&[Item]` references instead means each closure only moves a
    // (Copy) reference, leaving `args`/`items`/`measure_tool` usable
    // afterwards.
    let args_ref = &args;
    let items_ref = &items;
    let measure_ref = measure_tool.as_ref();

    thread::scope(|scope| {
        let primary_impl = args.implementation;
        let primary_outcome = run_with_big_stack(scope, move || {
            run_impl(primary_impl, args_ref, items_ref, measure_ref)
        });
        print_summary(primary_impl.label(), &primary_outcome, args.num_items);

        if args.check {
            // `--check` always runs the *other* implementation as a
            // correctness oracle, whichever one was selected as primary.
            // No point instrumenting the reference implementation's timings.
            let reference_impl = primary_impl.other();
            let reference_outcome = run_with_big_stack(scope, move || {
                run_impl(reference_impl, args_ref, items_ref, None)
            });
            print_summary(reference_impl.label(), &reference_outcome, args.num_items);
            compare_outcomes(
                &items,
                primary_impl.label(),
                &primary_outcome,
                reference_impl.label(),
                &reference_outcome,
            );
        }

        if let Some(tool) = &measure_tool {
            tool.print_stats();
            show_histogram(tool, args.num_bars);
        }
    });
}

// ---------------------------------------------------------------------
// Setup notes
// ---------------------------------------------------------------------
// 1. Place this at src/bin/ring_bench.rs (or benches/ if you'd rather run
//    it via `cargo bench`, though as a straight binary it plays nicer with
//    `perf stat`).
//
// 2. Fix the three `use crate::...` paths above to match your actual
//    module layout.
//
// 3. `SyncRingBuffer` is assumed to expose the exact same surface as
//    SpmcRingBuffer: `new()`, `get_new_producer(&self) -> Option<P>`,
//    `get_new_consumer(&self) -> Option<C>`, and `push`/`try_push` on the
//    producer, `pop`/`try_pop` on the consumer. If you don't have a
//    synchronous reference implementation yet, a trivial one is a Mutex
//    around one VecDeque<T> per consumer, with push() broadcasting the
//    item into every consumer's queue (since SpmcRingBuffer broadcasts,
//    not load-balances). It doesn't need to be bounded by CAPACITY, since
//    it's a correctness oracle, not something to benchmark for
//    throughput.
//
// 4. Add to Cargo.toml (only if not already present):
//      clap = { version = "4", features = ["derive"] }
//    ratatui / crossterm / thread-priority you already have via
//    measure_tool.rs.
//
// 5. Three things you may not expect, all already handled here, worth
//    knowing about if you extend this file:
//      - The dispatch! match's branches must stay behind #[inline(never)]
//        runner functions, or some optimization levels will inline several
//        monomorphizations into main's stack frame and overflow it before
//        main even starts.
//      - SpmcRingBuffer::new() returns Self by value, so constructing it
//        puts the whole CAPACITY-sized buffer on the calling thread's
//        stack. That's why the actual run happens on a thread with an
//        explicit 512 MB stack rather than directly on main.
//      - `--implementation` picks which implementation is treated as
//        "primary" (the one whose timings get reported and instrumented
//        with --measure). `--check` always runs whichever implementation
//        was *not* selected as the reference oracle, so `--check` compares
//        the two against each other regardless of which one is primary.
//
// 6. Example invocations:
//      cargo build --release --bin ring_bench
//      perf stat ./target/release/ring_bench -c 65536 -n 4 -i 5000000
//      ./target/release/ring_bench -c 4096 -n 4 -i 500000 --check
//      ./target/release/ring_bench -c 4096 -n 2 -i 200000 --measure
//      ./target/release/ring_bench --implementation locked -c 4096 -i 500000
//      ./target/release/ring_bench -I locked --check -c 4096 -n 2 -i 200000
