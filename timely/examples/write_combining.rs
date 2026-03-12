//! Microbenchmark comparing direct hash-scatter vs. software write combining (SWC).
//!
//! Usage:
//!     cargo run --release --example write_combining [N]
//!
//! N is the number of items to scatter (default: 10,000,000).
//!
//! The benchmark sweeps across partition counts (P) and item sizes, reporting
//! nanoseconds per item for direct scatter vs. SWC scatter, and the speedup ratio.
//! A speedup > 1.0 means SWC is faster.
//!
//! The direct scatter hashes each item and immediately pushes it to `destinations[hash % P]`.
//! The SWC scatter first pushes items into small per-partition staging buffers, flushing
//! each staging buffer to the destination in a burst when it fills up.

use std::hint::black_box;
use std::time::{Duration, Instant};

fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000_000);
    let iters = 7;

    println!("Software Write Combining Benchmark");
    println!("N = {n}, best of {iters} iterations\n");

    // Generate source data with pseudo-random values.
    let source_u64: Vec<u64> = (0..n as u64).map(scramble).collect();
    let source_2: Vec<[u64; 2]> = (0..n as u64)
        .map(|i| {
            let h = scramble(i);
            [h, h.wrapping_mul(3)]
        })
        .collect();
    let source_4: Vec<[u64; 4]> = (0..n as u64)
        .map(|i| {
            let h = scramble(i);
            [h, h.wrapping_mul(3), h.wrapping_mul(5), h.wrapping_mul(7)]
        })
        .collect();
    let source_8: Vec<[u64; 8]> = (0..n as u64)
        .map(|i| {
            let h = scramble(i);
            let mut a = [0u64; 8];
            for (j, x) in a.iter_mut().enumerate() {
                *x = h.wrapping_mul(2 * j as u64 + 1);
            }
            a
        })
        .collect();

    // Part 1: Sweep P for each item size.
    partition_sweep("u64 (8B)", &source_u64, |x| *x, iters);
    partition_sweep("[u64;2] (16B)", &source_2, |x| x[0], iters);
    partition_sweep("[u64;4] (32B)", &source_4, |x| x[0], iters);
    partition_sweep("[u64;8] (64B)", &source_8, |x| x[0], iters);

    // Part 2: Sweep stage_cap for a few interesting P values with u64 items.
    println!("=== Stage capacity sweep (u64, 8B) ===\n");
    for p_exp in [6, 8, 10] {
        stage_cap_sweep(&source_u64, |x| *x, 1 << p_exp, iters);
    }
}

/// Multiplicative hash / pseudo-random scramble.
fn scramble(x: u64) -> u64 {
    x.wrapping_mul(0x517cc1b727220a95)
}

// ---------------------------------------------------------------------------
// Part 1: Sweep partition count
// ---------------------------------------------------------------------------

fn partition_sweep<T: Copy>(
    label: &str,
    source: &[T],
    hash: impl Fn(&T) -> u64 + Copy,
    iters: usize,
) {
    let n = source.len();
    println!("--- {label} (stage_cap = {}) ---", default_stage_cap::<T>());
    println!(
        "{:>6} {:>16} {:>16} {:>8}",
        "P", "direct (ns/item)", "swc (ns/item)", "speedup"
    );

    for p_exp in 2..=10 {
        let p: usize = 1 << p_exp;
        let stage_cap = default_stage_cap::<T>();

        // Pre-allocate outside the timed section; reuse across iterations.
        let mut direct_dests: Vec<Vec<T>> = (0..p).map(|_| Vec::with_capacity(n / p + 64)).collect();
        let mut swc_dests: Vec<Vec<T>> = (0..p).map(|_| Vec::with_capacity(n / p + 64)).collect();
        let mut staging: Vec<Vec<T>> =
            (0..p).map(|_| Vec::with_capacity(stage_cap)).collect();

        let direct = bench(iters, || {
            for d in direct_dests.iter_mut() { d.clear(); }
            scatter_direct(source, &mut direct_dests, hash);
            black_box(&mut direct_dests);
        });

        let swc = bench(iters, || {
            for d in swc_dests.iter_mut() { d.clear(); }
            for s in staging.iter_mut() { s.clear(); }
            scatter_swc(source, &mut swc_dests, &mut staging, hash, stage_cap);
            black_box(&mut swc_dests);
        });

        let direct_ns = direct.as_nanos() as f64 / n as f64;
        let swc_ns = swc.as_nanos() as f64 / n as f64;
        let speedup = direct_ns / swc_ns;

        println!("{p:>6} {direct_ns:>16.2} {swc_ns:>16.2} {speedup:>8.2}x");
    }
    println!();
}

// ---------------------------------------------------------------------------
// Part 2: Sweep stage_cap for a fixed P
// ---------------------------------------------------------------------------

fn stage_cap_sweep<T: Copy>(
    source: &[T],
    hash: impl Fn(&T) -> u64 + Copy,
    p: usize,
    iters: usize,
) {
    let n = source.len();
    println!("--- P = {p} ---");
    println!(
        "{:>10} {:>16} {:>16} {:>8}",
        "stage_cap", "direct (ns/item)", "swc (ns/item)", "speedup"
    );

    let mut direct_dests: Vec<Vec<T>> = (0..p).map(|_| Vec::with_capacity(n / p + 64)).collect();
    let direct = bench(iters, || {
        for d in direct_dests.iter_mut() { d.clear(); }
        scatter_direct(source, &mut direct_dests, hash);
        black_box(&mut direct_dests);
    });
    let direct_ns = direct.as_nanos() as f64 / n as f64;

    for cap_exp in 1..=8 {
        let stage_cap: usize = 1 << cap_exp;

        let mut swc_dests: Vec<Vec<T>> = (0..p).map(|_| Vec::with_capacity(n / p + 64)).collect();
        let mut staging: Vec<Vec<T>> =
            (0..p).map(|_| Vec::with_capacity(stage_cap)).collect();
        let swc = bench(iters, || {
            for d in swc_dests.iter_mut() { d.clear(); }
            for s in staging.iter_mut() { s.clear(); }
            scatter_swc(source, &mut swc_dests, &mut staging, hash, stage_cap);
            black_box(&mut swc_dests);
        });
        let swc_ns = swc.as_nanos() as f64 / n as f64;
        let speedup = direct_ns / swc_ns;

        println!("{stage_cap:>10} {direct_ns:>16.2} {swc_ns:>16.2} {speedup:>8.2}x");
    }
    println!();
}

// ---------------------------------------------------------------------------
// Scatter implementations
// ---------------------------------------------------------------------------

/// Direct scatter: hash each item and push immediately to the destination.
#[inline(never)]
fn scatter_direct<T: Copy>(source: &[T], dests: &mut [Vec<T>], hash: impl Fn(&T) -> u64) {
    let mask = (dests.len() - 1) as u64;
    for item in source {
        let idx = (hash(item) & mask) as usize;
        dests[idx].push(*item);
    }
}

/// SWC scatter: hash each item into a small staging buffer; when the staging buffer
/// fills, flush it to the destination in a burst.
#[inline(never)]
fn scatter_swc<T: Copy>(
    source: &[T],
    dests: &mut [Vec<T>],
    staging: &mut [Vec<T>],
    hash: impl Fn(&T) -> u64,
    stage_cap: usize,
) {
    let mask = (dests.len() - 1) as u64;
    for item in source {
        let idx = (hash(item) & mask) as usize;
        staging[idx].push(*item);
        if staging[idx].len() >= stage_cap {
            dests[idx].extend(staging[idx].drain(..));
        }
    }
    // Flush remaining.
    for (dest, stage) in dests.iter_mut().zip(staging.iter_mut()) {
        dest.extend(stage.drain(..));
    }
}

// ---------------------------------------------------------------------------
// Timing utility
// ---------------------------------------------------------------------------

/// Run `f` for `iters` iterations, return the minimum elapsed time.
fn bench(iters: usize, mut f: impl FnMut()) -> Duration {
    (0..iters).map(|_| {
        let start = Instant::now();
        f();
        start.elapsed()
    }).min().unwrap()
}

/// Default staging capacity: enough items to fill ~4 cache lines (256 bytes).
fn default_stage_cap<T>() -> usize {
    const CACHE_LINE_BYTES: usize = 64;
    const TARGET_LINES: usize = 4;
    let item_size = std::mem::size_of::<T>();
    if item_size == 0 {
        CACHE_LINE_BYTES
    } else {
        (CACHE_LINE_BYTES * TARGET_LINES / item_size).max(2)
    }
}
