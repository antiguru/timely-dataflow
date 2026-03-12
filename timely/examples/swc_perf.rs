//! Diagnostic tool for investigating L1d cache set aliasing in Vec allocations.
//!
//! Usage:
//!     cargo run --release --example swc_perf
//!
//! Reports the L1d cache set index for each Vec's backing buffer across several
//! allocation strategies. If all buffers land in the same set, the scatter
//! benchmark will show artificial slowdowns due to conflict misses.

fn main() {
    let n: usize = 10_000_000;
    let p: usize = 64;

    // L1d set count: 32KB / 8-way / 64B = 64 sets (Zen 2/3, Firestorm).
    // Apple M1/M2 performance cores have 128KB L1d / 8-way / 64B = 256 sets,
    // so use 8 index bits there. We print both.
    println!("L1d cache set aliasing diagnostic (P={p}, cap={})\n", n / p + 64);

    println!("=== Fresh mmap-sized allocations (no prior allocs) ===");
    {
        let dests: Vec<Vec<u64>> = (0..p).map(|_| Vec::with_capacity(n / p + 64)).collect();
        print_sets(&dests);
    }

    println!("\n=== After allocate-free cycle (simulating prior P iterations) ===");
    {
        // Simulate earlier P values by allocating and freeing large buffers.
        for prev_p in [4, 8, 16, 32] {
            let _prev: Vec<Vec<u64>> = (0..prev_p * 3)
                .map(|_| Vec::with_capacity(n / prev_p + 64))
                .collect();
        }
        let dests: Vec<Vec<u64>> = (0..p).map(|_| Vec::with_capacity(n / p + 64)).collect();
        print_sets(&dests);
    }

    println!("\n=== Repeated alloc-free in loop (original bench pattern) ===");
    {
        let mut addrs = Vec::new();
        for _ in 0..3 {
            let dests: Vec<Vec<u64>> = (0..p).map(|_| Vec::with_capacity(n / p + 64)).collect();
            if addrs.is_empty() {
                addrs = dests.iter().map(|v| v.as_ptr() as usize).collect();
            }
        }
        print_addrs(&addrs);
    }
}

fn print_sets<T>(vecs: &[Vec<T>]) {
    let addrs: Vec<usize> = vecs.iter().map(|v| v.as_ptr() as usize).collect();
    print_addrs(&addrs);
}

fn print_addrs(addrs: &[usize]) {
    let sets_64: std::collections::HashSet<usize> =
        addrs.iter().map(|a| (a >> 6) & 0x3f).collect();
    let sets_256: std::collections::HashSet<usize> =
        addrs.iter().map(|a| (a >> 6) & 0xff).collect();

    for i in 0..8.min(addrs.len()) {
        let ptr = addrs[i];
        println!(
            "  [{i}] {ptr:#x}  set(64)={:<3} set(256)={}",
            (ptr >> 6) & 0x3f,
            (ptr >> 6) & 0xff,
        );
    }
    println!(
        "  unique sets: {}/64 (32KB L1d), {}/256 (128KB L1d)",
        sets_64.len(),
        sets_256.len(),
    );
}
