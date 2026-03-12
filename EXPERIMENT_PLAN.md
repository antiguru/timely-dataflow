# SWC cache aliasing experiment plan

## Background

On a Threadripper Pro 3955WX (Linux/glibc), direct scatter beats SWC at most
partition counts. Investigation revealed that glibc's allocator diversifies L1d
cache set indices (44/64 unique sets) due to arena reuse from prior iterations.
When allocations are forced to the same cache set (fresh mmap), direct scatter
slows down 3x and SWC wins — matching the M2 results.

Hypothesis: the M2's large SWC speedups are primarily caused by macOS's
allocator placing all Vec backing buffers at the same L1d cache set index.

## Steps

Run each command and paste the output into a file or message.

### Step 1: check allocator aliasing

```
cargo run --release --example swc_perf
```

Expected if hypothesis is correct:
* "Fresh mmap-sized allocations" shows 1/64 or 1/256 unique sets.
* "After allocate-free cycle" ALSO shows low unique-set counts (unlike Linux
  where it jumped to 44/64). This would confirm macOS's allocator doesn't
  diversify addresses after reuse.

### Step 2: run the reuse-based benchmark

```
cargo run --release --example write_combining
```

This version pre-allocates buffers outside the timing loop and reuses them
via `clear()`. The allocations happen in order (P=4, P=8, ..., P=1024) so
the allocator may or may not diversify addresses.

Compare with the original M2 results (from `~/Downloads/write_combining.txt`):
* If direct scatter is now much faster (closer to 1-2 ns/item instead of 5 ns),
  the original slowdown was allocator-induced aliasing.
* If direct scatter is still ~5 ns/item, check step 1 output — the reuse
  allocations might still alias on macOS.

### Step 3: force-diversify addresses (if step 2 still shows aliasing)

If both benchmarks still show aliasing on macOS, add this patch to
`write_combining.rs` to pad each Vec's base address to a different cache set.
Replace the `partition_sweep` allocation block:

```rust
// Add offset to diversify cache sets.
let mut direct_dests: Vec<Vec<T>> = (0..p).map(|i| {
    let mut v = Vec::with_capacity(n / p + 64 + i * 8);
    // Push dummy items to offset the write head, then clear.
    for _ in 0..(i * 8) { v.push(unsafe { std::mem::zeroed() }); }
    v.clear();
    v
}).collect();
```

This offsets each Vec's write-head start address by `i * 64` bytes (i cache
lines), ensuring they map to different L1d sets. Re-run the benchmark and
compare direct scatter times.

### Step 4: interpret results

| Outcome | Conclusion |
|---------|-----------|
| Step 1 shows all-same-set AND step 3 fixes direct scatter | Allocator aliasing is the root cause. SWC speedup on M2 is an artifact. |
| Step 1 shows diverse sets BUT direct scatter is still slow | M2 has a genuine hardware limitation for random stores (store buffer depth, L2 latency, etc.). |
| Step 1 shows all-same-set BUT step 3 doesn't help | Aliasing exists but isn't the bottleneck. Look at store buffer / memory subsystem differences. |
