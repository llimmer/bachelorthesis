pub const K: usize = 256;
pub const BLOCKSIZE: usize = 256;
pub const THRESHOLD: usize = 32;
pub const ALPHA: f64 = 2.0;

// Number of threads for parallel sorting, use 0 for system default
pub const NUM_THREADS: usize = 0;

const fn is_power_of_two(x: usize) -> bool {
    (x!=0) && ((x & (x-1)) == 0)
}

const _: () = {
    assert!(K > 1, "K must be greater than 1");
    assert!(is_power_of_two(K), "K must be a power of two");
    assert!((64f64 /K.ilog2() as f64 - (64/K.ilog2()) as f64) < 0.00001, "64 must be divisible by log2(K)");
    assert!(is_power_of_two(BLOCKSIZE), "BLOCKSIZE must be a power of two");
};