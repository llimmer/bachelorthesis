pub const K: usize = 16;
pub const BLOCKSIZE: usize = 16;
pub const THRESHOLD: usize = 8;
pub const ALPHA: f64 = 2.0;
pub const NUM_THREADS: usize = 16;

const fn is_power_of_two(x: usize) -> bool {
    (x!=0) && ((x & (x-1)) == 0)
}

const _: () = {
    assert!(is_power_of_two(K), "K must be a power of two");
    assert!((64f64 /K.ilog2() as f64 - (64/K.ilog2()) as f64) < 0.00001, "64 must be divisible by log2(K)");
    assert!(is_power_of_two(BLOCKSIZE), "BLOCKSIZE must be a power of two");
    assert!(NUM_THREADS > 0, "NUM_THREADS must be at least one");
};