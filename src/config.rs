pub const K: usize = 16;
pub const BLOCKSIZE: usize = LBA_SIZE / 64;
pub const THRESHOLD: usize = 128;
pub const ALPHA: f64 = 2.0;
pub const NUM_THREADS: usize = 8;

pub const LBA_SIZE: usize = 512 * 8;
pub const HUGE_PAGE_SIZE: usize = 2 * 1024 * 1024;

pub const DMA_BUFFERS: usize = 16;

const fn is_power_of_two(x: usize) -> bool {
    (x!=0) && ((x & (x-1)) == 0)
}

const _: () = {
    // TODO: modify asserts for DMA
    assert!(is_power_of_two(K), "K must be a power of two");
    assert!((64f64 /K.ilog2() as f64 - (64/K.ilog2()) as f64) < 0.00001, "64 must be divisible by log2(K)");
    assert!(is_power_of_two(BLOCKSIZE), "BLOCKSIZE must be a power of two");
    assert!(NUM_THREADS > 0, "NUM_THREADS must be at least one");
};