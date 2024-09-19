pub const K: usize = 16; // number of buckets
pub const BLOCKSIZE: usize = 128; // number of elements that belong to same bucket
pub const THRESHOLD: usize = 128; // Threshold from which samplesort is used
pub const NUM_THREADS: usize = 8;

// DMA:
pub const LBA_SIZE: usize = 512; // Logical Block Addressing Size of SSD (usually 512)
pub const CHUNK_SIZE: usize = 2*4096; // Number of bytes that can be read/written with a single SQE
pub const HUGE_PAGES_2M: usize = 256; // Number of 2 MiB hugepages allocated in main memory (-> Setup script)
pub const HUGE_PAGES_1G: usize = 4; // Number of 1 GiB hugepages allocated in main memory (-> Setup script)
pub const HUGE_PAGE_SIZE_2M: usize = 2 * 1024 * 1024; // Number of bytes per 2M hugepage
pub const HUGE_PAGE_SIZE_1G: usize = 1024 * 1024 * 1024; // Number of bytes per 1G hugepage
pub const CHUNKS_PER_HUGE_PAGE_2M: usize = HUGE_PAGE_SIZE_2M / CHUNK_SIZE;
pub const CHUNKS_PER_HUGE_PAGE_1G: usize = HUGE_PAGE_SIZE_1G / CHUNK_SIZE;
pub const ELEMENTS_PER_CHUNK: usize = CHUNK_SIZE / 8;
pub const LBA_PER_CHUNK: usize = CHUNK_SIZE / LBA_SIZE;


const fn is_power_of_two(x: usize) -> bool {
    (x!=0) && ((x & (x-1)) == 0)
}

const _: () = {
    // TODO: modify asserts for DMA
    assert!(is_power_of_two(K), "K must be a power of two");
    assert!((64f64 /K.ilog2() as f64 - (64/K.ilog2()) as f64) < 0.00001, "64 must be divisible by log2(K)");
    assert!(is_power_of_two(BLOCKSIZE), "BLOCKSIZE must be a power of two");
    assert!(NUM_THREADS > 0, "NUM_THREADS must be at least one");
    assert!(HUGE_PAGE_SIZE_1G % CHUNK_SIZE == 0, "LBA SIZE must be a divisor of HUGE_PAGE_SIZE");
    assert!(CHUNK_SIZE % LBA_SIZE == 0, "LBA SIZE must be a divisor of CHUNK_SIZE");
    //assert!(CHUNKS_PER_HUGE_PAGE < 1024, "CHUNKS_PER_HUGE_PAGE must be smaller than 1024");
    // TODO: check that at least one element buffer gets full during classification (need enough DMA buffers)

};