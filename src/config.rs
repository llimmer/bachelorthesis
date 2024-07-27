pub const K: usize = 8;
pub const BLOCKSIZE: usize = 8;
pub const THRESHOLD: usize = 16;
pub const ALPHA: f64 = 1.8;

const fn is_power_of_two(x: usize) -> bool {
    (x!=0) && ((x & (x-1)) == 0)
}

const _: () = {
    assert!(is_power_of_two(K), "K must be a power of two");
    assert!(is_power_of_two(BLOCKSIZE), "BLOCKSIZE must be a power of two");

};