use std::time::Instant;
use rand::prelude::{SliceRandom, StdRng};
use rand::SeedableRng;
use bachelorthesis::{sort, sort_parallel};

pub fn main() {
    let mut data: Vec<u64> = (0..134217728*2).collect();
    let mut rng = StdRng::seed_from_u64(12345);
    let mut rng2 = StdRng::seed_from_u64(12345);
    let mut rng3 = StdRng::seed_from_u64(12345);
    data.shuffle(&mut rng);

    let start = Instant::now();
    sort(&mut data);
    let duration = start.elapsed();

    println!("Sequential IPS2Ra: {:?}", duration);

    for j in 1..data.len() {
        assert!(data[j - 1] <= data[j]);
    }

    data.shuffle(&mut rng2);
    let start = Instant::now();
    sort_parallel(&mut data);
    let duration = start.elapsed();

    println!("Parallel IPS2Ra: {:?}", duration);
    for j in 1..data.len() {
        assert!(data[j - 1] <= data[j]);
    }


    data.shuffle(&mut rng3);
    let start = Instant::now();
    data.sort_unstable();
    let duration = start.elapsed();

    println!("Rust Sort: {:?}", duration);
}