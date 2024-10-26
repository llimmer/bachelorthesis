mod sampling;
mod base_case;
mod classification;
mod config;
mod permutation;
mod cleanup;
mod sort;
mod sorter;
mod sequential;
mod parallel;
mod conversion;
mod setup;
mod parallel_sort_merge;
mod rolling_sort;
mod sequential_sort_merge;
use vroom::memory::{DmaSlice};
use std::error::Error;
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};


fn verify_sorted(arr: &[u64]) {
    for i in 1..arr.len() {
        assert!(arr[i - 1] <= arr[i], "Difference at i={i}. {} > {}", arr[i - 1], arr[i]);
    }
}

use rand::prelude::*;
use rayon::prelude::ParallelSliceMut;
use crate::sort::{sort};



fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Error)
        .init();

    let mut rng = StdRng::seed_from_u64(54321);
    let mut data = generate_uniform(&mut rng, 1_000_000);

    sort(&mut data);
    verify_sorted(&data);

    Ok(())
}


fn generate_uniform(rng: &mut StdRng, length: usize) -> Vec<u64> {
    (0..length)
        .map(|_| rng.gen::<u64>())
        .collect()
}


