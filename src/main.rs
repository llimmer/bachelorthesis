#![feature(thread_spawn_unchecked)]

use log::LevelFilter;
use log::{debug, info, warn, error};
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng, thread_rng};
use std::cmp::max;
use std::time::Instant;

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

use sorter::Sorter;
use crate::base_case::insertion_sort;
use crate::sort::sort;

fn verify_sorted(arr: &Vec<u64>) {
    for i in 1..arr.len() {
        assert!(arr[i - 1] <= arr[i]);
    }
}

fn main() {
    env_logger::builder()
        .filter_level(LevelFilter::Error)
        .init();
    let mut rng = StdRng::seed_from_u64(12345);

    for i in 0..1000 {
        let n = rng.gen_range(256..1_000_000);
        //let n = 10_000_000;
        let mut arr: Vec<u64> = (0..n).collect();
        arr.shuffle(&mut rng);

        sort(&mut arr, false);
        verify_sorted(&arr);
    }
    println!("Sequential sort successful!");
}
