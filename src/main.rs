#![feature(thread_spawn_unchecked)]

use log::LevelFilter;
use log::{debug, info, error};
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use rand::{SeedableRng};
use std::time::Instant;
use bachelorthesis::sort::sort_parallel;

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
    let mut arr: Vec<u64> = (0..1_000_000_000).collect();
    arr.shuffle(&mut rng);
    let mut arr2 = arr.clone();
    let mut arr3 = arr.clone();

    //println!("unsorted: {:?}", arr);

    let start = Instant::now();
    sort(&mut arr);
    let duration = start.elapsed();
    println!("IPS2Ra Sort Sequential: {:?}", duration);

    let start = Instant::now();
    sort_parallel(&mut arr2);
    let duration = start.elapsed();
    println!("IPS2Ra Sort Parallel: {:?}", duration);


    let start = Instant::now();
    arr3.sort_unstable();
    let duration = start.elapsed();
    println!("Quicksort: {:?}", duration);

    verify_sorted(&arr);
    verify_sorted(&arr2);
}


