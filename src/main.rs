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

use crate::base_case::insertion_sort;
use crate::sort::sort;
use crate::sorter::IPS2RaSorter;

fn verify_sorted(arr: &Vec<u64>) {
    for i in 1..arr.len() {
        assert!(arr[i - 1] <= arr[i]);
    }
}
struct Block<'a> {
        arr: &'a mut[u64],
        count: usize,
}


fn main() {
    env_logger::builder()
        .filter_level(LevelFilter::Error)
        .init();
    let mut rng = StdRng::seed_from_u64(12345);
    let mut arr: Vec<u64> = (0..10_000_000).collect();
    arr.shuffle(&mut rng);
    let arr2 = arr.clone();

    //println!("unsorted: {:?}", arr);

    sort(&mut arr, false);
    //println!("sorted: {:?}", arr);

    verify_sorted(&arr);
}


