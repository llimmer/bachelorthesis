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

use sorter::IPS4oSorter;
use crate::base_case::insertion_sort;
use crate::sort::{ips2ra_sort, sort};
use crate::sorter::IPS2RaSorter;

fn verify_sorted(arr: &Vec<u64>) {
    for i in 1..arr.len() {
        assert!(arr[i - 1] <= arr[i]);
    }
}

fn main() {
    env_logger::builder()
        .filter_level(LevelFilter::Debug)
        .init();
    let mut rng = StdRng::seed_from_u64(12345);
    let mut arr: Vec<u64> = (0..4096).collect();
    arr.shuffle(&mut rng);
    // print array as binary
    print!("Array: [");
    for i in 0..arr.len()-1 {
        print!("{}: {:06b}, ", arr[i], arr[i]);
    }
    println!("{}: {:06b}]", arr[arr.len()-1], arr[arr.len()-1]);

    println!("{}", classification::find_bucket_ips2ra(31, 29));

    ips2ra_sort(&mut arr);

    verify_sorted(&arr);

    println!("sorted: {:?}", arr);


    //for i in 0..1000 {
    //    let n = rng.gen_range(256..1_000_000);
    //    //let n = 10_000_000;
    //    let mut arr: Vec<u64> = (0..n).collect();
    //    arr.shuffle(&mut rng);
//
    //    sort(&mut arr, false);
    //    verify_sorted(&arr);
    //}
    //println!("Sequential sort successful!");
}
