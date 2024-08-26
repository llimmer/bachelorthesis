#![feature(thread_spawn_unchecked)]

use log::LevelFilter;
use log::{debug, info, warn, error};
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng, thread_rng};
use std::cmp::max;
use std::io;
use std::time::Instant;
use rand::seq::index::sample;
use std::error::Error;

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

use crate::base_case::insertion_sort;
use crate::conversion::{u64_to_u8_slice, u8_to_u64, u8_to_u64_slice};
use crate::setup::{clear, setup_array};
use crate::sort::sort;
use crate::sorter::{DMATask, IPS2RaSorter, IPS2RaSorterDMA, Task};

fn verify_sorted(arr: &Vec<u64>) {
    for i in 1..arr.len() {
        assert!(arr[i - 1] <= arr[i]);
    }
}

fn main() -> Result<(), Box<dyn Error>>{
    env_logger::builder()
        .filter_level(LevelFilter::Error)
        .init();

    let mut nvme = vroom::init("0000:00:04.0")?;

    let length = 4097;

    let mut arr: Vec<u64> = (0..length).rev().collect();
    let mut rng = StdRng::seed_from_u64(12345);
    arr.shuffle(&mut rng);

    clear(10000, &mut nvme);
    setup_array(&mut arr, &mut nvme);

    let mut sorter1 = IPS2RaSorter::new_sequential();
    let mut sorter2 = IPS2RaSorterDMA::new_sequential(nvme);

    let mut task1 = Task::new(&mut arr, 0);
    task1.sample();



    let mut task2 = DMATask::new(0, 0, length as usize, task1.level);

    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();

    unsafe {
        sorter1.classify(&mut task1);
        sorter2.classify(&mut task2);
    }

    println!("\n\n\n{} of {} elements classified, Array after classification: {:?}", sorter1.classified_elements, length, task1.arr);
    println!("-------------------------------------------------");



    // read the array from disk into one big array
    let mut tmp = [0; 4097*8];
    let mut nvme = vroom::init("0000:00:04.0")?;
    for i in 0..64 {
        println!("Reading block {}", i);
        let mut target_slice = &mut tmp[i*512..(i+1)*512];
        nvme.read_copied(&mut target_slice, i as u64)?;
    }
    println!("\n\n\n{} of {} elements classified, Array after classification: {:?}", sorter2.classified_elements, length, u8_to_u64_slice(&mut tmp));


    println!("{:?}\n", sorter1);
    println!("{:?}\n", sorter2);


    println!("Done");

    Ok(())
}


