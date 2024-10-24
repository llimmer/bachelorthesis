use rand::prelude::*;
use std::{env, io};
use std::error::Error;
use std::time::Duration;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rayon::prelude::ParallelSliceMut;
use bachelorthesis::{BLOCKSIZE, sort, THRESHOLD, initialize_thread_pool, NUM_THREADS, prepare_benchmark, rolling_sort, HUGE_PAGE_SIZE_1G, sort_merge_initialize_thread_local, sort_merge, sort_parallel};


pub fn main() -> Result<(), Box<dyn Error>>{
    let mut args = env::args();
    args.next();

    let pci_addr = match args.next() {
        Some(arg) => arg,
        None => panic!("Expected <pci_addr> argument in format [0000:00:00.0]"),
    };

    let hugepages_input = match args.next() {
        Some(arg) => arg,
        None => panic!("Expected <array> argument in format [123, 234, 345, 456]"),
    };
    let hugepages_input = hugepages_input.trim_matches(|c| c == '[' || c == ']');
    let hugepages: Vec<usize> = hugepages_input
        .split(',')
        .map(|s| s.trim().parse::<usize>().unwrap()) // Parse each number
        .collect();

    let iterations = match args.next() {
        Some(arg) => arg.parse::<usize>().unwrap(),
        None => {
            panic!("Usage: cargo run --benches bench_sequential <size> <iterations> <seed?>");
        }
    };

    let seed = match args.next() {
        Some(arg) => arg.parse::<u64>().unwrap(),
        None => {
            eprintln!("No seed specified. Using 12345");
            12345
        }
    };

    let mut nvme = vroom::init(&pci_addr)?;
    let mut measurements: Vec<Duration> = Vec::with_capacity(hugepages.len());

    for i in 0..hugepages.len() {
        let mut local_measurements: Vec<Duration> = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            nvme = prepare_benchmark(nvme, hugepages[i], seed as usize);
            let mut start = std::time::Instant::now();
            nvme = sort_merge(nvme, hugepages[i] * HUGE_PAGE_SIZE_1G / 8, true)?;
            let duration = start.elapsed();
            local_measurements.push(duration);
        }
        let avg = local_measurements.iter().sum::<Duration>() / iterations as u32;
        measurements.push(avg);
    }
    // print as table

    println!("Number of hugepages: {:?}", hugepages);
    // print times in seconds
    println!("{:?}", measurements.iter().map(|d| d.as_secs_f64()).collect::<Vec<f64>>());

    Ok(())
}

fn generate_uniform(rng: &mut StdRng, length: usize) -> Vec<u64> {
    (0..length)
        .map(|_| rng.gen::<u64>())
        .collect()
}



