use rand::prelude::*;
use std::{env, io};
use std::time::Duration;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use bachelorthesis::{BLOCKSIZE, sort, THRESHOLD, initialize_thread_pool, NUM_THREADS, prepare_benchmark, rolling_sort, HUGE_PAGE_SIZE_1G, sort_merge_initialize_thread_local};


pub fn main() {
    let mut args = env::args();
    args.next();

    let pci_addr = match args.next() {
        Some(arg) => arg,
        None => panic!("Expected <pci_addr> argument in format [0000:00:00.0]"),
    };

    let num_hugepages = match args.next() {
        Some(arg) => arg.parse::<usize>().unwrap(),
        None => {
            panic!("Usage: cargo run --benches bench_sequential <size> <iterations> <seed?>");
        }
    };

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

    let mut measurements: Vec<Duration> = Vec::with_capacity(iterations);
    let mut rng = StdRng::seed_from_u64(seed);
    let mut nvme = vroom::init(&pci_addr).unwrap();
    initialize_thread_pool();
    nvme = sort_merge_initialize_thread_local(nvme);
    nvme = prepare_benchmark(nvme, num_hugepages, seed as usize);

    rolling_sort(nvme, num_hugepages*HUGE_PAGE_SIZE_1G/8).unwrap();
}

fn generate_uniform(rng: &mut StdRng, length: usize) -> Vec<u64> {
    (0..length)
        .map(|_| rng.gen::<u64>())
        .collect()
}



