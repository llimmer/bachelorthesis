use rand::prelude::*;
use std::{env, process};
use std::time::Duration;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use bachelorthesis::{initialize_thread_pool, sort, sort_parallel};
use rand_distr::{Distribution, Exp};
use zipf::ZipfDistribution;

pub fn main() {
    let mut args = env::args();
    args.next();

    let size = match args.next() {
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

    // warm up
    {
        let mut data = generate_uniform(&mut StdRng::seed_from_u64(seed), size);
        sort_parallel(&mut data);
    }
    println!("Starting benchmark");
    let mut measurements: Vec<Duration> = Vec::new();

    for i in 0..iterations {
        let mut data = generate_uniform(&mut StdRng::seed_from_u64(seed), size);
        println!("Iteration {}", i);
        let mut start = std::time::Instant::now();
        sort_parallel(&mut data);
        let duration = start.elapsed();
        measurements.push(duration);
    }

    let avg = measurements.iter().sum::<Duration>() / iterations as u32;
    println!("Parallel Sort using {} threads: Avg {:?}", rayon::current_num_threads(), avg);

}

// uniform distribution
fn generate_uniform(rng: &mut StdRng, length: usize) -> Vec<u64> {
    (0..length)
        .map(|_| rng.gen::<u64>())
        .collect()
}
