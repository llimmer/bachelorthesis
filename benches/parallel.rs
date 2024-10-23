use rand::prelude::*;
use std::{env};
use std::time::Duration;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rayon::prelude::ParallelSliceMut;
use bachelorthesis::{sort_parallel, BLOCKSIZE, THRESHOLD};


pub fn main() {
    let mut args = env::args();
    args.next();

    let sizes_input = match args.next() {
        Some(arg) => arg,
        None => panic!("Expected <array> argument in format [123, 234, 345, 456]"),
    };
    let sizes_input = sizes_input.trim_matches(|c| c == '[' || c == ']');
    let sizes: Vec<usize> = sizes_input
        .split(',')
        .map(|s| s.trim().parse::<usize>().unwrap()) // Parse each number
        .collect();

    let iterations = match args.next() {
        Some(arg) => arg.parse::<usize>().unwrap(),
        None => {
            panic!("Usage: cargo run --benches bench_sequential <size> <iterations> <seed?>");
        }
    };

    // 0: Ips2Ra parallel
    // 1: Rayon par_sort_unstable()
    // 2: Rayon par_sort_unstable()
    let mode = match args.next() {
        Some(arg) => arg.parse::<usize>().unwrap(),
        None => {
            eprintln!("No mode specified. Using 'ips2ra' (0)");
            0
        }
    };

    let seed = match args.next() {
        Some(arg) => arg.parse::<u64>().unwrap(),
        None => {
            eprintln!("No seed specified. Using 12345");
            12345
        }
    };

    let mut measurements: Vec<Duration> = Vec::with_capacity(sizes.len());
    let mut rng = StdRng::seed_from_u64(seed);
    for i in 0..sizes.len() {
        let mut local_measurements: Vec<Duration> = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let mut data = generate_uniform(&mut rng, sizes[i]);
            let mut start = std::time::Instant::now();
            match mode {
                0 => sort_parallel(&mut data),
                1 => data.par_sort(),
                2 => data.par_sort_unstable(),
                _ => panic!("Invalid mode"),
            }
            let duration = start.elapsed();
            local_measurements.push(duration);
        }
        let avg = local_measurements.iter().sum::<Duration>() / iterations as u32;
        measurements.push(avg);
    }
    // print as table
    match mode {
        0 => println!("IPS2Ra: BLOCKSIZE = {}, THRESHOLD = {}", BLOCKSIZE, THRESHOLD),
        1 => println!("Rayon par_sort()"),
        2 => println!("Rayon par_sort_unstable()"),
        _ => {}
    }
    println!("Sizes: {:?}", sizes);
    println!("{:?}", measurements);

}

// uniform distribution
fn generate_uniform(rng: &mut StdRng, length: usize) -> Vec<u64> {
    (0..length)
        .map(|_| rng.gen::<u64>())
        .collect()
}
