use rand::prelude::*;
use std::{env};
use std::time::Duration;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use bachelorthesis::{sort};


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

    // modes:
    // 0: all
    // 1: IPS2Ra sequential
    // 2: Rust sort()
    // 3: Rust sort_unstable()
    let mode = match args.next() {
        Some(arg) => arg.parse::<usize>().unwrap(),
        None => {
            eprintln!("No mode specified. Using 'all' (0)");
            0
        }
    };

    // distributions:
    // 0: all
    // 1: sorted
    // 2: reverse sorted
    // 3: almost sorted
    // 4: uniform
    // 5: exponential
    // 6: root dup
    // 7: two dup
    // 8: eight dup
    // 9: range
    let distribution = match args.next() {
        Some(arg) => arg.parse::<usize>().unwrap(),
        None => {
            eprintln!("No distribution specified. Using 'all' (0)");
            0
        }
    };

    if mode > 4 || distribution > 8 {
        panic!("Invalid mode or distribution specified. Mode: {}, Distribution: {}. Exiting.", mode, distribution);
    }

    let mut start_algo = { if mode == 0 { 1 } else { mode } };
    let mut max_algo = { if mode == 0 { 3 } else { mode } };
    let mut start_dist = { if distribution == 0 { 1 } else { distribution } };
    let mut max_dist = { if distribution == 0 { 9 } else { 1 } };

    let mut measurements: Vec<Vec<Vec<Duration>>> = vec![vec![Vec::with_capacity(iterations); (max_algo - start_algo) + 1]; (max_dist - start_dist) + 1];
    let mut rng = StdRng::seed_from_u64(seed);

    for it in 0..iterations {
        println!("Iteration {}", it);
        for j in start_dist..=max_dist {
            println!("Distribution {}", j);
            for k in start_algo..=max_algo {
                let mut data: Vec<u64> = match j {
                    1 => generate_sorted(size),
                    2 => generate_reverse_sorted(size),
                    3 => generate_almost_sorted(&mut rng, size),
                    4 => generate_uniform(&mut rng, size),
                    5 => generate_exponential(&mut rng, size),
                    6 => generate_root_dup(size),
                    7 => generate_two_dup(size),
                    8 => generate_eight_dup(size),
                    9 => generate_in_range(&mut rng, size, u32::MAX as u64),
                    _ => panic!("Invalid distribution")
                };
                println!("Algorithm {}", k);
                let start = std::time::Instant::now();
                match k {
                    1 => sort(&mut data),
                    2 => data.sort(),
                    3 => data.sort_unstable(),
                    _ => {}
                }
                measurements[j - 1][k - 1].push(start.elapsed());
            }
        }
        println!()
    }

    // print
    for k in start_algo..=max_algo {
        println!("{}:", match k {
            1 => "IPS2Ra sequential",
            2 => "Rust sort()",
            3 => "Rust sort_unstable()",
            _ => panic!("Invalid algorithm")
        });
        for j in start_dist..=max_dist {
            let mut sum = Duration::new(0, 0);
            for i in 0..iterations {
                sum += measurements[j - 1][k - 1][i];
            }
            let avg = sum / iterations as u32;
            println!("\t{}: {:?}", match j {
                1 => "sorted",
                2 => "reverse sorted",
                3 => "almost sorted",
                4 => "uniform",
                5 => "exponential",
                6 => "root dup",
                7 => "two dup",
                8 => "eight dup",
                9 => "u32 range",
                _ => panic!("Invalid distribution")
            }, avg);
        }
        println!();
    }
}

// Input generator functions

// exponential distribution.
fn generate_exponential(rng: &mut StdRng, n: usize) -> Vec<u64> {
    let log_n = (n as f64).log(2.0).ceil() as usize; // Calculate log base 2 of n
    (0..n).map(|i| {
        let i = (i % log_n) as f64; // i should be in [0, log_n)
        let lower_bound = (2f64.powf(i));
        let upper_bound = (2f64.powf(i + 1.0));
        rng.gen_range(lower_bound..upper_bound) as u64 // Select uniformly from [2^i, 2^(i+1))
    }).collect()
}

// rootDup distribution.
fn generate_root_dup(n: usize) -> Vec<u64> {
    let sqrt_n = (n as f64).sqrt() as usize; // Floor of the square root of n
    (0..n).map(|i| {
        let value = i % sqrt_n; // A[i] = i mod floor(sqrt(n))
        value as u64
    }).collect()
}

// twoDup distribution.
fn generate_two_dup(n: usize) -> Vec<u64> {
    (0..n).map(|i| {
        let value = (i * i + n / 2) % n; // A[i] = i^2 + n/2 mod n
        value as u64
    }).collect()
}

// eightDup distribution.
fn generate_eight_dup(n: usize) -> Vec<u64> {
    (0..n).map(|i| {
        let value = (i.pow(8) + n / 2) % n; // A[i] = i^8 + n/2 mod n
        value as u64
    }).collect()
}

// 95% sorted
fn generate_almost_sorted(rng: &mut StdRng, length: usize) -> Vec<u64> {
    let mut data: Vec<u64> = (0..length as u64).collect();

    for _ in 0..(length / 20) { // swap 5% of data
        let i = rng.gen_range(0..length);
        let j = rng.gen_range(0..length);
        data.swap(i, j);
    }
    data
}

// uniform distribution
fn generate_uniform(rng: &mut StdRng, length: usize) -> Vec<u64> {
    (0..length)
        .map(|_| rng.gen::<u64>())
        .collect()
}

// range
fn generate_in_range(rng: &mut StdRng, length: usize, range: u64) -> Vec<u64> {
    (0..length)
        .map(|_| rng.gen_range(0..range))
        .collect()
}

fn generate_sorted(length: usize) -> Vec<u64> {
    (0..length as u64).collect()
}

fn generate_reverse_sorted(length: usize) -> Vec<u64> {
    (0..length as u64).rev().collect()
}

