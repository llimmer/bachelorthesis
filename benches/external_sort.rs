// tests the speed to read, sort and write a hugepage

use std::env;
use std::error::Error;
use std::time::Duration;
use bachelorthesis::{prepare_benchmark, HUGE_PAGE_SIZE_1G, sort_parallel_threadlocal};

pub fn main() -> Result<(), Box<dyn Error>> {
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
    let mut measurements: Vec<Vec<Duration>> = Vec::with_capacity(hugepages.len());

    for i in 0..hugepages.len() {
        let mut local_measurements: Vec<Duration> = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            nvme = prepare_benchmark(nvme, hugepages[i], seed as usize);
            let mut start = std::time::Instant::now();
            sort_parallel_threadlocal(HUGE_PAGE_SIZE_1G/8, 1, 0);
            let duration = start.elapsed();
            local_measurements.push(duration);
        }
        measurements.push(local_measurements);
    }

    for i in 0..hugepages.len() {
        let avg = (measurements[i].iter().sum::<Duration>() / iterations as u32).as_secs_f64();
        let min = measurements[i].iter().min().unwrap().as_secs_f64();
        let max = measurements[i].iter().max().unwrap().as_secs_f64();
        println!("Number of hugepages: {:?}: Avg {:?}, Min: {:?}, Max: {:?}", hugepages[i], avg, min, max);
    }

    Ok(())
}