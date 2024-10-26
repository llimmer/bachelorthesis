use std::{env, process};
use std::error::Error;
use std::time::Instant;
use log::LevelFilter;
use bachelorthesis::{HUGE_PAGE_SIZE_1G, sort_merge};

fn main() -> Result<(), Box<dyn Error>>{
    env_logger::builder()
        .filter_level(LevelFilter::Error)
        .init();

    // Preparing data
    let mut args = env::args();
    args.next();

    let pci_addr = match args.next() {
        Some(arg) => arg,
        None => {
            eprintln!("Usage: cargo run --example example_sort_merge <pci bus id> <len>");
            process::exit(1);
        }
    };

    let num_hugepages = match args.next() {
        Some(arg) => arg.parse::<usize>().unwrap(),
        None => {
            eprintln!("Usage: cargo run --example example_sort_merge <pci bus id> <len?>\nNo length provided. Defaulting to 3 1GiB Hugepages.");
            3
        }
    };

    let mut nvme = vroom::init(&pci_addr)?;
    let start = Instant::now();
    nvme = sort_merge(nvme, num_hugepages* HUGE_PAGE_SIZE_1G/8, false)?;
    let duration = start.elapsed();
    println!("Duration: {:?}", duration);

    Ok(())
}

