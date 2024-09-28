use bachelorthesis::{HUGE_PAGE_SIZE_1G, sort_merge};
use std::{env, process};
use std::error::Error;
use log::LevelFilter;

fn main() -> Result<(), Box<dyn Error>>{
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .init();
    env::set_var("RUST_BACKTRACE", "1");

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
            eprintln!("Usage: cargo run --example example_sort_merge <pci bus id> <num_hugepages?>\nNo length provided. Defaulting to 3 1GiB Hugepages.");
            3
        }
    };

    let len = num_hugepages * HUGE_PAGE_SIZE_1G/8;

    let mut nvme = vroom::init(&pci_addr)?;
    nvme = sort_merge(nvme, len, true)?;

    Ok(())
}

