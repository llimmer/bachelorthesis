use std::{env, process};
use std::error::Error;

use vroom::QUEUE_LENGTH;
use bachelorthesis::{rolling_sort, HUGE_PAGE_SIZE_1G};

fn main() -> Result<(), Box<dyn Error>>{
    let mut args = env::args();
    args.next();

    let pci_addr = match args.next() {
        Some(arg) => arg,
        None => {
            eprintln!("Usage: cargo run --example example_sort_merge <pci bus id> <len?>");
            process::exit(1);
        }
    };

    let len = match args.next() {
        Some(arg) => arg.parse::<usize>().unwrap(),
        None => {
            eprintln!("Usage: cargo run --example example_sort_merge <pci bus id> <len?>\nNo length provided. Defaulting to 3 1GiB Hugepages.");
            3 * HUGE_PAGE_SIZE_1G/8
        }
    };

    let mut nvme = vroom::init(&pci_addr)?;
    nvme = rolling_sort(nvme, len, false)?;

    Ok(())
}

