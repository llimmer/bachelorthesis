use std::{env, process};
use std::error::Error;
use rand::prelude::{SliceRandom, StdRng};
use rand::SeedableRng;
use vroom::memory::HUGE_PAGE_SIZE_1G;
use vroom::QUEUE_LENGTH;
use bachelorthesis::{clear_chunks, setup_array, sort_merge, CHUNKS_PER_HUGE_PAGE_1G};

fn main() -> Result<(), Box<dyn Error>>{

    // Preparing data
    let mut args = env::args();
    args.next();

    let pci_addr = match args.next() {
        Some(arg) => arg,
        None => {
            eprintln!("Usage: cargo run --example hello_world <pci bus id>");
            process::exit(1);
        }
    };
    let mut nvme = vroom::init(&pci_addr)?;
    let mut qpair = nvme.create_io_queue_pair(QUEUE_LENGTH)?;

    let len: usize = 2*HUGE_PAGE_SIZE_1G/8;
    let mut data: Vec<u64> = (1..=len as u64).collect();
    let mut rng = StdRng::seed_from_u64(12345);
    data.shuffle(&mut rng);

    clear_chunks(CHUNKS_PER_HUGE_PAGE_1G*2, &mut qpair);
    setup_array(&mut data, &mut qpair);

    println!("Preparation complete");

    // read line from stdin TODO: remove
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();

    nvme = sort_merge(nvme, len, false);
    Ok(())
}

