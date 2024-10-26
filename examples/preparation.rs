use std::{env, process};
use log::debug;
use rand::prelude::{SliceRandom, StdRng};
use rand::{Rng, SeedableRng};
use vroom::memory::{Dma, DmaSlice};
use vroom::QUEUE_LENGTH;
use bachelorthesis::{clear_chunks, read_write_hugepage_1G, u64_to_u8_slice, CHUNKS_PER_HUGE_PAGE_1G, CHUNKS_PER_HUGE_PAGE_2M, CHUNK_SIZE, HUGE_PAGES_1G, HUGE_PAGE_SIZE_1G, HUGE_PAGE_SIZE_2M, LBA_PER_CHUNK};

pub fn main() {
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

    let mut nvme = vroom::init(&pci_addr).unwrap();
    let mut qpair = nvme.create_io_queue_pair(QUEUE_LENGTH).unwrap();

    let mut rng = StdRng::seed_from_u64(54321);
    let mut buffer = Dma::allocate(HUGE_PAGE_SIZE_1G).unwrap();

    println!("Clearing chunks");
    clear_chunks((num_hugepages+2)*CHUNKS_PER_HUGE_PAGE_1G, &mut qpair);
    println!("Done");

    for i in 0..num_hugepages{
        println!("Preparing hugepage {}", i);
        //let mut data: Vec<u64> = (0..HUGE_PAGE_SIZE_1G/8).map(|_| rng.gen_range(0..u64::MAX)).collect(); // Random data
        let mut data: Vec<u64> = (0..HUGE_PAGE_SIZE_1G as u64/8).collect(); // Sequential data -> shuffle
        data.shuffle(&mut rng);

        buffer[0..data.len()*8].copy_from_slice(&u64_to_u8_slice(&mut data));
        read_write_hugepage_1G(&mut qpair, i*LBA_PER_CHUNK*CHUNKS_PER_HUGE_PAGE_1G, &mut buffer, true);
    }
    println!("Preparation complete");
}