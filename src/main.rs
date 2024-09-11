#![feature(thread_spawn_unchecked)]

use log::LevelFilter;
use log::{debug, info, warn, error};
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng, thread_rng};
use std::cmp::max;
use std::io;
use std::time::Instant;
use rand::seq::index::sample;
use std::error::Error;
use vroom::memory::{Dma, DmaSlice};
use vroom::QUEUE_LENGTH;

mod sampling;
mod base_case;
mod classification;
mod config;
mod permutation;
mod cleanup;
mod sort;
mod sorter;
mod sequential;
mod parallel;
mod conversion;
mod setup;

use crate::base_case::insertion_sort;
use crate::config::{HUGE_PAGES, HUGE_PAGE_SIZE};
use crate::conversion::{u64_to_u8_slice, u8_to_u64, u8_to_u64_slice};
use crate::permutation::calculate_hugepage_chunk_block;
use crate::setup::{clear, setup_array};
use crate::sort::sort;
use crate::sorter::{DMATask, IPS2RaSorter, IPS2RaSorterDMA, Task};

fn verify_sorted(arr: &Vec<u64>) {
    for i in 1..arr.len() {
        assert!(arr[i - 1] <= arr[i]);
    }
}

fn main() -> Result<(), Box<dyn Error>>{
    env_logger::builder()
        .filter_level(LevelFilter::Error)
        .init();

    let mut nvme = vroom::init("0000:00:04.0")?;
    let mut qpair = nvme.create_io_queue_pair(QUEUE_LENGTH)?;
    let mut buffer: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
    const n: usize = 4096;

    let mut data: Vec<u64> = (0..n as u64).collect();
    let mut rng = StdRng::seed_from_u64(1357);

    data.shuffle(&mut rng);

    clear(2150, &mut qpair);
    setup_array(&mut data, &mut qpair, &mut buffer);

    //let mut input = String::new();
    //io::stdin().read_line(&mut input).expect("Failed to read input");


    let mut sampleTask = Task::new(&mut data, 0);
    sampleTask.sample();
    let mut sorter1 = IPS2RaSorter::new_sequential();
    let mut sorter2 = IPS2RaSorterDMA::new_sequential(qpair);

    let mut DMATask = DMATask::new(0, 0, n, sampleTask.level);
    unsafe {
        sorter1.classify(&mut sampleTask);
        sorter2.classify(&mut DMATask);
    }

    //println!("Sequential sorter classified {} elements", sorter1.classified_elements);
    //println!("Sequential DMA sorter classified {} elements", sorter2.classified_elements);

    //println!("\nArray after sequential classification: {:?}", sampleTask.arr);

    //println!("\nArray after dma classification: {:?}", read_data);

    //assert_eq!(sorter1.classified_elements, sorter2.classified_elements, "Classified elements do not match");
    //assert_eq!(sampleTask.arr[..sorter1.classified_elements], read_data[..sorter1.classified_elements], "Classified arrays do not match");


    let mut input = String::new();
    io::stdin().read_line(&mut input).expect("Failed to read input");

    sorter1.permutate_blocks(&mut sampleTask);
    sorter2.permutate_blocks(&mut DMATask);

    let mut buffer_read: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE*8).unwrap();
    nvme.read(&buffer_read.slice(0..HUGE_PAGE_SIZE*8), 0)?;
    let read_data = u8_to_u64_slice(&mut buffer_read[0..n*8]);

    println!("\nArray after sequential permutating: {:?}", sampleTask.arr);
    println!("\nArray after dma permutating: {:?}", read_data);

    assert_eq!(sampleTask.arr, read_data, "Permutated arrays do not match");




    println!("Done");
    Ok(())
}


