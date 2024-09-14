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
mod merge;

use crate::base_case::insertion_sort;
use crate::config::{CHUNKS_PER_HUGE_PAGE, CHUNK_SIZE, HUGE_PAGES, HUGE_PAGE_SIZE, LBA_PER_CHUNK};
use crate::conversion::{u64_to_u8_slice, u8_to_u64, u8_to_u64_slice};
use crate::merge::merge_sequential;
use crate::permutation::calculate_hugepage_chunk_block;
use crate::setup::{clear, setup_array};
use crate::sort::{sort, sort_dma, sort_parallel};
use crate::sorter::{DMATask, IPS2RaSorter, Task};

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
    let mut buffer = Dma::allocate(HUGE_PAGE_SIZE)?;
    // Prepare data: //todo: remove
    println!("Clearing hugepages");
    clear(CHUNKS_PER_HUGE_PAGE*1024+10, &mut qpair);
    println!("Done");
    // prepare first 4 hugepages
    let len = HUGE_PAGE_SIZE/8;
    let total_length = len*5 - 1000;
    let number_cunks = (total_length+len-1)/len;
    for i in 0..number_cunks-1 {
        let mut data: Vec<u64> = (0..len as u64).map(|x| x*number_cunks as u64+(i) as u64).collect();
        buffer[0..HUGE_PAGE_SIZE].copy_from_slice(u64_to_u8_slice(&mut data));
        let tmp = qpair.submit_io(&mut buffer.slice(0..HUGE_PAGE_SIZE), (i*LBA_PER_CHUNK*CHUNKS_PER_HUGE_PAGE) as u64, true);
        qpair.complete_io(tmp);
        println!("Input {i}: {:?}", data);
        //assert_eq!(tmp, 256);
    }
    // prepare last hugepage
    let mut data: Vec<u64> = (0..(total_length%len) as u64).map(|x| x*number_cunks as u64+(number_cunks-1) as u64).collect();
    buffer[0..HUGE_PAGE_SIZE].copy_from_slice(&[0u8; HUGE_PAGE_SIZE]);
    buffer[0..data.len()*8].copy_from_slice(u64_to_u8_slice(&mut data));
    let tmp = qpair.submit_io(&mut buffer.slice(0..data.len()*8), ((number_cunks-1)*LBA_PER_CHUNK*CHUNKS_PER_HUGE_PAGE) as u64, true);
    qpair.complete_io(tmp);
    println!("Input {}: {:?}", number_cunks-1, data);

    let mut buffers: Vec<Dma<u8>> = Vec::with_capacity(HUGE_PAGES - 1);
    for i in 0..HUGE_PAGES - 1 {
        buffers.push(Dma::allocate(HUGE_PAGE_SIZE)?);
    }
    let mut output_buffer = Dma::allocate(HUGE_PAGE_SIZE)?;
    merge_sequential(total_length, &mut qpair, &mut buffers, &mut output_buffer);



    let mut big_hugepage: Dma<u8> = Dma::allocate(1024*1024*1024)?;
    // read first len*number_chunks elements
    let bytes_to_read = total_length*8;
    for i in 0..(bytes_to_read+CHUNK_SIZE-1)/CHUNK_SIZE{
        let tmp = qpair.submit_io(&mut big_hugepage.slice(i*CHUNK_SIZE..(i+1)*CHUNK_SIZE), (i*LBA_PER_CHUNK) as u64, false);
        qpair.complete_io(tmp);
    }
    let slice = u8_to_u64_slice(&mut big_hugepage[0..bytes_to_read]);
    println!("\n\nResult: {:?}", slice);

    for i in 1..slice.len() {
        if slice[i-1] == slice[i] {
            println!("Duplicate elements at {} and {}", i-1, i);
            break;
        }
        if i == slice.len()-1 {
            println!("All elements are correct");
        }
    }





    //sort_dma("0000:00:04.0", 0, false)?;

    /*let mut data: Vec<u64> = (1..=300_000_000u64).collect();
    let mut rng = StdRng::seed_from_u64(12345);
    data.shuffle(&mut rng);
    let mut data2 = data.clone();
    let mut data3 = data.clone();

    // Sequential
    let start = Instant::now();
    sort(&mut data);
    let duration = start.elapsed();
    println!("Sequential: {:?}", duration);

    // Parallel
    let start = Instant::now();
    sort_parallel(&mut data2);
    let duration = start.elapsed();
    println!("Parallel: {:?}", duration);

    // Quicksort
    let start = Instant::now();
    data3.sort_unstable();
    let duration = start.elapsed();
    println!("Quicksort: {:?}", duration);*/

    Ok(())
}


