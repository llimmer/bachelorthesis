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
use bachelorthesis::sort::read_write_hugepage;
use bachelorthesis::{sort_merge, K, LBA_SIZE};

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
mod sort_merge;
mod rolling_sort;

use crate::base_case::insertion_sort;
use crate::config::{CHUNKS_PER_HUGE_PAGE_1G, CHUNK_SIZE, ELEMENTS_PER_CHUNK, HUGE_PAGES_1G, HUGE_PAGE_SIZE_1G, HUGE_PAGE_SIZE_2M, LBA_PER_CHUNK};
use crate::conversion::{u64_to_u8_slice, u8_to_u64, u8_to_u64_slice};
use crate::merge::merge_sequential;
use crate::setup::{clear_chunks, setup_array};
use crate::sort::{sort, sort_parallel};
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

    println!("Hello, world!");

    let mut data: Vec<u64> = (0..134217728*4).collect();
    let mut rng = StdRng::seed_from_u64(12345);
    let mut sorter = IPS2RaSorter::new_sequential();

    for i in 0..10000 {
        println!("\ni = {}", i);
        data.shuffle(&mut rng);
        let mut data_copy = data.clone();

        let start = Instant::now();
        let mut task = Task::new(&mut data, 0);
        task.sample();
        sorter.sort_sequential(&mut task);
        sorter.clear();
        let duration = start.elapsed();
        println!("Time elapsed in sorting hugepage {i} is: {:?}", duration);

        let start = Instant::now();
        data_copy.sort_unstable();
        let duration = start.elapsed();
        println!("Time elapsed in quicksorting hugepage {i} is: {:?}", duration);
    }

    /*let mut nvme = vroom::init("0000:00:04.0")?;
    let mut qpair = nvme.create_io_queue_pair(QUEUE_LENGTH)?;

    clear_chunks(10, &mut qpair);

    let mut sorter_qpair = nvme.create_io_queue_pair(QUEUE_LENGTH)?;
    let mut buffer_large: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE_1G)?;

    println!("Buffer size: {}", buffer_large.size);


    let mut buffers_small = Vec::new();
    buffers_small.push(Dma::allocate(HUGE_PAGE_SIZE_2M)?);
    buffers_small.push(Dma::allocate(HUGE_PAGE_SIZE_2M)?);
    buffers_small.push(Dma::allocate(HUGE_PAGE_SIZE_2M)?);
    buffers_small.push(Dma::allocate(HUGE_PAGE_SIZE_2M)?);
    let mut tmp = Dma::allocate(HUGE_PAGE_SIZE_2M)?;

    let mut sorter = IPS2RaSorter::new_sequential();
    let mut ext_sorter = IPS2RaSorter::new_ext_sequential(sorter_qpair, buffers_small, tmp);

    let len: usize = 9234678;
    const offset: usize = 65;

    let mut data: Vec<u64> = vec![0u64; offset];
    let mut data2: Vec<u64> = (1..=len as u64).collect();
    let mut rng = StdRng::seed_from_u64(12345);
    data2.shuffle(&mut rng);

    for i in 0..len {
        data.push(data2[i]);
    }

    //println!("Data: {:?}", data);



    // prepare data on ssd
    buffer_large[0..(len+offset)*8].copy_from_slice(u64_to_u8_slice(&mut data));
    read_write_hugepage(&mut qpair, 0, &mut buffer_large, true);


    // remove offset from original data
    data = data[offset..].to_vec();

    let mut task = Task::new(&mut data, 0);
    task.sample();
    let mut dma_task = DMATask::new(offset*8/LBA_SIZE, offset%(LBA_SIZE/8), len, task.level);

    sorter.classify(&mut task);
    ext_sorter.classify_ext(&mut dma_task);

    read_write_hugepage(&mut qpair, 0, &mut buffer_large, false);

    //println!("Array after classification: {:?}", task.arr);
    //println!("External array after classification: {:?}", u8_to_u64_slice(&mut buffer_large[0..(len*8)]));

    println!("Classified elements: {}, external = {}", sorter.classified_elements, ext_sorter.classified_elements);

    let slice = u8_to_u64_slice(&mut buffer_large[offset*8..(len+offset)*8]);
    for i in 0..len {
        if task.arr[i] != slice[i] {
            println!("Difference after classification at i = {}, task = {}, res = {}", i, task.arr[i], slice[i]);
            return Ok(());
        }
    }

    assert_eq!(sorter.classified_elements, ext_sorter.classified_elements, "Classified elements not equal");
    assert_eq!(task.arr[0..sorter.classified_elements], u8_to_u64_slice(&mut buffer_large[offset*8..(len+offset)*8])[0..ext_sorter.classified_elements], "Data not classified correctly");

    assert_eq!(task.arr, u8_to_u64_slice(&mut buffer_large[offset*8..(len+offset)*8]), "Data classified correctly but arr[classified_elements..] not equal. Further testing not possible");


    sorter.permutate_blocks(&mut task);
    ext_sorter.permutate_blocks_ext(&mut dma_task);


    read_write_hugepage(&mut qpair, 0, &mut buffer_large, false);
    assert_eq!(task.arr, u8_to_u64_slice(&mut buffer_large[offset*8..(len+offset)*8]), "Data not permutated correctly");

    println!("Overflows: {:?}, external = {:?}", sorter.overflow_buffer, ext_sorter.overflow_buffer);
    //println!("Array after permutation: {:?}", task.arr);
    //println!("SSD after permutation: {:?}", u8_to_u64_slice(&mut buffer_large[0..(len+offset)*8]));

    //println!("Sorter struct before cleanup: {:?}", sorter);
    //println!("External sorter struct before cleanup: {:?}", ext_sorter);

    //println!("Data before cleanup: {:?}", task.arr);
    //println!("External data before cleanup: {:?}", u8_to_u64_slice(&mut buffer_large[offset*8..(len+offset)*8]));

    sorter.cleanup(&mut task);
    ext_sorter.cleanup_ext(&mut dma_task);

    read_write_hugepage(&mut qpair, 0, &mut buffer_large, false);
    let res = u8_to_u64_slice(&mut buffer_large[0..(len+offset)*8]);

    //println!("Result: {:?}", task.arr);
    //println!("External Result: {:?}", res);
    for i in 0..len {
        if task.arr[i] != res[i+offset] {
            println!("Difference after cleanup at i = {}, task = {}, res = {}", i, task.arr[i], res[i]);
            return Ok(());
        }
    }

    println!("Sorting done.");

    return Ok(());*/

    /*for i in 0..100000 {
        let len: u64 = 8192+i;//8192+1024;//;
        println!("i = {}", i);
        sorter.clear();
        ext_sorter.clear();
        let mut data: Vec<u64> = (1..=len).collect();
        let mut rng = StdRng::seed_from_u64(i);
        data.shuffle(&mut rng);

        // write data to ssd
        buffer_large[0..(len * 8) as usize].copy_from_slice(u64_to_u8_slice(&mut data));
        read_write_hugepage(&mut qpair, 0, &mut buffer_large, true);

        let mut task = Task::new(&mut data, 0);
        task.sample();
        let mut dma_task = DMATask::new(0, 0, len as usize, task.level);

        println!("Starting classification");
        sorter.classify(&mut task);
        println!("Done\nStarting external classification");
        ext_sorter.classify_ext(&mut dma_task);
        println!("Done");

        // read to check if data is classified correctly
        read_write_hugepage(&mut qpair, 0, &mut buffer_large, false);

        println!("Classified elements: {}, external = {}", sorter.classified_elements, ext_sorter.classified_elements);
        assert_eq!(task.arr, u8_to_u64_slice(&mut buffer_large[0..(len * 8) as usize]), "Data not classified correctly");
        //println!("Data after classification: {:?}", task.arr);
        //println!("Data after external classification: {:?}", u8_to_u64_slice(&mut buffer_large[0..(len*8) as usize]));


        // permutation
        sorter.permutate_blocks(&mut task);
        ext_sorter.permutate_blocks_ext(&mut dma_task);

        // read to check if data is permutated correctly
        read_write_hugepage(&mut qpair, 0, &mut buffer_large, false);

        assert_eq!(task.arr, u8_to_u64_slice(&mut buffer_large[0..(len * 8) as usize]), "Data not permutated correctly");
        println!("Overflows: {:?}, external = {:?}", sorter.overflow_buffer, ext_sorter.overflow_buffer);
    }*/








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


