use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize};
use std::{io, thread};
use log::{debug, error, info};
use crate::config::{BLOCKSIZE, CHUNKS_PER_HUGE_PAGE, CHUNK_SIZE, HUGE_PAGE_SIZE, K, LBA_PER_CHUNK, NUM_THREADS, THRESHOLD};
use crate::parallel::process_task;
use crate::sorter::{IPS2RaSorter, Task};
use crate::setup::{clear, setup_array};
use std::error::Error;
use rand::prelude::{SliceRandom, StdRng};
use rand::SeedableRng;
use vroom::memory::{Dma, DmaSlice};
use vroom::{NvmeQueuePair, QUEUE_LENGTH};
use crate::conversion::u8_to_u64_slice;

pub fn sort(arr: &mut [u64]) {
    let mut s = IPS2RaSorter::new_sequential();
    let mut task = Task::new(arr, 0);
    task.sample();
    debug!("Task after sampling: {:?}", task.arr);
    info!("Level: {:?}", task.level);
    s.sort_sequential(&mut task);
}
pub fn sort_parallel(arr: &mut [u64]) {
    if NUM_THREADS > 0 {
        rayon::ThreadPoolBuilder::new().num_threads(NUM_THREADS).build_global().unwrap();
    }
    let mut initial_task = Task::new(arr, 0);
    initial_task.sample();
    process_task(&mut initial_task);
}


pub fn sort_dma(pci_addr: &str, len: usize, parallel: bool) -> Result<(), Box<dyn Error>>{
    let mut nvme = vroom::init(pci_addr)?;
    let mut qpair = nvme.create_io_queue_pair(QUEUE_LENGTH)?;

    // Testing
    /*let mut zero_buffer = Dma::allocate(HUGE_PAGE_SIZE_2M)?;
    let tmp = [1; 8192];
    zero_buffer[0..tmp.len()].copy_from_slice(&tmp);
    let num = qpair.submit_io(&mut zero_buffer.slice(0..tmp.len()), (LBA_PER_CHUNK*CHUNKS_PER_HUGE_PAGE) as u64, true);
    qpair.complete_io(1);
    println!("Submitted zero buffer to lba {}, queue entries: {}", LBA_PER_CHUNK*CHUNKS_PER_HUGE_PAGE, num);

    return Ok(());*/




    // Prepare data: //todo: remove
    println!("Clearing hugepages");
    clear(CHUNKS_PER_HUGE_PAGE*2+10, &mut qpair);
    println!("Done");
    let len = 134217728*2+3;

    println!("Number of hugepages to sort: {}", (len+HUGE_PAGE_SIZE/8-1)/(HUGE_PAGE_SIZE/8));

    println!("Generating data");
    let mut data: Vec<u64> = (1..=len as u64).collect();
    let mut rng = StdRng::seed_from_u64(12345);
    data.shuffle(&mut rng);
    println!("Done");

    println!("Setting up arrray");
    setup_array(&mut data, &mut qpair);
    println!("Done");

    // read line from stdin
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();

    if !parallel {
        let mut sorter = IPS2RaSorter::new_sequential();
        let mut buffer = Dma::allocate(HUGE_PAGE_SIZE)?;
        let mut remaining = len;
        for i in 0..((len+HUGE_PAGE_SIZE/8-1)/(HUGE_PAGE_SIZE/8)){
            // read hugepage from ssd
            println!("Reading hugepage {i}");
            read_write_hugepage(&mut qpair, i, &mut buffer, false);
            println!("Done");

            let u64slice = u8_to_u64_slice(&mut buffer[0..{
                if remaining > HUGE_PAGE_SIZE/8{
                    remaining = remaining - HUGE_PAGE_SIZE/8;
                    HUGE_PAGE_SIZE
                } else {
                    let res = remaining;
                    remaining = 0;
                    res*8
                }
            }]);
            println!("Creating and sampling task of length {}", u64slice.len());
            let mut task = Task::new(u64slice, 0);
            task.sample();
            println!("Done");
            println!("Sorting hugepage {i}");
            sorter.sort_sequential(&mut task);
            println!("Done");
            println!("Writing hugepage {i}");
            read_write_hugepage(&mut qpair, i, &mut buffer, true);
            println!("Done");
        }
    }

    return Ok(());
}

pub fn read_write_hugepage(qpair: &mut NvmeQueuePair, offset: usize, segment: &mut Dma<u8>, write: bool){
    let max_chunks_per_queue = QUEUE_LENGTH/8;
    let chunks_per_segment = HUGE_PAGE_SIZE/CHUNK_SIZE;

    println!("Hugepage Size: {}, Max chunks per hugepage: {}, chunks_per_hugepage: {}", HUGE_PAGE_SIZE, max_chunks_per_queue, chunks_per_segment);
    if chunks_per_segment <= max_chunks_per_queue {
        for i in 0..chunks_per_segment {
            //println!("Requesting lba {} to chunk {}", i*LBA_PER_CHUNK, i);
            qpair.submit_io(&mut segment.slice(i*CHUNK_SIZE..(i+1)*CHUNK_SIZE), (LBA_PER_CHUNK*CHUNKS_PER_HUGE_PAGE*offset) as u64 + (i*LBA_PER_CHUNK) as u64, write);
        }
        qpair.complete_io(chunks_per_segment);
    } else {
        // request max_chunks_per_queue chunks
        for i in 0..max_chunks_per_queue {
            //println!("Requesting lba {} to chunk {}", i*LBA_PER_CHUNK, i);
            qpair.submit_io(&mut segment.slice(i*CHUNK_SIZE..(i+1)*CHUNK_SIZE), (LBA_PER_CHUNK*CHUNKS_PER_HUGE_PAGE*offset) as u64 + (i*LBA_PER_CHUNK) as u64, write);
        }
        //println!("////////////////////////////////////////////");
        for i in max_chunks_per_queue..chunks_per_segment {
            qpair.complete_io(1);
            qpair.submit_io(&mut segment.slice(i*CHUNK_SIZE..(i+1)*CHUNK_SIZE), (LBA_PER_CHUNK*CHUNKS_PER_HUGE_PAGE*offset) as u64 + (i*LBA_PER_CHUNK) as u64, write);
        }
        // wait for remaining chunks
        qpair.complete_io(max_chunks_per_queue);
    }
}

#[cfg(test)]
mod tests {
    use rand::prelude::StdRng;
    use rand::{Rng, SeedableRng};
    use super::*;

    #[test]
    fn small_sequential() {
        let mut rng = StdRng::seed_from_u64(12345);
        let n = rng.gen_range(512..1024);
        let mut arr: Vec<u64> = (0..n).map(|_| rng.gen_range(0..u64::MAX)).collect();

        sort(&mut arr);
        for i in 1..arr.len() {
            assert!(arr[i - 1] <= arr[i]);
        }
    }

    #[test]
    fn big_sequential() {
        let mut rng = StdRng::seed_from_u64(12345);
        for _ in 0..1024 {
            let n = rng.gen_range(512..1024);
            let mut arr: Vec<u64> = (0..n).map(|_| rng.gen_range(0..u64::MAX)).collect();

            sort(&mut arr);
            for i in 1..arr.len() {
                assert!(arr[i - 1] <= arr[i]);
            }
        }
    }
}
