use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize};
use std::{io, thread};
use log::{debug, error, info};
use crate::config::{BLOCKSIZE, CHUNKS_PER_HUGE_PAGE_1G, CHUNK_SIZE, HUGE_PAGES_1G, HUGE_PAGE_SIZE_1G, K, LBA_PER_CHUNK, NUM_THREADS, THRESHOLD, HUGE_PAGES_2M, HUGE_PAGE_SIZE_2M};
use crate::parallel::process_task;
use crate::sorter::{DMATask, IPS2RaSorter, Task};
use crate::setup::{clear_chunks, setup_array};
use std::error::Error;
use rand::prelude::{SliceRandom, StdRng};
use rand::SeedableRng;
use vroom::memory::{Dma, DmaSlice};
use vroom::{NvmeDevice, NvmeQueuePair, QUEUE_LENGTH};
use crate::conversion::u8_to_u64_slice;
use crate::sort_merge::sequential_sort_merge;

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

pub fn sort_merge(mut nvme: NvmeDevice, len: usize, parallel: bool) -> Result<NvmeDevice, Box<dyn Error>>{
    if !parallel {
        sequential_sort_merge(nvme, len)
    } else {
        unimplemented!();
    }
}


pub fn rolling_sort(mut nvme: NvmeDevice, len: usize, parallel: bool) -> Result<NvmeDevice, Box<dyn Error>> {
    if(!parallel){
        let mut qpair = nvme.create_io_queue_pair(QUEUE_LENGTH)?;
        let mut sort_buffer = Dma::allocate(HUGE_PAGE_SIZE_1G)?;

        let mut buffers: Vec<Dma<u8>> = Vec::new();
        for _ in 0..HUGE_PAGES_2M {
            buffers.push(Dma::allocate(HUGE_PAGE_SIZE_2M)?);
        }

        let mut sorter = IPS2RaSorter::new_ext_sequential(qpair, buffers, sort_buffer);
        let mut task = DMATask::new(0, 0, len, 0);
        sorter.sequential_rolling_sort(&mut task);
    } else {
        unimplemented!();
    }

    Ok(nvme)
}

pub fn read_write_hugepage(qpair: &mut NvmeQueuePair, lba_offset: usize, segment: &mut Dma<u8>, write: bool){
    let max_chunks_per_queue = QUEUE_LENGTH/8;
    let chunks_per_segment = HUGE_PAGE_SIZE_1G /CHUNK_SIZE;

    //println!("Hugepage Size: {}, Max chunks per hugepage: {}, chunks_per_hugepage: {}, offset: {}", HUGE_PAGE_SIZE, max_chunks_per_queue, chunks_per_segment, offset);
    if chunks_per_segment <= max_chunks_per_queue {
        for i in 0..chunks_per_segment {
            let tmp = qpair.submit_io(&mut segment.slice(i*CHUNK_SIZE..(i+1)*CHUNK_SIZE), (i*LBA_PER_CHUNK + lba_offset) as u64, write);
            //println!("Requesting lba {} to chunk {}, SQEs: {}", (LBA_PER_CHUNK*CHUNKS_PER_HUGE_PAGE*offset), i, tmp);
        }
        qpair.complete_io(chunks_per_segment);
    } else {
        // request max_chunks_per_queue chunks
        for i in 0..max_chunks_per_queue {
            //println!("Requesting lba {} to chunk {}", (LBA_PER_CHUNK*CHUNKS_PER_HUGE_PAGE*offset), i);
            qpair.submit_io(&mut segment.slice(i*CHUNK_SIZE..(i+1)*CHUNK_SIZE), (i*LBA_PER_CHUNK + lba_offset) as u64, write);
        }
        //println!("////////////////////////////////////////////");
        for i in max_chunks_per_queue..chunks_per_segment {
            qpair.complete_io(1);
            qpair.submit_io(&mut segment.slice(i*CHUNK_SIZE..(i+1)*CHUNK_SIZE),  (i*LBA_PER_CHUNK + lba_offset) as u64, write);
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
