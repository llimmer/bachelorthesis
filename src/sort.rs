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
use crate::LBA_SIZE;
use crate::sequential_sort_merge::sequential_sort_merge;

pub fn sort(arr: &mut [u64]) {
    let mut task = Task::new(arr, 0, 0, 8);
    if !task.sample(){
        return;
    }
    let mut s = IPS2RaSorter::new_sequential();
    debug!("Task after sampling: {:?}", task.arr);
    info!("Level: {:?}", task.level);
    s.sequential_rec(&mut task);
}

pub fn sort_parallel(arr: &mut [u64]) {
    if NUM_THREADS > 0 {
        rayon::ThreadPoolBuilder::new().num_threads(NUM_THREADS).build_global().unwrap();
    }
    let mut initial_task = Task::new(arr, 0, 0, 0);
    if !initial_task.sample(){
        return;
    }
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
        let mut task = DMATask::new(0, 0, len, 6, 6, 8);
        sorter.sequential_rolling_sort(&mut task);
    } else {
        unimplemented!();
    }

    Ok(nvme)
}

pub fn read_write_elements(qpair: &mut NvmeQueuePair, buffer: &mut Dma<u8>, target_lba: usize, target_offset: usize, num_elements: usize, write: bool) {
    //println!("starting read_write_elements");
    let num_lba = (target_offset*8 + num_elements*8 + LBA_SIZE - 1) / LBA_SIZE;

    let mut remaining_chunks = num_lba / LBA_PER_CHUNK;
    let remaining_lba = num_lba % LBA_PER_CHUNK;
    let max_lba_per_queue = QUEUE_LENGTH*LBA_PER_CHUNK;

    //println!("Qpair at start: {}", qpair.sub_queue.is_empty());


    assert!(buffer.size >= num_lba*LBA_SIZE/8, "Buffer size too small");

    if num_lba < max_lba_per_queue{
        let tmp = qpair.submit_io(&mut buffer.slice(0..num_lba*LBA_SIZE), target_lba as u64, write);
        qpair.complete_io(tmp);
    } else {
        // request/write max_lba_per_queue lbas
        let mut sum = 0;
        for i in 0..max_lba_per_queue/LBA_PER_CHUNK {
            let tmp = qpair.submit_io(&mut buffer.slice(i*CHUNK_SIZE..(i+1)*CHUNK_SIZE), (i*LBA_PER_CHUNK + target_offset) as u64, write);
            assert_eq!(tmp, 1);
            sum += tmp;
            if qpair.sub_queue.is_full(){
                println!("Queue full after {} requests", sum);
                break;
            }
        }
        remaining_chunks -= sum;

        for i in 0..remaining_chunks {
            qpair.complete_io(1);
            let tmp = qpair.submit_io(&mut buffer.slice((i+sum)*CHUNK_SIZE..(i+1+sum)*CHUNK_SIZE),  (i*LBA_PER_CHUNK + target_offset + sum*LBA_PER_CHUNK) as u64, write);
            assert_eq!(tmp, 1);
        }

        for i in 0..remaining_lba {
            qpair.complete_io(1);
            let tmp = qpair.submit_io(&mut buffer.slice((i+sum+remaining_chunks)*CHUNK_SIZE..(i+1+sum+remaining_chunks)*CHUNK_SIZE + LBA_SIZE),  (i + target_offset + (sum+remaining_chunks)*LBA_PER_CHUNK) as u64, write);
            assert_eq!(tmp, 1);
        }

        qpair.complete_io(sum);
    }

}

pub fn read_write_hugepage_1G(qpair: &mut NvmeQueuePair, lba_offset: usize, segment: &mut Dma<u8>, write: bool){
    read_write_elements(qpair, segment, lba_offset, 0, HUGE_PAGE_SIZE_1G/8, write);
}

pub fn read_write_hugepage_2M(qpair: &mut NvmeQueuePair, lba_offset: usize, segment: &mut Dma<u8>, write: bool){
    read_write_elements(qpair, segment, lba_offset, 0, HUGE_PAGE_SIZE_2M/8, write);
}

impl IPS2RaSorter{
    pub fn read_write_sort_buffer_1G(&mut self, lba_offset: usize, write: bool){
        assert!(self.qpair.is_some(), "Queue pair not initialized");
        assert!(self.sort_buffer.is_some(), "Sort buffer not initialized");
        let mut qpair = self.qpair.as_mut().unwrap();
        let mut sort_buffer = self.sort_buffer.as_mut().unwrap();
        read_write_elements(qpair, sort_buffer, lba_offset, 0, HUGE_PAGE_SIZE_1G/8, write);
    }

    pub fn read_write_sort_buffer_2M(&mut self, lba_offset: usize, write: bool){
        assert!(self.qpair.is_some(), "Queue pair not initialized");
        assert!(self.sort_buffer.is_some(), "Sort buffer not initialized");
        let mut qpair = self.qpair.as_mut().unwrap();
        let mut sort_buffer = self.sort_buffer.as_mut().unwrap();
        read_write_elements(qpair, sort_buffer, lba_offset, 0, HUGE_PAGE_SIZE_2M/8, write);
    }
}