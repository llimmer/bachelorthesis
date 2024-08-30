use std::cmp::min;
use vroom::{NvmeDevice, NvmeQueuePair};
use vroom::memory::{Dma, DmaSlice};
use crate::config::{CHUNK_SIZE, HUGE_PAGES, HUGE_PAGE_SIZE, LBA_PER_CHUNK, LBA_SIZE};
use crate::conversion::{u64_to_u8_slice, u8_to_u64_slice};

pub fn setup_array(arr: &mut [u64], qpair: &mut NvmeQueuePair, buffer: &mut Dma<u8>) {
    let length = arr.len();
    println!("Buffer pointer: {:?}, {:?}", buffer.virt, buffer.phys);
    let u8_arr = u64_to_u8_slice(arr);

    let mut max = (u8_arr.len() / HUGE_PAGE_SIZE);
    if u8_arr.len() % HUGE_PAGE_SIZE != 0 {
        max += 1;
    }

    // write hugepages to disk
    for i in 0..max {
        let slice = &u8_arr[i*HUGE_PAGE_SIZE..min((i+1)*HUGE_PAGE_SIZE, u8_arr.len())];
        buffer[0..slice.len()].copy_from_slice(slice);

        let tmp = qpair.submit_io(&mut buffer.slice(0..slice.len()), (i*HUGE_PAGE_SIZE/LBA_SIZE) as u64, true);
        println!("Submitting slice {} to {} to lba {}, queue entries: {}", i*HUGE_PAGE_SIZE/8, min((i+1)*HUGE_PAGE_SIZE/8, length), i*HUGE_PAGE_SIZE/LBA_SIZE, tmp);

        qpair.complete_io(tmp);
    }

}

pub fn clear(chunks: usize, qpair: &mut NvmeQueuePair) {
    let mut buffer = Dma::allocate(HUGE_PAGE_SIZE).unwrap();
    let tmp = [0; LBA_SIZE*LBA_PER_CHUNK];
    buffer[0..tmp.len()].copy_from_slice(&tmp);
    for i in 0..chunks  {
        qpair.submit_io(&buffer.slice(0..tmp.len()), (i*LBA_PER_CHUNK) as u64, true);
        if i != 0 {
             qpair.complete_io(1);
        }
    }
    qpair.complete_io(1);


}