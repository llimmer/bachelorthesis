use std::cmp::min;
use vroom::{NvmeQueuePair};
use vroom::memory::{Dma, DmaSlice};
use crate::config::{CHUNK_SIZE, HUGE_PAGES, HUGE_PAGE_SIZE, HUGE_PAGE_SIZE_2M, LBA_PER_CHUNK, LBA_SIZE};
use crate::conversion::{u64_to_u8_slice, u8_to_u64_slice};

pub fn setup_array(arr: &mut [u64], qpair: &mut NvmeQueuePair) {
    let mut buffer = Dma::allocate(HUGE_PAGE_SIZE_2M).unwrap();
    let length = arr.len();
    println!("Buffer pointer: {:?}, {:?}", buffer.virt, buffer.phys);
    let u8_arr = u64_to_u8_slice(arr);

    let mut max = u8_arr.len() / HUGE_PAGE_SIZE_2M;
    // write hugepages to disk
    for i in 0..max {
        let slice = &u8_arr[i*HUGE_PAGE_SIZE_2M..min((i+1)*HUGE_PAGE_SIZE_2M, u8_arr.len())];
        buffer[0..slice.len()].copy_from_slice(slice);

        let tmp = qpair.submit_io(&mut buffer.slice(0..slice.len()), (i*HUGE_PAGE_SIZE_2M/LBA_SIZE) as u64, true);
        //println!("Submitting slice {} to {} to lba {}, queue entries: {}", i*HUGE_PAGE_SIZE_2M/8, min((i+1)*HUGE_PAGE_SIZE_2M/8, length), i*HUGE_PAGE_SIZE_2M/LBA_SIZE, tmp);

        qpair.complete_io(tmp);
    }
    if u8_arr.len() % HUGE_PAGE_SIZE_2M != 0 {
        let slice = &u8_arr[max*HUGE_PAGE_SIZE_2M..u8_arr.len()];
        buffer[0..slice.len()].copy_from_slice(slice);
        let zero_buf = [0u8; HUGE_PAGE_SIZE_2M];
        buffer[slice.len()..HUGE_PAGE_SIZE_2M].copy_from_slice(&zero_buf[slice.len()..HUGE_PAGE_SIZE_2M]);
        let tmp = qpair.submit_io(&mut buffer.slice(0..slice.len()), (max*HUGE_PAGE_SIZE_2M/LBA_SIZE) as u64, true);
        qpair.complete_io(tmp);
        assert_eq!(tmp, 1);
    }


}

pub fn clear(chunks: usize, qpair: &mut NvmeQueuePair) {
    let mut buffer = Dma::allocate(HUGE_PAGE_SIZE_2M).unwrap();
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