use std::cmp::max;
use std::ptr::write;
use log::{debug, info};
use vroom::memory::{Dma, DmaSlice};
use vroom::{NvmeQueuePair, HUGE_PAGE_SIZE_2M};
use crate::config::{BLOCKSIZE, K, LBA_SIZE};
use crate::classification::{find_bucket_ips2ra};
use crate::conversion::{u8_to_u64, u8_to_u64_slice};
use crate::sorter::{DMATask, IPS2RaSorter, Task};
use crate::{read_write_hugepage, u64_to_u8_slice, LBA_PER_CHUNK};

impl IPS2RaSorter {
    fn calculate_pointers(&mut self) {
        let mut sum = 0;
        for i in 0..K{
            sum += self.element_counts[i];
            self.boundaries[i+1] = sum;
        }

        // set pointers
        for i in 0..K{
            let start = Self::align_to_next_block(self.boundaries[i] as usize);
            let stop = Self::align_to_next_block(self.boundaries[i+1] as usize);
            self.pointers[i] = (start as i64, {
                if start >= self.classified_elements {
                    start as i64
                } else if stop <= self.classified_elements {
                    stop as i64
                } else {
                    self.classified_elements as i64
                }
            }-BLOCKSIZE as i64)
        }
    }
    pub fn permutate_blocks(&mut self, task: &mut Task) {
        self.calculate_pointers();

        let mut read_bucket = 0;
        let max_off = Self::align_to_next_block(task.arr.len()+1) - BLOCKSIZE;


        for i in 0..K {
            debug!("i={i}");
            let mut dest_bucket: i64;

            while {
                dest_bucket = self.classify_and_read_block(read_bucket, task);
                dest_bucket != -1
            } {
                let mut current_swap: bool = false;
                while {
                    dest_bucket = self.swap_block(max_off, dest_bucket, current_swap, task);
                    dest_bucket != -1
                } {
                    current_swap = !current_swap;
                }
            }
            read_bucket = (read_bucket + 1) % K;
        }
    }

    fn classify_and_read_block(&mut self, bucket: usize, task: &mut Task) -> i64 {
        let (write_ptr, read_ptr) = self.fetch_sub_most_significant(bucket);

        debug!("Classify block {bucket}: write_ptr={write_ptr}, read_ptr={read_ptr}");
        if read_ptr<write_ptr {
            return -1;
        }

        debug!("Copying {:?} (start_index: {read_ptr}) to swap buffer 0", &task.arr[read_ptr as usize..read_ptr as usize + BLOCKSIZE]);
        self.swap_buffer[0].copy_from_slice(&task.arr[read_ptr as usize..read_ptr as usize + BLOCKSIZE]);

        find_bucket_ips2ra(self.swap_buffer[0][0], task.level) as i64
    }

    fn swap_block(&mut self, max_off: usize, dest_bucket: i64, current_swap: bool, task: &mut Task) -> i64 {
        debug!("Swap block: dest_bucket={dest_bucket}, current_swap={current_swap}");
        let mut new_dest_bucket: i64;
        let mut write_ptr: i64 = -1;
        let mut read_ptr: i64 = -1;
        loop {
            (write_ptr, read_ptr) = self.fetch_add_least_significant(dest_bucket as usize);

            if write_ptr > read_ptr {
                if write_ptr >= max_off as i64 {
                    // case overflow
                    self.overflow_buffer.copy_from_slice(&self.swap_buffer[current_swap as usize]);
                    self.overflow = true;
                    return -1;
                }
                debug!("write ptr ({}) > read ptr ({}) && write_ptr > max_off ({})", write_ptr, read_ptr, max_off);

                // Write swap block
                debug!("Writing swap buffer {current_swap} to {:?} (start_index: {write_ptr})", &task.arr[write_ptr as usize..(write_ptr + BLOCKSIZE as i64) as usize]);
                task.arr[write_ptr as usize..(write_ptr + BLOCKSIZE as i64) as usize].copy_from_slice(&self.swap_buffer[current_swap as usize]);
                return -1;
            }
            debug!("Reading new block: {:?} (start_index: {write_ptr})", &task.arr[write_ptr as usize..write_ptr as usize + BLOCKSIZE]);
            new_dest_bucket = find_bucket_ips2ra(task.arr[write_ptr as usize], task.level) as i64;

            if new_dest_bucket != dest_bucket {
                break;
            }
        }
        debug!("Copying {:?} (start_index: {write_ptr}) to swap buffer {}", &task.arr[write_ptr as usize..(write_ptr + BLOCKSIZE as i64) as usize], 1-current_swap as usize);
        self.swap_buffer[1-current_swap as usize].copy_from_slice(&task.arr[write_ptr as usize..(write_ptr + BLOCKSIZE as i64) as usize]);
        debug!("Writing swap buffer {current_swap} to {:?} (start_index: {write_ptr})", &task.arr[write_ptr as usize..(write_ptr + BLOCKSIZE as i64) as usize]);
        task.arr[write_ptr as usize..(write_ptr + BLOCKSIZE as i64) as usize].copy_from_slice(&self.swap_buffer[current_swap as usize]);

        new_dest_bucket
    }

    fn fetch_sub_most_significant(&mut self, bucket: usize) -> (i64, i64){
        let tmp = self.pointers[bucket].1;
        self.pointers[bucket].1 -= BLOCKSIZE as i64;
        (self.pointers[bucket].0, tmp)
    }

    fn fetch_add_least_significant(&mut self, bucket: usize) -> (i64, i64){
        let tmp = self.pointers[bucket].0;
        self.pointers[bucket].0 += BLOCKSIZE as i64;
        (tmp, self.pointers[bucket].1)
    }

    pub fn permutate_blocks_ext(&mut self, task: &mut DMATask){
        self.calculate_pointers();

        debug!("External Sorter before permutation: {:?}", self);

        assert!(self.qpair.is_some(), "Cannot classify_in_out without qpair");
        assert!(self.buffers.is_some(), "Cannot classify_in_out without buffers");

        let buffer = self.buffers.as_mut().unwrap();

        assert!(buffer.len() > 1, "Need at least two buffers for external permutation");

        let mut read_bucket = 0;
        let max_off = Self::align_to_next_block(task.size+1) - BLOCKSIZE;


        for i in 0..K {
            debug!("i={i}");
            let mut dest_bucket: i64;

            while {
                dest_bucket = self.classify_and_read_block_ext(read_bucket, task);
                dest_bucket != -1
            } {
                let mut current_swap: bool = false;
                while {
                    dest_bucket = self.swap_block_ext(max_off, dest_bucket, current_swap, task);
                    dest_bucket != -1
                } {
                    current_swap = !current_swap;
                }
            }
            read_bucket = (read_bucket + 1) % K;
        }
    }

    fn classify_and_read_block_ext(&mut self, bucket: usize, task: &mut DMATask) -> i64 {
        let (write_ptr, read_ptr) = self.fetch_sub_most_significant(bucket);
        let qpair = self.qpair.as_mut().unwrap();
        let buffer = self.buffers.as_mut().unwrap();
        debug!("Classify block {bucket}: write_ptr={write_ptr}, read_ptr={read_ptr}");

        if read_ptr<write_ptr {
            return -1;
        }

        // read from ssd
        let (cur_lba, cur_offset) = calculate_lba_offset(read_ptr as usize, task.start_lba, task.offset);
        read_write_elements(qpair, &mut buffer[0], cur_lba, cur_offset, BLOCKSIZE, false);
        debug!("Copying {:?} (lba: {cur_lba}) to swap buffer 0", &u8_to_u64_slice(&mut buffer[0][cur_offset*8..(cur_offset+BLOCKSIZE)*8]));
        self.swap_buffer[0].copy_from_slice(u8_to_u64_slice(&mut buffer[0][cur_offset*8..(cur_offset+BLOCKSIZE)*8]));

        find_bucket_ips2ra(self.swap_buffer[0][0], task.level) as i64
    }

    fn swap_block_ext(&mut self, max_off: usize, dest_bucket: i64, current_swap: bool, task: &mut DMATask) -> i64 {
        let mut new_dest_bucket: i64;
        let mut write_ptr: i64 = -1;
        let mut read_ptr: i64 = -1;
        let mut cur_lba: usize;
        let mut cur_offset: usize;

        debug!("Swap block: dest_bucket={dest_bucket}, current_swap={current_swap}");
        loop {
            (write_ptr, read_ptr) = self.fetch_add_least_significant(dest_bucket as usize);

            if write_ptr > read_ptr {
                if write_ptr >= max_off as i64 {
                    // case overflow
                    self.overflow_buffer.copy_from_slice(&self.swap_buffer[current_swap as usize]);
                    self.overflow = true;
                    return -1;
                }
                debug!("write ptr ({}) > read ptr ({}) && write_ptr > max_off ({})", write_ptr, read_ptr, max_off);

                (cur_lba, cur_offset) = calculate_lba_offset(write_ptr as usize, task.start_lba, task.offset);
                read_write_elements(self.qpair.as_mut().unwrap(), &mut self.buffers.as_mut().unwrap()[0], cur_lba, cur_offset, BLOCKSIZE, false);
                debug!("1: Writing swap buffer {current_swap} to {:?} (lba: {cur_lba})", u8_to_u64_slice(&mut self.buffers.as_mut().unwrap()[0][cur_offset*8..(cur_offset+BLOCKSIZE)*8]));
                self.buffers.as_mut().unwrap()[0][cur_offset*8..(cur_offset+BLOCKSIZE)*8].copy_from_slice(u64_to_u8_slice(&mut self.swap_buffer[current_swap as usize]));

                // write back to ssd
                read_write_elements(self.qpair.as_mut().unwrap(), &mut self.buffers.as_mut().unwrap()[0], cur_lba, cur_offset, BLOCKSIZE, true);

                return -1;
            }
            // read next block
            (cur_lba, cur_offset) = calculate_lba_offset(write_ptr as usize, task.start_lba, task.offset);
            read_write_elements(self.qpair.as_mut().unwrap(), &mut self.buffers.as_mut().unwrap()[0], cur_lba, cur_offset, BLOCKSIZE, false);
            debug!("Reading new block: {:?} (lba: {cur_lba})", u8_to_u64_slice(&mut self.buffers.as_mut().unwrap()[0][cur_offset*8..(cur_offset+BLOCKSIZE)*8]));
            new_dest_bucket = find_bucket_ips2ra(u8_to_u64(&mut self.buffers.as_mut().unwrap()[0][cur_offset*8..(cur_offset+1)*8]), task.level) as i64;

            if new_dest_bucket != dest_bucket {
                break;
            }
        }
        // copy to swap buffer
        debug!("Copying {:?} (lba: {cur_lba}) to swap buffer {}", &u8_to_u64_slice(&mut self.buffers.as_mut().unwrap()[0][cur_offset*8..(cur_offset+BLOCKSIZE)*8]), 1-current_swap as usize);
        self.swap_buffer[1-current_swap as usize].copy_from_slice(u8_to_u64_slice(&mut self.buffers.as_mut().unwrap()[0][cur_offset*8..(cur_offset+BLOCKSIZE)*8]));
        debug!("Writing swap buffer {current_swap} to {:?} (lba: {cur_lba})", u8_to_u64_slice(&mut self.buffers.as_mut().unwrap()[0][cur_offset*8..(cur_offset+BLOCKSIZE)*8]));
        self.buffers.as_mut().unwrap()[0][cur_offset*8..(cur_offset+BLOCKSIZE)*8].copy_from_slice(u64_to_u8_slice(&mut self.swap_buffer[current_swap as usize]));

        // write back to ssd
        read_write_elements(self.qpair.as_mut().unwrap(), &mut self.buffers.as_mut().unwrap()[0], cur_lba, cur_offset, BLOCKSIZE, true);

        new_dest_bucket
    }


    /*pub fn permutate_blocks_ext_old(&mut self, task: &mut DMATask) {
        self.calculate_pointers();

        debug!("External Sorter before permutation: {:?}", self);

        assert!(self.qpair.is_some(), "Cannot classify_in_out without qpair");
        assert!(self.buffers.is_some(), "Cannot classify_in_out without buffers");

        let qpair = self.qpair.as_mut().unwrap();
        let buffer = self.buffers.as_mut().unwrap();

        assert!(buffer.len() > 1, "Need at least two buffers for external permutation");


        let mut pb: usize = self.primary_bucket;
        let mut swap_buffer = [[0; BLOCKSIZE*8]; 2];
        let mut swap_buffer_idx: usize = 0;
        let mut lba = [0usize; 2];

        // TODO: check if already in correct bucket, think of logic

        'outer: loop {

            // check if block is processed
            if self.pointers[pb].1 < self.pointers[pb].0 {
                pb = (pb+ 1usize) % K;
                // check if cycle is finished
                if pb == self.primary_bucket {
                    break 'outer;
                }
                continue 'outer;
            }

            // decrement read pointers
            self.pointers[pb].1 -= BLOCKSIZE as i64;


            // TODO: check if already in right bucket and read < write, skip in this case
            // TODO: maybe keep track of loaded hugepages, only write/read if not in memory
            // TODO: use buffer directly as swap buffer

            let idx = self.pointers[pb].1 + BLOCKSIZE as i64;
            //let (hugepage, chunk, block) = calculate_hugepage_chunk_block(idx as usize);

            // read chunk
            let (cur_lba, cur_offset) = calculate_lba_offset(idx as usize, task.start_lba, task.offset);
            lba[swap_buffer_idx] = cur_lba;
            read_write_elements(qpair, &mut buffer[swap_buffer_idx], lba[swap_buffer_idx], cur_offset, BLOCKSIZE, false);

            // read block into swap buffer
            swap_buffer[swap_buffer_idx].copy_from_slice(&buffer[swap_buffer_idx][cur_offset*8..(cur_offset+BLOCKSIZE)*8]);

            debug!("Read lba {} into swap buffer {}: {:?}", lba[swap_buffer_idx], swap_buffer_idx, u8_to_u64_slice(&mut swap_buffer[swap_buffer_idx]));

            'inner: loop {
                let first_element = u8_to_u64(&swap_buffer[swap_buffer_idx][0..8]);
                let mut bdest = find_bucket_ips2ra(first_element, task.level) as u64;
                let mut wdest = &mut self.pointers[bdest as usize].0;
                let mut rdest = &mut self.pointers[bdest as usize].1;
                debug!("First element: {}, Bucket: {}, Write: {}, Read: {}", first_element, bdest, wdest, rdest);

                // increment wdest pointers
                *wdest += BLOCKSIZE as i64;

                // read block into second swap buffer
                let next_swap_buffer_idx = (swap_buffer_idx + 1) % 2;
                let next_idx = *wdest as usize - BLOCKSIZE;

                let (next_lba, next_offset) = calculate_lba_offset(next_idx, task.start_lba, task.offset);
                //let (next_hugepage, next_chunk, next_block) = calculate_hugepage_chunk_block(next_idx);

                // read chunk
                lba[next_swap_buffer_idx] = next_lba; // next_hugepage*CHUNKS_PER_HUGE_PAGE_2M*LBA_PER_CHUNK+next_chunk*LBA_PER_CHUNK
                read_write_elements(qpair, &mut buffer[next_swap_buffer_idx], lba[next_swap_buffer_idx], next_offset, BLOCKSIZE, false);

                if *wdest-BLOCKSIZE as i64 <= *rdest {
                    // copy to new swap buffer
                    swap_buffer[next_swap_buffer_idx].copy_from_slice(&buffer[next_swap_buffer_idx][next_offset*8..(next_offset+BLOCKSIZE)*8]);
                    // overwrite with old swap buffer
                    buffer[next_swap_buffer_idx][next_offset*8..(next_offset+BLOCKSIZE)*8].copy_from_slice(&swap_buffer[swap_buffer_idx]);

                    // write back to disk
                    debug!("writing {:?} to lba {}", u8_to_u64_slice(&mut swap_buffer[swap_buffer_idx]), lba[next_swap_buffer_idx]);
                    read_write_elements(qpair, &mut buffer[next_swap_buffer_idx], lba[next_swap_buffer_idx], next_offset, BLOCKSIZE, true);

                    swap_buffer_idx = next_swap_buffer_idx;
                } else {
                    if *wdest > task.size as i64 {
                        // write to overflow buffer
                        debug!("Write to overflow buffer - wdest: {}, tasklen: {}", wdest, task.size);

                        // TODO: debug, remove later
                        assert_eq!(bdest, crate::permutation::compute_overflow_bucket(&self.element_counts) as u64, "Overflow bucket not correct");

                        // TODO: do better
                        let mut overflow_slice = u8_to_u64_slice(&mut swap_buffer[swap_buffer_idx]);
                        debug!("Writing Overflow Buffer: {:?}", overflow_slice);
                        self.overflow_buffer.append(&mut overflow_slice.to_vec());
                        self.overflow = true;
                        break 'inner;
                    }

                    // write swap buffer to new chunk
                    debug!("break writing {:?} to lba {}", u8_to_u64_slice(&mut swap_buffer[swap_buffer_idx]), lba[next_swap_buffer_idx]);

                    buffer[next_swap_buffer_idx][next_offset*8..(next_offset+BLOCKSIZE)*8].copy_from_slice(&swap_buffer[swap_buffer_idx]);
                    read_write_elements(qpair, &mut buffer[next_swap_buffer_idx], lba[next_swap_buffer_idx], next_offset, BLOCKSIZE, true);

                    break 'inner;
                }
            }
        }


    }*/

    pub fn align_to_next_block(index: usize) -> usize {
        index + BLOCKSIZE-1 & !(BLOCKSIZE-1)
    }
}

// read num_elements elements from target_lba (+target_offset elements) to buffer. Wait for completion.
fn read_write_elements(qpair: &mut NvmeQueuePair, buffer: &mut Dma<u8>, target_lba: usize, target_offset: usize, num_elements: usize, write: bool) {
    let num_lba = (target_offset*8 + num_elements*8 + LBA_SIZE - 1) / LBA_SIZE;
    debug!("Reading {} elements (=> {} lbas) from lba {} with offset {} to buffer", num_elements, num_lba, target_lba, target_offset);
    let tmp = qpair.submit_io(&mut buffer.slice(0..num_lba*LBA_SIZE), target_lba as u64, write);
    qpair.complete_io(tmp);
    debug!("Read: {:?}", u8_to_u64_slice(&mut buffer[0..num_lba*LBA_SIZE]));
}

// TODO: include offset from task
pub fn calculate_lba_offset(index: usize, start_lba: usize, task_offset: usize) -> (usize, usize){
    let lba = index*8/LBA_SIZE + start_lba;
    let offset = index % BLOCKSIZE + task_offset;

    debug!("Index: {}, LBA: {}, Offset: {}", index, lba, offset);

    (lba, offset)
}

/*
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(BLOCKSIZE, 4, "BLOCKSIZE 4 required for this test");
        assert_eq!(K, 8, "8 buckets required for this test");

        let mut input = [37, 54, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 36, 35, 56, 62, 58, 60, 34, 33, 32, 50, 20, 29, 28, 27, 26, 25, 24, 23, 15, 18, 17, 16, 10, 13, 12, 11, 57, 59, 61, 55, 5, 9, 8, 7, 6, 4, 3, 2, 1, 14, 22, 21, 19, 30, 31, 51, 52, 53, 63, 64];
        let decision_tree = [29, 13, 54, 9, 18, 31, 62];
        let classified_elements = 52;
        let element_count = [9, 4, 5, 11, 2, 23, 8, 2];
        let mut pointers = [(0, 0); K];
        let mut boundaries = [0; K + 1];
        let mut overflow_buffer: Vec<u64> = vec![];

        let length = input.len();

        //permutate_blocks(&mut input, &decision_tree, classified_elements, &element_count, &mut pointers, &mut boundaries, &mut overflow_buffer, 0, length);

        //debug!("Pointers: {:?}", pointers);
        let expected_pointers = [(8, -4), (16, 12), (20, 16), (28, 20), (32, 28), (52, 48), (64, 48), (64, 48)];
        for i in 0..K {
            assert_eq!(pointers[i], expected_pointers[i]);
        }
    }

}*/

