use std::cmp::max;
use log::{debug, info};
use vroom::memory::{Dma, DmaSlice};
use vroom::NvmeQueuePair;
use crate::config::{BLOCKSIZE, K, LBA_SIZE};
use crate::classification::{find_bucket_ips2ra};
use crate::conversion::{u8_to_u64, u8_to_u64_slice};
use crate::sorter::{DMATask, IPS2RaSorter, Task};

impl IPS2RaSorter {
    fn calculate_pointers(&mut self, length: usize) {
        self.boundaries[0] = 0;
        self.pointers[0].0 = 0;
        let mut sum = 0;

        for i in 0..K-1 {
            // round up to next block
            sum += self.element_counts[i];

            let mut tmp = sum;
            if tmp % BLOCKSIZE as u64 != 0 {
                tmp += BLOCKSIZE as u64 - (sum % BLOCKSIZE as u64);
            }
            self.boundaries[i+1] = {
                if tmp <= length as u64 {
                    tmp
                } else {
                    length as u64
                }
            };
            self.pointers[i + 1].0 = tmp as i64;

            if sum <= self.classified_elements as u64 {
                self.pointers[i].1 = (tmp as i64 - BLOCKSIZE as i64);
                //pointers[i].1 = from as i64 + (tmp-BLOCKSIZE as u64) as i64;
            } else {
                self.pointers[i].1 = (self.classified_elements as i64 - BLOCKSIZE as i64 - (self.classified_elements % BLOCKSIZE) as i64);
                //pointers[i].1 = from as i64 + (classified_elements - BLOCKSIZE - classified_elements%BLOCKSIZE) as i64;
            }
        }
        self.boundaries[K] = sum + self.element_counts[K - 1];
        self.pointers[K - 1].1 = max(self.classified_elements as i64 - BLOCKSIZE as i64 - (self.classified_elements % BLOCKSIZE) as i64, 0);
    }
    pub fn permutate_blocks(&mut self, task: &mut Task) {
        self.calculate_pointers(task.arr.len());

        debug!("Sorter before permutation: {:?}", self);

        let mut pb: usize = self.primary_bucket;
        let mut swap_buffer = [[0; BLOCKSIZE]; 2];
        let mut swap_buffer_idx: usize = 0;

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
            self.pointers[pb as usize].1 -= BLOCKSIZE as i64;


            // TODO: check if already in right bucket and read < write, skip in this case

            // read block into swap buffer
            for i in 0..BLOCKSIZE {
                swap_buffer[swap_buffer_idx][i] = task.arr[(self.pointers[pb].1 + BLOCKSIZE as i64 + i as i64) as usize];
            }
            debug!("Read position {} into swap buffer {}: {:?}", self.pointers[pb].1 + BLOCKSIZE as i64, swap_buffer_idx, swap_buffer[swap_buffer_idx]);

            'inner: loop {
                let mut bdest = find_bucket_ips2ra(swap_buffer[swap_buffer_idx][0], task.level) as u64;
                let mut wdest = &mut self.pointers[bdest as usize].0;
                let mut rdest = &mut self.pointers[bdest as usize].1;
                debug!("First element: {}, Bucket: {}, Write: {}, Read: {}", swap_buffer[swap_buffer_idx][0], bdest, wdest, rdest);

                if *wdest <= *rdest {
                    // increment wdest pointers
                    *wdest += BLOCKSIZE as i64;

                    // read block into second swap buffer and write first swap buffer
                    let next_swap_buffer_idx = (swap_buffer_idx + 1) % 2;
                    debug!("writing {:?} to position {}", &mut swap_buffer[swap_buffer_idx], *wdest as usize - BLOCKSIZE);
                    for i in 0..BLOCKSIZE {
                        swap_buffer[next_swap_buffer_idx][i] = task.arr[*wdest as usize - BLOCKSIZE + i];
                        task.arr[*wdest as usize - BLOCKSIZE + i] = swap_buffer[swap_buffer_idx][i];
                    }
                    swap_buffer_idx = next_swap_buffer_idx;
                } else {
                    *wdest += BLOCKSIZE as i64;
                    if *wdest > task.arr.len() as i64 {
                        // write to overflow buffer
                        debug!("Write to overflow buffer - wdest: {}, tasklen: {}", wdest, task.arr.len());

                        // TODO: debug, remove later
                        assert_eq!(bdest, compute_overflow_bucket(&self.element_counts), "Overflow bucket not correct");

                        for i in 0..BLOCKSIZE {
                            self.overflow_buffer.push(swap_buffer[swap_buffer_idx][i]);
                        }
                        debug!("Writing Overflow Buffer: {:?}", &self.overflow_buffer);
                        self.overflow = true;
                        break 'inner;
                    }
                    // write swap buffer
                    debug!("break writing {:?} to position {}", &mut swap_buffer[swap_buffer_idx], *wdest as usize - BLOCKSIZE);
                    for i in 0..BLOCKSIZE {
                        task.arr[*wdest as usize - BLOCKSIZE + i] = swap_buffer[swap_buffer_idx][i];
                    }
                    break 'inner;
                }
            }
        }
    }

    pub fn permutate_blocks_ext(&mut self, task: &mut DMATask) {
        self.calculate_pointers(task.size);

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
    }
}


pub fn compute_overflow_bucket(element_count: &[u64]) -> u64 {
    for i in 1..=K {
        if element_count[K - i] >= BLOCKSIZE as u64 {
            return K as u64 - i as u64;
        }
    }
    return 0;
}

/*pub fn calculate_hugepage_chunk_block(input: usize) -> (usize, usize, usize) {
    let elements_per_hugepage = HUGE_PAGE_SIZE_2M / 8;
    let hugepage = input / elements_per_hugepage;
    let chunk_tmp = input % elements_per_hugepage;
    let chunk = chunk_tmp / (CHUNK_SIZE/8);
    let block = (chunk_tmp % (CHUNK_SIZE/8)) / BLOCKSIZE;

    (hugepage, chunk, block)
}*/

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

}

