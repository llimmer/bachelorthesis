use log::info;
use vroom::memory::{Dma, DmaSlice};
use vroom::{NvmeQueuePair, QUEUE_LENGTH};
use crate::config::{BLOCKSIZE, K, LBA_PER_CHUNK, LBA_SIZE};
use crate::conversion::{u64_to_u8_slice, u8_to_u64_slice};
use crate::permutation::compute_overflow_bucket;
use crate::sorter::{DMATask, IPS2RaSorter, Task};

impl IPS2RaSorter {
    #[inline(never)]
    pub fn cleanup(&mut self, task: &mut Task) {
        info!("Starting cleanup");
        let mut sum = 0;
        let overflow_bucket = compute_overflow_bucket(&self.element_counts) as usize;

        for i in 0..K {
            info!("\n\ni = {}:", i);
            // dst = start of bucket
            let mut dst = sum as usize;

            let write_ptr = self.pointers[i].0;
            sum += self.element_counts[i];

            if self.overflow && i == overflow_bucket {
                let mut tailsize = sum as usize + BLOCKSIZE - write_ptr as usize;
                assert_eq!(self.overflow_buffer.len(), BLOCKSIZE);
                let to_write: usize = BLOCKSIZE + self.block_counts[i];

                info!("Overflow: tailsize = {}, to_write = {}", tailsize, to_write);

                // case overflowbuffer > frontspace
                let mut to_write_front = to_write - tailsize;
                if to_write_front < BLOCKSIZE {
                    info!("Overflow: to_write_front: {} < BLOCKSIZE: {}", to_write_front, BLOCKSIZE);
                    // fill front
                    info!("Copying {to_write_front} elements from overflow_buffer[..{to_write_front}] to {dst}");
                    info!("Src: -> {:?}", &self.overflow_buffer[..to_write_front]);
                    let target_slice = &mut task.arr[dst..dst + to_write_front];
                    target_slice.copy_from_slice(&self.overflow_buffer[..to_write_front]);
                    dst = sum as usize - tailsize;

                    // fill back
                    let overflow_back = BLOCKSIZE - to_write_front;
                    info!("Copying {overflow_back} elements from overflow_buffer[{to_write_front}..] to {dst}");
                    info!("Src: -> {:?}", &self.overflow_buffer[to_write_front..]);
                    let target_slice = &mut task.arr[dst..dst + overflow_back];
                    target_slice.copy_from_slice(&self.overflow_buffer[to_write_front..]);
                    dst += overflow_back;
                    tailsize -= overflow_back;

                    // fill back with blocks
                    info!("Copying {tailsize} elements from block {i} to {dst}");
                    info!("Src: -> {:?}", &self.blocks[i][0..self.block_counts[i]]);
                    let target_slice = &mut task.arr[dst..dst + tailsize];
                    target_slice.copy_from_slice(&self.blocks[i][0..self.block_counts[i]]);
                } else { // case overflowbuffer <= frontspace
                    info!("Overflow: to_write_front: {} >= BLOCKSIZE: {}", to_write_front, BLOCKSIZE);
                    // fill front
                    info!("Copying {BLOCKSIZE} elements from overflow_buffer[..] to {dst}");
                    info!("Src: -> {:?}", &self.overflow_buffer[..]);
                    let target_slice = &mut task.arr[dst..dst + BLOCKSIZE];
                    target_slice.copy_from_slice(&self.overflow_buffer[..]);
                    dst += BLOCKSIZE;
                    to_write_front -= BLOCKSIZE;

                    // fill front with blocks
                    info!("Copying {to_write_front} elements from block {i} to {dst}");
                    info!("Src: -> {:?}", &self.blocks[i][..to_write_front]);
                    let target_slice = &mut task.arr[dst..dst + to_write_front];
                    target_slice.copy_from_slice(&self.blocks[i][..to_write_front]);
                    dst = sum as usize - tailsize;

                    // fill back with blocks
                    info!("Copying {tailsize} elements from block {i} to {dst}");
                    info!("Src: -> {:?}", &self.blocks[i][to_write_front..]);
                    let target_slice = &mut task.arr[dst..dst + tailsize];
                    target_slice.copy_from_slice(&self.blocks[i][to_write_front..]);
                }
                continue;
            }

            let mut to_write: usize = 0;

            if write_ptr <= self.boundaries[i] as i64 || write_ptr as usize > task.arr.len() {
                info!("write ptr: {write_ptr} <= boundaries: {} or write ptr: {write_ptr} > task.size: {} --> skip", self.boundaries[i], task.arr.len());
                // do nothing
            }
            // write ptr > sum => (write ptr-sum) elements overwrite to right
            // TODO: check if i!=K-1 is necessary
            else if write_ptr > sum as i64 && i != K - 1 {
                info!("write ptr: {write_ptr} > sum: {sum} => (write ptr-sum): {} elements overwrite to right", write_ptr as u64-sum);
                // read elements and write to correct position
                // TODO: check if possible with slice copy
                info!("Copying {} elements from {sum} to {dst}", write_ptr as u64-sum);
                info!("Src: -> {:?}", &task.arr[sum as usize..sum as usize + (write_ptr as u64-sum) as usize]);
                info!("Dst: -> {:?}", &task.arr[dst..dst + (write_ptr as u64-sum) as usize]);
                for j in 0..((write_ptr as u64 - sum) as usize) {
                    let element = task.arr[sum as usize + j];
                    task.arr[dst] = element;
                    dst += 1;
                }
            } else {
                info!("write ptr: {write_ptr} <= sum: {sum}");
                // fill the back
                to_write = sum as usize - write_ptr as usize;
                if to_write > 0 {
                    info!("Copying {to_write} elements from block {i} to {write_ptr}");
                    info!("Src: -> {:?}", &self.blocks[i][..to_write]);
                    info!("Dst: -> {:?}", &task.arr[write_ptr as usize..sum as usize]);
                    let target_slice = &mut task.arr[write_ptr as usize..sum as usize];
                    target_slice.copy_from_slice(&self.blocks[i][..to_write]);
                }
            }

            // fill the front with remaining elements from blocks buffer
            let remaining = self.block_counts[i] - to_write;
            if remaining > 0 {
                info!("Copying {remaining} elements from block {i} to {dst}");
                info!("Src: -> {:?}", &self.blocks[i][to_write..self.block_counts[i]]);
                info!("Dst: -> {:?}", &task.arr[dst..dst + remaining]);
                let target_slice = &mut task.arr[dst..dst + remaining];
                target_slice.copy_from_slice(&self.blocks[i][to_write..self.block_counts[i]]);
            }
        }
    }

    pub fn cleanup_ext(&mut self, task: &mut DMATask) {
        info!("Starting external cleanup");
        let mut sum = 0;
        let overflow_bucket = compute_overflow_bucket(&self.element_counts) as usize;

        assert!(self.qpair.is_some());
        assert!(self.buffers.is_some());

        let mut qpair = self.qpair.as_mut().unwrap();
        let mut buffer = self.buffers.as_mut().unwrap();

        assert!(buffer.len() > 1);

        for i in 0..K {
            info!("\n\ni = {}:", i);
            // dst = start of bucket
            let mut dst = sum as usize;

            let write_ptr = self.pointers[i].0;
            sum += self.element_counts[i];

            if self.overflow && i == overflow_bucket {

                let mut tailsize = sum as usize + BLOCKSIZE - write_ptr as usize;
                assert_eq!(self.overflow_buffer.len(), BLOCKSIZE);
                let to_write: usize = BLOCKSIZE + self.block_counts[i];

                info!("Overflow: tailsize = {}, to_write = {}", tailsize, to_write);

                // case overflowbuffer > frontspace
                let mut to_write_front = to_write - tailsize;
                if to_write_front < BLOCKSIZE {
                    info!("Overflow: to_write_front: {} < BLOCKSIZE: {}", to_write_front, BLOCKSIZE);
                    // fill front
                    // read from ssd
                    let start_lba = calculate_lba(dst);
                    read_elements(&mut qpair, &mut buffer[0], start_lba, dst%BLOCKSIZE, to_write_front, false);

                    // copy to slice
                    info!("Copying {to_write_front} elements from overflow_buffer[..{to_write_front}] to {dst}");
                    let target_slice = &mut buffer[0][(dst%(LBA_SIZE/8))*8..((dst%(LBA_SIZE/8)) + to_write_front)*8];
                    target_slice.copy_from_slice(u64_to_u8_slice(&mut self.overflow_buffer[..to_write_front]));

                    // write back to ssd
                    write_elements(&mut qpair, &mut buffer[0], start_lba, dst%BLOCKSIZE, to_write_front);

                    dst = sum as usize - tailsize;

                    // fill back
                    let overflow_back = BLOCKSIZE - to_write_front;

                    // read from ssd
                    let start_lba = calculate_lba(dst);
                    read_elements(&mut qpair, &mut buffer[0], start_lba, dst%BLOCKSIZE, overflow_back, false);

                    // copy to slice
                    info!("Copying {overflow_back} elements from overflow_buffer[{to_write_front}..] to {dst}");
                    let target_slice = &mut buffer[0][(dst%(LBA_SIZE/8))*8..((dst%(LBA_SIZE/8)) + overflow_back)*8];
                    target_slice.copy_from_slice(u64_to_u8_slice(&mut self.overflow_buffer[to_write_front..]));

                    // write back to ssd
                    write_elements(&mut qpair, &mut buffer[0], start_lba, dst%BLOCKSIZE, overflow_back);

                    dst += overflow_back;
                    tailsize -= overflow_back;

                    // fill back with blocks
                    // read from ssd
                    let start_lba = calculate_lba(dst);
                    read_elements(&mut qpair, &mut buffer[0], start_lba, dst%BLOCKSIZE, tailsize, false);

                    // copy to slice
                    info!("Copying {tailsize} elements from block {i} to {dst}");
                    let target_slice = &mut buffer[0][(dst%(LBA_SIZE/8))*8..((dst%(LBA_SIZE/8)) + tailsize)*8];
                    target_slice.copy_from_slice(u64_to_u8_slice(&mut self.blocks[i][0..self.block_counts[i]]));

                    // write back to ssd
                    write_elements(&mut qpair, &mut buffer[0], start_lba, dst%BLOCKSIZE, tailsize);

                } else { // case overflowbuffer <= frontspace
                    info!("Overflow: to_write_front: {} >= BLOCKSIZE: {}", to_write_front, BLOCKSIZE);
                    // fill front
                    // read from ssd
                    let start_lba = calculate_lba(dst);
                    read_elements(&mut qpair, &mut buffer[0], start_lba, dst%BLOCKSIZE, BLOCKSIZE, false);

                    // copy to slice
                    info!("Copying {BLOCKSIZE} elements from overflow_buffer[..] to {dst}");
                    let target_slice = &mut buffer[0][(dst%(LBA_SIZE/8))*8..((dst%(LBA_SIZE/8)) + BLOCKSIZE)*8];
                    target_slice.copy_from_slice(u64_to_u8_slice(&mut self.overflow_buffer[..]));

                    // write back to ssd
                    write_elements(&mut qpair, &mut buffer[0], start_lba, dst%BLOCKSIZE, BLOCKSIZE);

                    dst += BLOCKSIZE;
                    to_write_front -= BLOCKSIZE;

                    // fill front with blocks
                    // read from ssd
                    let start_lba = calculate_lba(dst);
                    read_elements(&mut qpair, &mut buffer[0], start_lba, dst%BLOCKSIZE, to_write_front, false);

                    // copy to slice
                    info!("Copying {to_write_front} elements from block {i} to {dst}");
                    let target_slice = &mut buffer[0][(dst%(LBA_SIZE/8))*8..((dst%(LBA_SIZE/8)) + to_write_front)*8];
                    target_slice.copy_from_slice(u64_to_u8_slice(&mut self.blocks[i][..to_write_front]));

                    // write back to ssd
                    write_elements(&mut qpair, &mut buffer[0], start_lba, dst%BLOCKSIZE, to_write_front);

                    dst = sum as usize - tailsize;

                    // fill back with blocks
                    // read from ssd
                    let start_lba = calculate_lba(dst);
                    read_elements(&mut qpair, &mut buffer[0], start_lba, dst%BLOCKSIZE, tailsize, false);

                    // copy to slice
                    info!("Copying {tailsize} elements from block {i} to {dst}");
                    let target_slice = &mut buffer[0][(dst%(LBA_SIZE/8))*8..((dst%(LBA_SIZE/8)) + tailsize)*8];
                    target_slice.copy_from_slice(u64_to_u8_slice(&mut self.blocks[i][to_write_front..]));

                    // write back to ssd
                    write_elements(&mut qpair, &mut buffer[0], start_lba, dst%BLOCKSIZE, tailsize);

                }
                continue;
            }

            let mut to_write: usize = 0;

            if write_ptr <= self.boundaries[i] as i64 || write_ptr as usize > task.size {
                info!("write ptr: {write_ptr} <= boundaries: {} or write ptr: {write_ptr} > task.size: {} --> skip", self.boundaries[i], task.size);
                // do nothing
            }

            // write ptr > sum => (write ptr-sum) elements overwrite to right
            // TODO: check if i!=K-1 is necessary
            else if write_ptr > sum as i64 && i != K - 1 {
                info!("write ptr: {write_ptr} > sum: {sum} => (write ptr-sum): {} elements overwrite to right", write_ptr as u64-sum);
                let to_write = (write_ptr as u64 - sum) as usize;
                // read elements and write to correct position

                // load src and dst from ssd
                let src_start_lba = calculate_lba(sum as usize);
                let dst_start_lba = calculate_lba(dst);

                read_elements(&mut qpair, &mut buffer[0], src_start_lba, sum as usize % BLOCKSIZE, to_write, false);
                read_elements(&mut qpair, &mut buffer[1], dst_start_lba, dst % BLOCKSIZE, to_write, false);

                let (src_buffer, dst_buffer) = buffer.split_at_mut(1); // Split into two non-overlapping parts

                info!("Copying {to_write} elements from {sum} to {dst}");
                info!("Src: -> {:?}", u8_to_u64_slice(&mut src_buffer[0][(sum as usize % (LBA_SIZE/8))*8..((sum as usize % (LBA_SIZE/8)) + to_write)*8]));
                info!("Dst: -> {:?}\n", u8_to_u64_slice(&mut dst_buffer[0][(dst % (LBA_SIZE/8))*8..((dst % (LBA_SIZE/8)) + to_write)*8]));
                let target_slice = &mut dst_buffer[0][(dst % (LBA_SIZE/8))*8..((dst % (LBA_SIZE/8)) + to_write)*8];
                target_slice.copy_from_slice(&src_buffer[0][(sum as usize % (LBA_SIZE/8))*8..((sum as usize % (LBA_SIZE/8)) + to_write)*8]);


                // write back to ssd
                write_elements(&mut qpair, &mut buffer[1], dst_start_lba, dst%BLOCKSIZE, to_write);

                dst += to_write;

            } else {
                info!("write ptr: {write_ptr} <= sum: {sum}");
                // fill the back
                to_write = sum as usize - write_ptr as usize;
                if to_write > 0 {
                    // read from ssd
                    let start_lba = calculate_lba(write_ptr as usize);
                    read_elements(&mut qpair, &mut buffer[0], start_lba, write_ptr as usize % BLOCKSIZE, to_write, false);

                    // copy to slice
                    info!("Copying {to_write} elements from block {i} to {write_ptr}");
                    info!("Src: -> {:?}", &self.blocks[i][..to_write]);
                    info!("Dst: -> {:?}\n", u8_to_u64_slice(&mut buffer[0][(write_ptr as usize % (LBA_SIZE/8))*8..((write_ptr as usize % (LBA_SIZE/8)) + to_write)*8]));
                    let target_slice = &mut buffer[0][(write_ptr as usize % (LBA_SIZE/8))*8..((write_ptr as usize % (LBA_SIZE/8)) + to_write)*8];
                    target_slice.copy_from_slice(u64_to_u8_slice(&mut self.blocks[i][..to_write]));

                    // write back to ssd
                    write_elements(&mut qpair, &mut buffer[0], start_lba, write_ptr as usize % BLOCKSIZE, to_write);
                }
            }

            // fill the front with remaining elements from blocks buffer
            let remaining = self.block_counts[i] - to_write;
            if remaining > 0 {
                // read from ssd
                let start_lba = calculate_lba(dst);
                read_elements(&mut qpair, &mut buffer[0], start_lba, dst%BLOCKSIZE, remaining, false);

                // copy to slice
                info!("Copying {remaining} elements from block {i} to {dst}");
                info!("Src: -> {:?}", &self.blocks[i][to_write..self.block_counts[i]]);
                info!("Dst: -> {:?}\n", u8_to_u64_slice(&mut buffer[0][(dst%(LBA_SIZE/8))*8..((dst%(LBA_SIZE/8)) + remaining)*8]));

                let target_slice = &mut buffer[0][(dst%(LBA_SIZE/8))*8..((dst%(LBA_SIZE/8)) + remaining)*8];
                target_slice.copy_from_slice(u64_to_u8_slice(&mut self.blocks[i][to_write..self.block_counts[i]]));

                // write back to ssd
                write_elements(&mut qpair, &mut buffer[0], start_lba, dst%BLOCKSIZE, remaining);
            }
        }
    }
}

// read num_elements elements from target_lba (+target_offset elements) to buffer. Wait for completion.
fn read_elements(qpair: &mut NvmeQueuePair, buffer: &mut Dma<u8>, target_lba: usize, target_offset: usize, num_elements: usize, write: bool) {
    let num_lba = (target_offset*8 + num_elements*8 + LBA_SIZE - 1) / LBA_SIZE;
    info!("Reading {} elements (=> {} lbas) from lba {} with offset {} to buffer", num_elements, num_lba, target_lba, target_offset);
    let tmp = qpair.submit_io(&mut buffer.slice(0..num_lba*LBA_SIZE), target_lba as u64, write);
    qpair.complete_io(tmp);
    info!("Read: {:?}", u8_to_u64_slice(&mut buffer[0..num_lba*LBA_SIZE]));
}

fn write_elements(qpair: &mut NvmeQueuePair, buffer: &mut Dma<u8>, target_lba: usize, target_offset: usize, num_elements: usize) {
    let num_lba = (target_offset*8 + num_elements*8 + LBA_SIZE - 1) / LBA_SIZE;
    info!("Reading {} elements (=> {} lbas) from lba {} with offset {} to buffer", num_elements, num_lba, target_lba, target_offset);
    let tmp = qpair.submit_io(&mut buffer.slice(0..num_lba*LBA_SIZE), target_lba as u64, true);
    qpair.complete_io(tmp);
}

// TODO: may include offset from task
fn calculate_lba(index: usize) -> usize{
    index*8/LBA_SIZE
}




#[cfg(test)]
mod tests {
    use log::debug;
    use super::*;

    fn check_range(input: &[u64], from: u64, to: u64) {
        'outer: for i in from..=to {
            for j in input.iter() {
                if i == *j {
                    continue 'outer;
                }
            }
            panic!("Element {} not found", i);
        }
    }

    #[test]
    fn test_small() {
        let mut input = [5, 9, 8, 7, 6, 4, 3, 2, 43, 42, 41, 40, 10, 13, 12, 11, 15, 18, 17, 16, 26, 25, 24, 23, 20, 29, 28, 27, 26, 25, 24, 23, 43, 42, 41, 40, 47, 46, 45, 44, 39, 38, 36, 35, 37, 54, 49, 48, 34, 33, 32, 50, 1, 14, 22, 21, 56, 62, 58, 60, 57, 59, 61, 55];
        let blocks: Vec<Vec<u64>> = vec![vec![1], vec![], vec![14], vec![22, 21, 19], vec![30, 31], vec![51, 52, 53], vec![], vec![63, 64]];
        let decision_tree = [29, 13, 54, 9, 18, 31, 62];
        let element_counts = [9, 4, 5, 11, 2, 23, 8, 2];
        let boundaries = [0, 12, 16, 20, 32, 32, 56, 64, 64];
        let pointers = [(8, 0), (16, 12), (20, 16), (28, 24), (32, 28), (52, 48), (64, 48), (64, 48)];
        let mut overflow_buffer: Vec<u64> = vec![];
        //let mut s = Sorter::new_(&mut input, decision_tree, 0, pointers, boundaries, 0, blocks, element_counts, false, overflow_buffer);
        //s.cleanup();
        //
        //info!("{}", s);

        check_range(&input, 1, 64);

        info!("{:?}", input)
    }

    #[test]
    fn test_overflow_small() {
        let mut input = [5, 9, 8, 7, 6, 4, 3, 2, 43, 42, 41, 40, 10, 13, 12, 11, 15, 18, 17, 16, 26, 25, 24, 23, 20, 29, 28, 27, 26, 25, 24, 23, 43, 42, 41, 40, 47, 46, 45, 44, 39, 38, 36, 35, 37, 54, 49, 48, 34, 33, 32, 50, 63, 64, 65, 66, 56, 62, 58, 60, 57, 59, 61, 55, 52, 53, 67];
        let mut blocks: Vec<Vec<u64>> = vec![vec![1], vec![], vec![14], vec![22, 21, 19], vec![30, 31], vec![51, 52, 53], vec![], vec![67]];
        let decision_tree = [29, 13, 54, 9, 18, 31, 62];
        let element_counts = [9, 4, 5, 11, 2, 23, 8, 5];
        let boundaries = [0, 12, 16, 20, 32, 32, 56, 67, 67];
        let pointers = [(8, 0), (16, 12), (20, 16), (28, 20), (32, 28), (52, 48), (64, 48), (68, 48)];
        let mut overflow_buffer = vec![63, 64, 65, 66];
        //let mut s = Sorter::new_(&mut input, decision_tree, 0, pointers, boundaries, 0, blocks, element_counts, true, overflow_buffer);
        //s.cleanup();

        //info!("{}", s);

        check_range(&input, 1, 64);

        info!("{:?}", input)
    }
}
