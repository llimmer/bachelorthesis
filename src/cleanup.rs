use log::{debug, info};
use vroom::memory::{Dma, DmaSlice};
use vroom::{NvmeQueuePair, QUEUE_LENGTH};
use crate::config::{BLOCKSIZE, K, LBA_PER_CHUNK, LBA_SIZE, THRESHOLD};
use crate::conversion::{u64_to_u8_slice, u8_to_u64_slice};
use crate::sorter::{DMATask, IPS2RaSorter, Task};
use crate::{insertion_sort};

impl IPS2RaSorter {

    pub fn cleanup(&mut self, task: &mut Task) {
        let first_bucket = 0;
        let last_bucket = K;

        let swap_bucket: i64 = -1;
        let in_swap_buffer = 0;
        let overflow_bucket = Self::compute_overflow_bucket(&self.boundaries);

        let is_last_level = task.level+1 == task.level_end;

        for i in first_bucket..last_bucket {
            let bstart = self.boundaries[i];
            let bend = self.boundaries[i + 1];
            let bwrite = self.pointers[i].0;

            let mut dst = bstart as usize;
            let mut remaining = Self::align_to_next_block(bstart as usize) - bstart as usize;

            debug!("i={}: bstart: {}, bend: {}, bwrite: {}, dst: {}, remaining: {}", i, bstart, bend, bwrite, dst, remaining);

            if i == overflow_bucket && self.overflow {
                debug!("Overflow bucket");
                let tail_size = BLOCKSIZE - remaining;
                let mut src = 0;
                // head
                task.arr[dst..dst + remaining].copy_from_slice(&self.overflow_buffer[src..src + remaining]);
                src += remaining;

                remaining = usize::MAX;

                dst = bwrite as usize - BLOCKSIZE;
                task.arr[dst..dst + tail_size].copy_from_slice(&self.overflow_buffer[src..src + tail_size]);
                dst += tail_size;

                // overflow -> reset //TODO: check what this does exactly
            } else if i as i64 == swap_bucket && in_swap_buffer != 0 {
                // only relevant for parallel version
                unimplemented!();
            } else if bwrite > bend as i64 && bend - bstart > BLOCKSIZE as u64 {
                debug!("bwrite ({}) > bend ({}) && bend - bstart ({}) > BLOCKSIZE", bwrite, bend, bend - bstart);
                let mut src = bend as usize;
                let mut head_size = bwrite as usize - bend as usize;

                debug!("Copying {:?} to {:?}", &task.arr[src..src+head_size], &task.arr[dst..dst+head_size]);

                //task.arr[dst..dst + head_size].copy_from_slice(&task.arr[src..src + head_size]);
                for i in 0..head_size {
                    task.arr[dst + i] = task.arr[src + i];
                }

                dst += head_size;
                remaining -= head_size;
            }

            // write elements from buffers
            let mut src = 0;
            let mut count = self.block_counts[i];

            if count <= remaining {
                if count > 0 {
                    debug!("Copying blocks[{i}][{}..{}] to {:?}", src, src+count, &task.arr[dst..dst + count]);
                    task.arr[dst..dst + count].copy_from_slice(&self.blocks[i][src..src + count]);
                }
                dst += count;
                remaining -= count;
            } else {
                if remaining > 0{
                    debug!("Copying blocks[{i}][{}..{}] to {:?}", src, src+count, &task.arr[dst..dst + remaining]);
                    task.arr[dst..dst + remaining].copy_from_slice(&self.blocks[i][src..src + remaining]);
                }

                src += remaining;
                count -= remaining;
                remaining = usize::MAX;

                dst = bwrite as usize;
                if count > 0 {
                    debug!("Copying blocks[{i}][{}..{}] to {:?}", src, src+count, &task.arr[dst..dst + count]);
                    task.arr[dst..dst + count].copy_from_slice(&self.blocks[i][src..src + count]);
                }
                dst += count;
            }

            self.block_counts[i] = 0;
            if !is_last_level {
                if bend-bstart <= THRESHOLD as u64{
                    insertion_sort(&mut task.arr[bstart as usize..bend as usize]);
                }
            }
        }

    }

    pub fn compute_overflow_bucket(boundaries: &[u64; K+1]) -> usize {
        let mut bucket = K-1;
        while (bucket >= 0 && (boundaries[bucket+1] - boundaries[bucket]) <= BLOCKSIZE as u64){
            if bucket == 0 {
                return 0;
            }
            bucket -= 1;
        }
        bucket
    }

    pub fn cleanup_ext(&mut self, task: &mut DMATask){
        assert!(self.qpair.is_some(), "Cannot classify_in_out without qpair");
        assert!(self.buffers.is_some(), "Cannot classify_in_out without buffers");

        let qpair = self.qpair.as_mut().unwrap();
        let buffer = self.buffers.as_mut().unwrap();

        let first_bucket = 0;
        let last_bucket = K;

        let swap_bucket: i64 = -1;
        let in_swap_buffer = 0;
        let overflow_bucket = Self::compute_overflow_bucket(&self.boundaries);

        let is_last_level = task.level+1 == task.level_end;

        for i in first_bucket..last_bucket {
            let bstart = self.boundaries[i];
            let bend = self.boundaries[i + 1];
            let bwrite = self.pointers[i].0;

            let mut dst = bstart as usize;
            let mut remaining = Self::align_to_next_block(bstart as usize) - bstart as usize;

            debug!("i={}: bstart: {}, bend: {}, bwrite: {}, dst: {}, remaining: {}", i, bstart, bend, bwrite, dst, remaining);

            if i == overflow_bucket && self.overflow {
                debug!("Overflow bucket");
                let tail_size = BLOCKSIZE - remaining;
                let mut src = 0;
                // head
                // read remaining elements from ssd
                let (start_lba, start_offset) = calculate_lba_offset(dst, task.start_lba, task.offset);
                read_write_elements(qpair, &mut buffer[0], start_lba, dst % BLOCKSIZE + start_offset, remaining, false);
                buffer[0][(dst % (LBA_SIZE / 8) + start_offset) * 8..(dst % (LBA_SIZE / 8) + start_offset + remaining) * 8].copy_from_slice(u64_to_u8_slice(&mut self.overflow_buffer[..remaining]));
                // write elements back to ssd
                read_write_elements(qpair, &mut buffer[0], start_lba, dst % BLOCKSIZE + start_offset, remaining, true);

                src += remaining;
                remaining = usize::MAX;
                dst = bwrite as usize - BLOCKSIZE;

                // read tailsize elements from ssd
                let (start_lba, start_offset) = calculate_lba_offset(dst, task.start_lba, task.offset);
                read_write_elements(qpair, &mut buffer[0], start_lba, dst % BLOCKSIZE + start_offset, tail_size, false);
                buffer[0][(dst % (LBA_SIZE / 8) + start_offset) * 8..(dst % (LBA_SIZE / 8) + start_offset + tail_size) * 8].copy_from_slice(u64_to_u8_slice(&mut self.overflow_buffer[src..src + tail_size]));
                // write elements back to ssd
                read_write_elements(qpair, &mut buffer[0], start_lba, dst % BLOCKSIZE + start_offset, tail_size, true);

                dst += tail_size;

                // overflow -> reset //TODO: check what this does exactly
            } else if i as i64 == swap_bucket && in_swap_buffer != 0 {
                // only relevant for parallel version
                unimplemented!();
            } else if bwrite > bend as i64 && bend - bstart > BLOCKSIZE as u64 {
                debug!("bwrite ({}) > bend ({}) && bend - bstart ({}) > BLOCKSIZE", bwrite, bend, bend - bstart);
                let mut src = bend as usize;
                let mut head_size = bwrite as usize - bend as usize;

                //task.arr[dst..dst + head_size].copy_from_slice(&task.arr[src..src + head_size]);
                // read head_size elements from ssd
                let (src_start_lba, src_start_offset) = calculate_lba_offset(src, task.start_lba, task.offset);
                let (dst_start_lba, dst_start_offset) = calculate_lba_offset(dst, task.start_lba, task.offset);

                read_write_elements(qpair, &mut buffer[0], src_start_lba, src % BLOCKSIZE + src_start_offset, head_size, false);
                read_write_elements(qpair, &mut buffer[1], dst_start_lba, dst % BLOCKSIZE + dst_start_offset, head_size, false);

                let (src_buffer, dst_buffer) = buffer.split_at_mut(1); // Split into two non-overlapping parts

                debug!("Copying {:?} to {:?}", &src_buffer[0][(src % (LBA_SIZE / 8) + src_start_offset) * 8..(src % (LBA_SIZE / 8) + src_start_offset + head_size) * 8], &dst_buffer[0][(dst % (LBA_SIZE / 8) + dst_start_offset) * 8..(dst % (LBA_SIZE / 8) + dst_start_offset + head_size) * 8]);

                let target_slice = &mut dst_buffer[0][(dst % (LBA_SIZE / 8) + dst_start_offset) * 8..(dst % (LBA_SIZE / 8) + dst_start_offset + head_size) * 8];
                target_slice.copy_from_slice(&src_buffer[0][(src % (LBA_SIZE / 8) + src_start_offset) * 8..(src % (LBA_SIZE / 8) + src_start_offset + head_size) * 8]);

                read_write_elements(qpair, &mut buffer[1], dst_start_lba, dst % BLOCKSIZE + dst_start_offset, head_size, true);

                dst += head_size;
                remaining -= head_size;
            }

            // write elements from buffers
            let mut src = 0;
            let mut count = self.block_counts[i];

            if count <= remaining {
                if count > 0 {
                    // read count elements from ssd
                    let (start_lba, start_offset) = calculate_lba_offset(dst, task.start_lba, task.offset);
                    read_write_elements(qpair, &mut buffer[0], start_lba, dst % BLOCKSIZE + start_offset, count, false);
                    debug!("Copying blocks[{i}][{}..{}] to {:?}", src, src+count, &mut buffer[0][(dst % (LBA_SIZE / 8) + start_offset) * 8..(dst % (LBA_SIZE / 8) + start_offset + count) * 8]);
                    buffer[0][(dst % (LBA_SIZE / 8) + start_offset) * 8..(dst % (LBA_SIZE / 8) + start_offset + count) * 8].copy_from_slice(u64_to_u8_slice(&mut self.blocks[i][src..src + count]));
                    // write elements back to ssd
                    read_write_elements(qpair, &mut buffer[0], start_lba, dst % BLOCKSIZE + start_offset, count, true);
                }
                dst += count;
                remaining -= count;
            } else {
                if remaining > 0 {
                    // read remaining elements from ssd
                    let (start_lba, start_offset) = calculate_lba_offset(dst, task.start_lba, task.offset);
                    read_write_elements(qpair, &mut buffer[0], start_lba, dst % BLOCKSIZE + start_offset, remaining, false);
                    debug!("Copying blocks[{i}][{}..{}] to {:?}", src, src+remaining, &mut buffer[0][(dst % (LBA_SIZE / 8) + start_offset) * 8..(dst % (LBA_SIZE / 8) + start_offset + remaining) * 8]);
                    buffer[0][(dst % (LBA_SIZE / 8) + start_offset) * 8..(dst % (LBA_SIZE / 8) + start_offset + remaining) * 8].copy_from_slice(u64_to_u8_slice(&mut self.blocks[i][src..src + remaining]));
                    // write elements back to ssd
                    read_write_elements(qpair, &mut buffer[0], start_lba, dst % BLOCKSIZE + start_offset, remaining, true);
                }
                src += remaining;
                count -= remaining;
                remaining = usize::MAX;

                dst = bwrite as usize;
                if count > 0 {
                    // read count elements from ssd
                    let (start_lba, start_offset) = calculate_lba_offset(dst, task.start_lba, task.offset);
                    read_write_elements(qpair, &mut buffer[0], start_lba, dst % BLOCKSIZE + start_offset, count, false);
                    debug!("Copying blocks[{i}][{}..{}] to {:?}", src, src+count, &mut buffer[0][(dst % (LBA_SIZE / 8) + start_offset) * 8..(dst % (LBA_SIZE / 8) + start_offset + count) * 8]);
                    buffer[0][(dst % (LBA_SIZE / 8) + start_offset) * 8..(dst % (LBA_SIZE / 8) + start_offset + count) * 8].copy_from_slice(u64_to_u8_slice(&mut self.blocks[i][src..src + count]));
                    // write elements back to ssd
                    read_write_elements(qpair, &mut buffer[0], start_lba, dst % BLOCKSIZE + start_offset, count, true);
                }

                dst += count;

            }
            self.block_counts[i] = 0;
            if !is_last_level {
                let diff = bend - bstart;
                if diff <= THRESHOLD as u64 && diff > 1 {
                    let (start_lba, start_offset) = calculate_lba_offset(bstart as usize, task.start_lba, task.offset);
                    read_write_elements(qpair, &mut buffer[0], start_lba, bstart as usize % BLOCKSIZE + start_offset, (bend-bstart) as usize, false);
                    insertion_sort(u8_to_u64_slice(&mut buffer[0][(bstart as usize % (LBA_SIZE / 8) + start_offset) * 8..(bstart as usize % (LBA_SIZE / 8) + start_offset + (bend-bstart) as usize) * 8]));
                    read_write_elements(qpair, &mut buffer[0], start_lba, bstart as usize % BLOCKSIZE + start_offset, (bend-bstart) as usize, true);
                }
            }
        }
    }
}

// read num_elements elements from target_lba (+target_offset elements) to buffer. Wait for completion.
fn read_write_elements(qpair: &mut NvmeQueuePair, buffer: &mut Dma<u8>, target_lba: usize, target_offset: usize, num_elements: usize, write: bool) {
    let num_lba = (target_offset * 8 + num_elements * 8 + LBA_SIZE - 1) / LBA_SIZE;
    debug!("Reading {} elements (=> {} lbas) from lba {} with offset {} to buffer", num_elements, num_lba, target_lba, target_offset);
    let tmp = qpair.submit_io(&mut buffer.slice(0..num_lba * LBA_SIZE), target_lba as u64, write);
    qpair.complete_io(tmp);
    debug!("Read: {:?}", u8_to_u64_slice(&mut buffer[0..num_lba * LBA_SIZE]));
}

pub fn calculate_lba_offset(index: usize, start_lba: usize, task_offset: usize) -> (usize, usize) {
    let lba = index * 8 / LBA_SIZE + start_lba;
    let offset = task_offset;

    debug!("Index: {}, LBA: {}, Offset: {}", index, lba, offset);

    (lba, offset)
}

/*
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
        //debug!("{}", s);

        check_range(&input, 1, 64);

        debug!("{:?}", input)
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

        //debug!("{}", s);

        check_range(&input, 1, 64);

        debug!("{:?}", input)
    }
}*/
