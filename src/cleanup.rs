use crate::config::{BLOCKSIZE, K};
use crate::permutation::compute_overflow_bucket;
use crate::sorter::{IPS2RaSorter, Task};

impl IPS2RaSorter {
    #[inline(never)]
    pub fn cleanup(&mut self, task: &mut Task) {
        let mut sum = 0;
        let overflow_bucket = compute_overflow_bucket(&self.element_counts) as usize;

        for i in 0..K {
            // dst = start of bucket
            let mut dst = sum as usize;

            let write_ptr = self.pointers[i].0;
            sum += self.element_counts[i];

            if (self.overflow && i == overflow_bucket) {
                let mut tailsize = sum as usize + BLOCKSIZE - write_ptr as usize;
                assert!(tailsize >= 0);
                assert_eq!(self.overflow_buffer.len(), BLOCKSIZE);
                let to_write: usize = BLOCKSIZE + self.block_counts[i];

                // case overflowbuffer > frontspace
                let mut to_write_front = to_write - tailsize as usize;
                if to_write_front < BLOCKSIZE {
                    // fill front
                    let target_slice = &mut task.arr[dst..dst + to_write_front];
                    target_slice.copy_from_slice(&self.overflow_buffer[..to_write_front]);
                    dst = sum as usize - tailsize;

                    // fill back
                    let overflow_back = BLOCKSIZE - to_write_front;
                    let target_slice = &mut task.arr[dst..dst + overflow_back];
                    target_slice.copy_from_slice(&self.overflow_buffer[to_write_front..]);
                    dst += overflow_back;
                    tailsize -= overflow_back;

                    // fill back with blocks
                    let target_slice = &mut task.arr[dst..dst + tailsize];
                    target_slice.copy_from_slice(&self.blocks[i][0..self.block_counts[i]]);
                } else { // case overflowbuffer <= frontspace
                    // fill front
                    let target_slice = &mut task.arr[dst..dst + BLOCKSIZE];
                    target_slice.copy_from_slice(&self.overflow_buffer[..]);
                    dst += BLOCKSIZE;
                    to_write_front -= BLOCKSIZE;

                    // fill front with blocks
                    let target_slice = &mut task.arr[dst..dst + to_write_front];
                    target_slice.copy_from_slice(&self.blocks[i][..to_write_front]);
                    dst = sum as usize - tailsize;

                    // fill back with blocks
                    let target_slice = &mut task.arr[dst..dst + tailsize];
                    target_slice.copy_from_slice(&self.blocks[i][to_write_front..]);
                }
                continue;
            }

            let mut to_write: usize = 0;

            if (write_ptr <= self.boundaries[i] as i64 || write_ptr as usize > task.arr.len()) {
                // do nothing
            }
            // write ptr > sum => (write ptr-sum) elements overwrite to right
            // TODO: check if i!=K-1 is necessary
            else if write_ptr > sum as i64 && i != K - 1 {
                // read elements and write to correct position
                // TODO: check if possible with slice copy
                for j in 0..((write_ptr as u64 - sum) as usize) {
                    let element = task.arr[(sum as usize + j) as usize];
                    task.arr[dst] = element;
                    dst += 1;
                }
            } else {
                // fill the back
                to_write = sum as usize - write_ptr as usize;
                if to_write > 0 {
                    let target_slice = &mut task.arr[write_ptr as usize..sum as usize];
                    target_slice.copy_from_slice(&self.blocks[i][..to_write]);
                }
            }

            // fill the front with remaining elements from blocks buffer
            let remaining = self.block_counts[i] - to_write;
            if remaining > 0 {
                let target_slice = &mut task.arr[dst..dst + remaining];
                target_slice.copy_from_slice(&self.blocks[i][to_write..self.block_counts[i]]);
            }
        }
    }
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
        //println!("{}", s);

        check_range(&input, 1, 64);

        println!("{:?}", input)
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

        //println!("{}", s);

        check_range(&input, 1, 64);

        println!("{:?}", input)
    }
}
