use crate::config::{K, BLOCKSIZE};
use crate::sorter::{IPS2RaSorter, Task};

impl IPS2RaSorter {
    pub unsafe fn classify(&mut self, task: &mut Task) {
        let mut write_idx = 0;

        for i in 0..task.arr.len() {
            let element = task.arr.get_unchecked(i);
            let block_idx = find_bucket_ips2ra(*element, task.level);
            *self.element_counts.get_unchecked_mut(block_idx) += 1;

            // TODO: paper suggests to check if full first, then insert. Maybe change.
            *self.blocks[block_idx].get_unchecked_mut(self.block_counts[block_idx]) = *element;
            *self.block_counts.get_unchecked_mut(block_idx) += 1;

            if *self.block_counts.get_unchecked(block_idx) == BLOCKSIZE {
                let target_slice = &mut task.arr[write_idx..write_idx + BLOCKSIZE];
                target_slice.copy_from_slice(&self.blocks[block_idx]);
                write_idx += BLOCKSIZE;
                *self.block_counts.get_unchecked_mut(block_idx) = 0;
            }
        }



        self.classified_elements = write_idx;
    }
}


pub fn find_bucket_ips2ra(input: u64, level: usize) -> usize {
    // TODO: error with K > 4: FIX
    let number_bits = (K as u64).ilog2() as usize;
    let start = 64 - (number_bits * (level + 1));
    let mask = (1 << number_bits) - 1;
    ((input >> start) & mask) as usize
}


