use crate::config::{K, BLOCKSIZE};
use crate::sorter::{IPS2RaSorter, Task};

impl IPS2RaSorter {
    pub fn classify(&mut self, task: &mut Task) {
        let mut write_idx = 0;

        for i in 0..task.arr.len() {
            let element = task.arr[i];
            let block_idx = find_bucket_ips2ra(element, task.level);
            self.element_counts[block_idx] += 1;

            // TODO: paper suggests to check if full first, then insert. Maybe change.
            self.blocks[block_idx].push(element);
            if self.blocks[block_idx].len() == BLOCKSIZE {
                for j in 0..BLOCKSIZE {
                    task.arr[write_idx] = self.blocks[block_idx][j];
                    write_idx += 1;
                }
                self.blocks[block_idx].clear();
            }
        }


        // exTODO: only debug reason, remove later
        let mut tmp = write_idx;
        //for i in 0..K {
        //    for j in 0..self.blocks[i].len() {
        //        self.arr[tmp] = self.blocks[i][j];
        //        tmp += 1;
        //    }
        //}
        self.classified_elements = write_idx;

    }
}


pub fn find_bucket_ips2ra(input: u64, level: usize) -> usize {
    // TODO: error with K > 4: FIX
    let number_bits = (K as u64).ilog2() as usize;
    let start = 64 - (number_bits * (level+1));
    let mask = (1 << number_bits) - 1;
    ((input >> start) & mask) as usize
}


