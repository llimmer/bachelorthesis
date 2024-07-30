use crate::config::{K, BLOCKSIZE};
use crate::sorter::{IPS2RaSorter, IPS4oSorter};

impl<'a> IPS4oSorter<'a> {
    pub fn classify(&mut self) {
        let mut write_idx = 0;

        for i in 0..self.arr.len() {
            let element = self.arr[i];
            let block_idx = find_bucket_ips4o(element, &self.decision_tree);
            self.element_counts[block_idx] += 1;

            // TODO: paper suggests to check if full first, then insert. Maybe change.
            self.blocks[block_idx].push(element);
            if self.blocks[block_idx].len() == BLOCKSIZE {
                for j in 0..BLOCKSIZE {
                    self.arr[write_idx] = self.blocks[block_idx][j];
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

pub(crate) fn find_bucket_ips4o(input: u64, decision_tree: &[u64; K - 1]) -> usize {
    let mut index = 0;
    let mut tree_index = 0;

    for _ in 0..K.ilog2() {
        let threshold = decision_tree[tree_index];

        if input <= threshold {
            index = 2 * index + 1; // Go to left child
        } else {
            index = 2 * index + 2; // Go to right child
        }

        tree_index = index;
    }
    index - (K - 1)
}

impl<'a> IPS2RaSorter<'a> {
    pub fn classify(&mut self) {
        let mut write_idx = 0;

        for i in 0..self.arr.len() {
            let element = self.arr[i];
            let block_idx = find_bucket_ips2ra(element, self.level);
            self.element_counts[block_idx] += 1;

            // TODO: paper suggests to check if full first, then insert. Maybe change.
            self.blocks[block_idx].push(element);
            if self.blocks[block_idx].len() == BLOCKSIZE {
                for j in 0..BLOCKSIZE {
                    self.arr[write_idx] = self.blocks[block_idx][j];
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


