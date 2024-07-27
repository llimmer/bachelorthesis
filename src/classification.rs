use log::debug;
use crate::config::{K, BLOCKSIZE};
use crate::sorter::Sorter;

impl<'a> Sorter<'a> {
    pub fn classify(&mut self) -> usize {
        let mut write_idx = 0;

        for i in 0..self.arr.len() {
            let element = self.arr[i];
            let block_idx = find_block(element, &self.decision_tree);
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

        // TODO: only debug reason, remove later
        let mut tmp = write_idx;
        for i in 0..K {
            for j in 0..self.blocks[i].len() {
                self.arr[tmp] = self.blocks[i][j];
                tmp += 1;
            }
        }
        return write_idx;
    }
}

pub fn find_block(input: u64, decision_tree: &[u64; K-1]) -> usize {
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

/*
pub fn sorter_classify(sorter: &mut Sorter) -> u64 {
    classify(sorter.arr, sorter.decision_tree, sorter.blocks, sorter.element_count, sorter.from, sorter.to) as u64
}
pub fn classify(input: &mut [u64], decision_tree: &[u64], blocks: &mut Vec<Vec<u64>>, element_count: &mut [u64;K], from: usize, to: usize) -> usize {
    let mut write_idx = from;

    let arr: [[u64; BLOCKSIZE];K]= [[0; BLOCKSIZE]; K];

    for i in from..to {
        let element = input[i];
        let block_idx = find_block(element, decision_tree);
        element_count[block_idx] += 1;

        // TODO: paper suggests to check if full first, then insert. Maybe change.
        blocks[block_idx].push(element);
        if blocks[block_idx].len() == BLOCKSIZE {
            for j in 0..BLOCKSIZE {
                input[write_idx] = blocks[block_idx][j];
                write_idx += 1;
            }
            blocks[block_idx].clear();
        }
    }

    // TODO: only debug reason, remove later
    let mut tmp = write_idx;
    for i in 0..K {
        for j in 0..blocks[i].len() {
            input[tmp] = blocks[i][j];
            tmp += 1;
        }
    }
    return write_idx-from;
}*/

