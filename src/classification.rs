use log::debug;
use crate::config::{K, BLOCKSIZE};

pub fn classify(input: &mut [u32], decision_tree: &[u32], blocks: &mut Vec<Vec<u32>>, element_count: &mut [u32;K], from: usize, to: usize) -> usize {
    let mut write_idx = from;

    let arr: [[u32; BLOCKSIZE];K]= [[0; BLOCKSIZE]; K];

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
}

pub fn find_block(input: u32, decision_tree: &[u32]) -> usize {
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
    index - (K-1)
}