use log::{debug, error, info};
use crate::config::{BLOCKSIZE, K};
use crate::sampling::sample;
use crate::classification::classify;
use crate::permutation::permutate_blocks;
use crate::cleanup::cleanup;

pub fn sort(arr: &mut [u32]){
    debug!("Input: {:?}", arr);

    // buffer for decision tree/pointer/boundaries
    let mut decision_tree: Vec<u32> = vec![];
    let mut pointers = [(0, 0); K];
    let mut boundaries = [0; K+1];

    // local buffers
    let mut blocks: Vec<Vec<u32>> = vec![vec![]; K];
    let mut element_count: [u32; K] = [0; K];

    sample(arr, &mut decision_tree);
    debug!("Array after sampling: {:?}", arr);
    info!("Decision Tree: {:?}", decision_tree);

    debug!("Overwriting decision tree");
    decision_tree = vec![29, 13, 54, 9, 18, 31, 62];

    let classified_elements = classify(arr, &decision_tree, &mut blocks, &mut element_count);
    debug!("Array after classification: {:?}", arr);
    info!("Classified Elements: {}", classified_elements);
    info!("Element Count: {:?}", element_count);
    info!("Blocks: {:?}", blocks);

    permutate_blocks(arr, &decision_tree, classified_elements, &element_count, &mut pointers, &mut boundaries);
    debug!("Array after permutation: {:?}", arr);
    info!("Pointers: {:?}", pointers);
    info!("Boundaries: {:?}", boundaries);

    cleanup(arr, &boundaries, &element_count, &pointers, &mut blocks);
    debug!("Output: {:?}", arr);
}

#[cfg(test)]
mod tests {
    use rand::seq::SliceRandom;
    use rand::thread_rng;
    use super::*;

    #[test]
    fn test_small() {
        let mut vec: Vec<u32> = (1..=64).rev().collect();
        //shuffle
        vec.shuffle(&mut thread_rng());
        sort(&mut vec);
        check_range(&vec, 1, 64);
    }

    fn check_range(input: &[u32], from: u32, to: u32) {
        'outer: for i in from..=to {
            for j in input.iter() {
                if i == *j {
                    continue 'outer;
                }
            }
            panic!("Element {} not found", i);
        }
    }
}