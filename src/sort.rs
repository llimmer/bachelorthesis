use log::{debug, error, info};
use crate::base_case::{insertion_sort, insertion_sort_bound};
use crate::config::{BLOCKSIZE, K, THRESHOLD};
use crate::sampling::sample;
use crate::classification::classify;
use crate::permutation::permutate_blocks;
use crate::cleanup::cleanup;

pub fn sort(arr: &mut [u32]) {
    _sort(arr, 0, arr.len());
}

fn _sort(arr: &mut [u32], from: usize, to: usize) {

    // TODO: check and handle overflow case
    if to as i32 - from as i32 <= THRESHOLD as i32 {
        //debug!("Base case: {:?}", &arr[from..to]);
        insertion_sort_bound(arr, from, to);
        return;
    }

    debug!("Input: {:?}", arr);

    // buffer for decision tree/pointer/boundaries
    let mut decision_tree: Vec<u32> = vec![];
    let mut pointers = [(0, 0); K];
    let mut boundaries = [0; K + 1];

    // local buffers
    let mut blocks: Vec<Vec<u32>> = vec![vec![]; K];
    let mut element_count: [u32; K] = [0; K];
    let mut overflow_buffer: Vec<u32> = vec![];
    overflow_buffer.reserve(BLOCKSIZE);

    sample(arr, &mut decision_tree, from, to);
    debug!("Array after sampling: {:?}", arr);
    info!("Decision Tree: {:?}", decision_tree);


    let mut classified_elements = classify(arr, &decision_tree, &mut blocks, &mut element_count, from, to);
    debug!("Array after classification: {:?}", arr);
    info!("Classified Elements: {}", classified_elements);
    info!("Element Count: {:?}", element_count);
    info!("Blocks: {:?}", blocks);

    //if (to - from == 64) {
    //    debug!("First run: Overwriting array, decisiontree, element_count, blocks");
    //    let mut tmp = [37, 54, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 36, 35, 56, 62, 58, 60, 34, 33, 32, 50, 20, 29, 28, 27, 26, 25, 24, 23, 15, 18, 17, 16, 10, 13, 12, 11, 57, 59, 61, 55, 5, 9, 8, 7, 6, 4, 3, 2, 1, 14, 22, 21, 19, 30, 31, 51, 52, 53, 63, 64];
    //    // Overwrite array
    //    for i in 0..64 {
    //        arr[i] = tmp[i];
    //    }
    //    decision_tree = vec![29, 13, 54, 9, 18, 31, 62];
    //    classified_elements = 52;
    //    element_count = [9, 4, 5, 11, 2, 23, 8, 2];
    //    blocks = vec![vec![1], vec![], vec![14], vec![22, 21, 19], vec![30, 31], vec![51, 52, 53], vec![], vec![63, 64]];
    //}

    permutate_blocks(arr, &decision_tree, classified_elements, &element_count, &mut pointers, &mut boundaries, &mut overflow_buffer, from, to);
    debug!("Array after permutation: {:?}", arr);
    info!("Pointers: {:?}", pointers);
    info!("Boundaries: {:?}", boundaries);

    cleanup(arr, &boundaries, &element_count, &pointers, &mut blocks, &mut overflow_buffer);
    debug!("Output: {:?}", arr);

    // RECURSION:
    let mut sum = 0;
    for i in 0..K {
        let start = sum;
        sum += element_count[i];
        let end = sum;
        //debug!("Recursion sort from index {} (inclusive) to {} (exclusive)", start, end);
        _sort(arr, start as usize, end as usize);
    }
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

    #[test]
    fn test_all(){
        let mut sampled_input = [1, 2, 3, 4, 5, 8, 9, 13, 14, 16, 17, 19, 20, 22, 23, 24, 28, 31, 36, 38, 37, 30, 7, 25, 35, 32, 39, 34, 29, 40, 21, 27, 15, 10, 11, 12, 18, 26, 33, 6, 43, 44, 41, 42, 46, 49, 45, 48, 47, 53, 52, 51, 50, 57, 56, 54, 55, 58, 59, 62, 60, 63, 61, 64];
        let mut decision_tree = [20, 14, 28, 9, 17, 23, 36];
        let mut blocks: Vec<Vec<u32>> = vec![vec![]; K];
        let mut element_count: [u32; K] = [0; K];
        let mut overflow_buffer: Vec<u32> = vec![];

        let mut pointers = [(0, 0); K];
        let mut boundaries = [0; K + 1];
        let length = sampled_input.len();
        let mut classified_elements = classify(&mut sampled_input, &decision_tree, &mut blocks, &mut element_count, 0, length);

        let mut compare_buf = [31, 36, 30, 35, 38, 37, 39, 40, 24, 28, 25, 27, 13, 14, 10, 11, 32, 34, 29, 33, 8, 9, 7, 6, 12, 16, 17, 15, 19, 20, 18, 22, 23, 21, 26, 12, 18, 26, 33, 6, 43, 44, 41, 42, 46, 49, 45, 48, 47, 53, 52, 51, 50, 57, 56, 54, 55, 58, 59, 62, 60, 63, 61, 64];
        is_equal(&sampled_input, &compare_buf);
        assert_eq!(classified_elements, 24);
        is_equal(&element_count, &[4, 5, 3, 3, 3, 5, 8, 4]);
        println!("Blocks: {:?}, compare:  [[], [12], [16, 17, 15], [19, 20, 18], [22, 23, 21], [26], [], []]", blocks);

        let length = sampled_input.len();
        permutate_blocks(&mut sampled_input, &decision_tree, classified_elements, &element_count, &mut pointers, &mut boundaries, &mut overflow_buffer, 0, length);

    }

    fn is_equal(input: &[u32], compare: &[u32]) {
        if input.len() != compare.len() {
            panic!("Length mismatch: Expected {}, got {}", compare.len(), input.len());
        }

        for i in 0..input.len() {
            if input[i] != compare[i] {
                panic!("Error at index {}: Expected {}, got {}", i, compare[i], input[i]);
            }
        }
    }
}