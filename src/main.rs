use log::LevelFilter;
use log::{debug, info, warn, error};
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use rand::{SeedableRng, thread_rng};
use std::cmp::max;
mod sampling;
mod base_case;
mod classification;
mod config;
mod permutation;
mod cleanup;
mod sort;
mod sorter;
mod cleanup_old;
mod tmp;

use sorter::Sorter;

fn main() {
    env_logger::builder()
        .filter_level(LevelFilter::Error)
        .init();

    //let mut arr = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48];
    //_sort(&mut arr, 16, 64);

    //let mut arr: Vec<u64> = (1..128).collect();
    //// shuffle array each iteration to get different results
    //let seed: u64 = 34123;
    //let mut rng = StdRng::seed_from_u64(seed);
    //arr.shuffle(&mut rng);

    //let mut s = Sorter::new(&mut arr);
    //s.sort();

    //println!("Output: {}", s);

    //for i in 0..arr.len() {
    //    if arr[i] != (i + 1) as u64 {
    //        println!("Error at index {}: Expected {}, got {}", i, i + 1, arr[i]);
    //    }
    //}
    //println!("Success");

    //fn check_range(input: &[u64], from: u64, to: u64) {
    //    'outer: for i in from..=to {
    //        for j in input.iter() {
    //            if i == *j {
    //                continue 'outer;
    //            }
    //        }
    //        panic!("Element {} not found", i);
    //    }
    //}


    //let mut arr = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48];
    //let mut decision_tree = [24, 12, 36, 6, 18, 30, 42];
    //let mut blocks = vec![vec![]; K];
    //let mut element_count = [0; K];
    //let mut pointers = [(0, 0); K];
    //let mut boundaries = [0; K+1];
    //let mut overflow_buffer = vec![];
    //overflow_buffer.reserve(BLOCKSIZE);
    //
    //let length = arr.len();
    //let count = classify(&mut arr, &decision_tree, &mut blocks, &mut element_count, 16, length);
    //permutate_blocks(&mut arr, &decision_tree, count, &element_count, &mut pointers, &mut boundaries, &mut overflow_buffer, 16, length);
    //
    //cleanup(&mut arr, &boundaries, &element_count, &pointers, &mut blocks, &mut overflow_buffer, 16, length);
    //println!("{:?}", arr);
    //
    //let mut arr = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48];
    //_sort(&mut arr, 16, 64);
    //println!("{:?}", arr);

    //for j in 0..100000 {

    let mut arr = [25, 56, 14, 19, 55, 29, 47, 58, 61, 59, 6, 34, 17, 31, 49, 37, 45, 50, 21, 52, 48, 64, 44, 1, 8, 16, 10, 40, 27, 36, 3, 15, 26, 62, 60, 7, 18, 5, 57, 2, 54, 46, 43, 4, 12, 9, 30, 22, 35, 24, 51, 33, 42, 38, 11, 28, 63, 13, 39, 41, 23, 32, 20, 53];
    let mut s = Sorter::new(&mut arr);
    s.sort();
    println!("{}", s);
    let mut i = 0;
    let mut rng = StdRng::seed_from_u64(34123);



    loop{
        let mut arr: Vec<u64> = (1..128).collect();
        let length = arr.len();
        // shuffle array each iteration to get different results
        arr.shuffle(&mut rng);
        println!("Org Input {}: {:?}", i, arr);
        let mut s = Sorter::new(&mut arr);
        s.sort();

        for i in 0..length {
            if s.arr[i] != (i + 1) as u64 {
                println!("{}", s);
                panic!("Error at index {}: Expected {}, got {}", i, i + 1, s.arr[i]);

                break;
            }
        }
        println!("Success: {}", s);
        break;
        i+=1;

    }

    //let mut arr: Vec<u64> = (1..=64).collect();
    //// shuffle array each iteration to get different results
    //let mut trng = thread_rng();
    //arr.shuffle(&mut trng);
    //sort::sort(&mut arr);
    //println!("Output: {:?}", arr);

    //for i in 0..arr.len() {
    //    if arr[i] != (i + 1) as u64 {
    //        println!("Error at index {}: Expected {}, got {}", i, i + 1, arr[i]);
    //    }
    //     break;
    //}

    //let mut arr = [9, 16, 37, 39, 42, 44, 52, 54, 62, 75, 92, 101, 112, 125, 128, 22, 122, 34, 23, 95, 127, 46, 113, 21, 81, 47, 116, 97, 83, 10, 110, 79, 78, 18, 56, 70, 1, 89, 29, 74, 91, 115, 15, 27, 14, 49, 59, 53, 13, 5, 87, 38, 20, 108, 32, 103, 55, 73, 61, 102, 17, 12, 86, 117, 58, 69, 90, 65, 36, 66, 7, 24, 11, 51, 121, 123, 60, 120, 96, 67, 85, 43, 124, 100, 50, 99, 109, 30, 80, 118, 6, 28, 31, 88, 33, 63, 26, 57, 114, 4, 35, 76, 2, 48, 105, 64, 8, 98, 119, 68, 3, 40, 25, 41, 72, 93, 84, 107, 77, 111, 106, 71, 82, 45, 94, 19, 126, 104];
    //// shuffle array
    //let mut trng = thread_rng();
    //arr.shuffle(&mut trng);
    //let mut decision_tree = [54, 39, 101, 16, 44, 75, 125];
    //let mut blocks = vec![vec![]; K];
    //let mut element_count = [0; K];
    //let mut pointers = [(0, 0); K];
    //let mut boundaries = [0; K+1];
    //let mut overflow_buffer = vec![];
    //overflow_buffer.reserve(BLOCKSIZE);
    //
    //let length = arr.len();
    //let count = classify(&mut arr, &decision_tree, &mut blocks, &mut element_count, 0, length);
    //permutate_blocks(&mut arr, &decision_tree, count, &element_count, &mut pointers, &mut boundaries, &mut overflow_buffer, 0, length);
    //cleanup(&mut arr, &boundaries, &element_count, &pointers, &mut blocks, &mut overflow_buffer);
    //
    //let mut sum = 0;
    //for i in 0..K {
    //    let start = sum;
    //    sum += element_count[i];
    //    let end = sum;
    //    //debug!("Recursion sort from index {} (inclusive) to {} (exclusive)", start, end);
    //    sort::_sort(&mut arr, start as usize, end as usize);
    //}
    //println!("Output: {:?}", arr);
    //
    //for i in 0..arr.len() {
    //    if arr[i] != (i + 1) as u64 {
    //        println!("Error at index {}: Expected {}, got {}", i, i + 1, arr[i]);
    //    }
    //}
    //println!("Success");
    //


    //let mut arr = [1, 2, 3, 4, 5, 6, 7, 8, 9, 24, 36, 34, 11, 42, 33, 10, 17, 20, 12, 23, 39, 29, 28, 27, 25, 37, 32, 16, 19, 31, 41, 21, 40, 30, 13, 38, 14, 35, 15, 22, 26, 18, 44, 43, 45, 52, 50, 49, 48, 51, 46, 47, 54, 59, 56, 57, 53, 58, 55, 60, 61, 63, 62, 64];
    //sort::sort(&mut arr);


    //let mut arr = [19, 25, 24, 20, 50, 61, 53, 58, 12, 13, 11, 7, 6, 10, 9, 8, 19, 25, 24, 20, 14, 16, 21, 17, 23, 15, 18, 22, 26, 36, 31, 28, 29, 27, 35, 33, 39, 46, 44, 40, 38, 42, 45, 43, 23, 15, 18, 22, 48, 57, 52, 54, 50, 61, 53, 58, 60, 47, 55, 56, 49, 63, 62, 64];
    //let mut decision_tree = [25, 5, 46, 2, 13, 36, 61];
    //let mut blocks = vec![vec![1, 2], vec![4, 5, 3], vec![], vec![], vec![34, 30, 32], vec![37, 41], vec![51,59,49], vec![63, 62, 64]];
    //let mut element_count = [2, 3, 8, 12, 11, 10, 15, 3];
    //let pointers = [(0, -4), (4, 0), (16, 12), (28, 20), (36, 32), (44, 40), (60, 44), (64, 44)];
    //let boundaries = [0, 4, 8, 16, 28, 36, 48, 64, 64];
    //let mut overflow_buffer = vec![];
    //overflow_buffer.reserve(BLOCKSIZE);
    //

    //let mut arr = [8, 9, 1, 5, 7, 2, 3, 6, 29, 33, 25, 28, 47, 52, 44, 46, 15, 20, 17, 19, 29, 33, 25, 28, 26, 24, 30, 22, 32, 23, 31, 27, 38, 36, 41, 42, 35, 43, 37, 40, 38, 36, 41, 42, 47, 52, 44, 46, 50, 51, 45, 48, 54, 62, 61, 60, 58, 56, 63, 57, 64, 55, 59, 53];
    //let mut decision_tree = [20, 12, 43, 9, 14, 33, 52];
    //let mut blocks = vec![vec![4], vec![10, 12, 11], vec![13, 14], vec![18, 16], vec![21], vec![39, 34], vec![49], vec![]];
    //let mut element_count = [9, 3, 2, 6, 13, 10, 9, 12];
    //let mut pointers = [(8, 0), (12, 8), (12, 8), (20, 16), (32, 28), (44, 40), (52, 48), (64, 48)];
    //let mut boundaries = [0, 12, 12, 16, 20, 36, 44, 52, 64];
    //let mut overflow_buffer = vec![];
    //overflow_buffer.reserve(BLOCKSIZE);

    //
    //cleanup(&mut arr, &boundaries, &element_count, &pointers, &mut blocks, &overflow_buffer);


    //println!("Output: {:?}", arr);

    //let mut vec = vec![5, 9, 10, 13, 15, 18, 20, 29, 30, 31, 37, 54, 56, 62, 63, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 58, 36, 35, 34, 33, 32, 60, 50, 51, 28, 27, 26, 25, 24, 23, 22, 21, 52, 19, 57, 17, 16, 64, 14, 59, 12, 11, 61, 55, 8, 7, 6, 53, 4, 3, 2, 1];
    //sort::sort(&mut vec);

    //let mut input = [37, 54, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 36, 35, 56, 62, 58, 60, 34, 33, 32, 50, 20, 29, 28, 27, 26, 25, 24, 23, 15, 18, 17, 16, 10, 13, 12, 11, 57, 59, 61, 55, 5, 9, 8, 7, 6, 4, 3, 2, 63, 64, 65, 66, 1, 14, 22, 21, 19, 30, 31, 51, 52, 53, 67];
    //let decision_tree = [29, 13, 54, 9, 18, 31, 62];
    //let classified_elements = 56;
    //let element_count = [9, 4, 5, 11, 2, 23, 8, 5];
    //let mut pointers = [(0, 0); K];
    //let mut boundaries = [0, 12, 16, 20, 32, 32, 56, 64, 64];
    //let mut overflow_buffer = [0; BLOCKSIZE];
    //let len = input.len();
    //permutate_blocks(&mut input, &decision_tree, classified_elements, &element_count, &mut pointers, &mut boundaries, &mut overflow_buffer, 0, len);
    //println!("Input after permutation: {:?}", input);
    //
    //let mut blocks: Vec<Vec<u64>> = vec![vec![1], vec![], vec![14], vec![22, 21, 19], vec![30, 31], vec![51, 52, 53], vec![], vec![67]];
    //
    //cleanup(&mut input, &boundaries, &element_count, &pointers, &mut blocks, &overflow_buffer);
    //println!("Input after cleanup: {:?}", input);

    // RECURSION
    //let mut arr = [5, 9, 8, 7, 6, 4, 3, 2, 1, 13, 12, 11, 10, 17, 16, 14, 15, 18, 21, 22, 26, 25, 24, 23, 20, 29, 28, 27, 19, 31, 30, 51, 43, 42, 41, 40, 47, 46, 45, 44, 39, 38, 36, 35, 37, 54, 49, 48, 34, 33, 32, 50, 53, 52, 61, 55, 56, 62, 58, 60, 57, 59, 64, 63];
    //debug!("Input: {:?}", arr);
    //
    //// buffer for decision tree/pointer/boundaries
    //let mut decision_tree: Vec<u64> = vec![];
    //let mut pointers = [(0, 0); K];
    //let mut boundaries = [0; K+1];
    //
    //// local buffers
    //let mut blocks: Vec<Vec<u64>> = vec![vec![]; K];
    //let mut element_count: [u64; K] = [0; K];
    //
    //sample(&mut arr, &mut decision_tree, 31, 52);
    //debug!("Array after sampling: {:?}", arr);
    //info!("Decision Tree: {:?}", decision_tree);

    //debug!("Overwriting decision tree");
    //decision_tree = vec![29, 13, 54, 9, 18, 31, 62];
    //
    //let classified_elements = classify(&mut arr, &decision_tree, &mut blocks, &mut element_count, 0, arr.len());
    //debug!("Array after classification: {:?}", arr);
    //info!("Classified Elements: {}", classified_elements);
    //info!("Element Count: {:?}", element_count);
    //info!("Blocks: {:?}", blocks);
    //
    //permutate_blocks(&mut arr, &decision_tree, classified_elements, &element_count, &mut pointers, &mut boundaries,0, arr.len());
    //debug!("Array after permutation: {:?}", arr);
    //info!("Pointers: {:?}", pointers);
    //info!("Boundaries: {:?}", boundaries);
    //
    //cleanup(&mut arr, &boundaries, &element_count, &pointers, &mut blocks);
    //debug!("Output: {:?}", arr);

    // create vector with i64 ints from 0 to 9 in a random order
    //let mut vec: Vec<u64> = (0..128).collect();
    //vec.shuffle(&mut thread_rng());
    //debug!("Input: {:?}", vec);
    //debug!("{:?}", vec);
    //let mut vec : Vec<u64> = vec![76, 41, 5, 66, 98, 58, 95, 36, 63, 75, 21, 3, 12, 23, 90, 67, 86, 59, 20, 30, 52, 11, 25, 2, 39, 70, 60, 13, 51, 16, 47, 34, 43, 91, 55, 1, 29, 9, 0, 46, 97, 38, 62, 83, 40, 87, 44, 54, 73, 18, 4, 22, 7, 19, 8, 37, 26, 56, 99, 24, 35, 14, 64, 81, 50, 92, 88, 82, 17, 53, 80, 61, 28, 69, 32, 85, 94, 33, 45, 48, 42, 6, 96, 74, 27, 31, 84, 89, 10, 71, 79, 72, 77, 15, 57, 68, 65, 93, 78, 49];
    //debug!("{}", vec.len());
    //let mut vec: Vec<u64> = (1..=64).rev().collect();


    // LEN 64, BUCKET SIZE 4
    //let mut vec = vec![5, 9, 10, 13, 15, 18, 20, 29, 30, 31, 37, 54, 56, 62, 63, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 58, 36, 35, 34, 33, 32, 60, 50, 51, 28, 27, 26, 25, 24, 23, 22, 21, 52, 19, 57, 17, 16, 64, 14, 59, 12, 11, 61, 55, 8, 7, 6, 53, 4, 3, 2, 1];
    //let decision_tree = vec![29, 13, 54, 9, 18, 31, 62];

    // LEN 128, BUCKET SIZE 8
    //let decision_tree = vec![75, 45, 97, 27, 50, 86, 110];
    //let mut vec = vec![18, 27, 37, 45, 48, 50, 58, 75, 80, 86, 88, 97, 102, 110, 127, 87, 31, 61, 40, 117, 17, 98, 74, 90, 67, 109, 44, 124, 116, 12, 51, 23, 84, 49, 34, 89, 81, 30, 32, 52, 119, 56, 64, 71, 93, 114, 107, 113, 70, 24, 43, 15, 36, 25, 73, 21, 99, 16, 3, 111, 9, 38, 14, 4, 69, 82, 46, 105, 41, 104, 42, 76, 7, 0, 55, 13, 108, 118, 60, 68, 19, 10, 1, 77, 112, 62, 92, 115, 8, 106, 29, 20, 95, 103, 100, 2, 26, 35, 101, 63, 57, 94, 59, 22, 47, 6, 5, 91, 72, 66, 96, 125, 33, 54, 120, 39, 11, 53, 78, 123, 28, 79, 85, 126, 121, 83, 65, 122];


    //let decision_tree = sampling::sample(&mut vec);
    //debug!("Vector after sampling: {:?}", vec);
    //debug!("Decision Tree: {:?}", decision_tree);
    //let (classified_elements, mut blocks, element_count) = classification::classify(&mut vec, &decision_tree);
    //debug!("Blocks: {:?}", blocks);
    //debug!("Element Count: {:?}", element_count);
    ////println!("Classified Elements: {}", classified_elements);
    ////debug!("Vector after classification: {:?}", vec);
    //let (pointer, boundaries) = permutation::permutate_blocks(&mut vec, &decision_tree, classified_elements, &element_count);
    //debug!("Vector after permutation: {:?}", vec);
    //debug!("Pointer: {:?}", pointer);
    //debug!("Boundaries: {:?}", boundaries);

    //cleanup::cleanup(&mut vec, &boundaries, &element_count, &pointer, &mut blocks, &decision_tree);
    //debug!("Vector after cleanup: {:?}", vec);
    //info!("All elements: {:?}", vec);
}
