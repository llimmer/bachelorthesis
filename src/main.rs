use log::LevelFilter;
use log::{debug, info, warn, error};
use rand::prelude::SliceRandom;
use rand::thread_rng;
use crate::cleanup::cleanup;

mod sampling;
mod base_case;
mod classification;
mod config;
mod permutation;
mod cleanup;
mod sort;

fn main() {
    env_logger::builder()
        .filter_level(LevelFilter::Debug)
        .init();

    //let mut arr: Vec<u32> = (1..65).collect();
    //arr.shuffle(&mut thread_rng());
    //sort::sort(&mut arr);

    let mut vec = vec![5, 9, 10, 13, 15, 18, 20, 29, 30, 31, 37, 54, 56, 62, 63, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 58, 36, 35, 34, 33, 32, 60, 50, 51, 28, 27, 26, 25, 24, 23, 22, 21, 52, 19, 57, 17, 16, 64, 14, 59, 12, 11, 61, 55, 8, 7, 6, 53, 4, 3, 2, 1];
    sort::sort(&mut vec);


    // create vector with i32 ints from 0 to 9 in a random order
    //let mut vec: Vec<u32> = (0..128).collect();
    //vec.shuffle(&mut thread_rng());
    //debug!("Input: {:?}", vec);
    //debug!("{:?}", vec);
    //let mut vec : Vec<u32> = vec![76, 41, 5, 66, 98, 58, 95, 36, 63, 75, 21, 3, 12, 23, 90, 67, 86, 59, 20, 30, 52, 11, 25, 2, 39, 70, 60, 13, 51, 16, 47, 34, 43, 91, 55, 1, 29, 9, 0, 46, 97, 38, 62, 83, 40, 87, 44, 54, 73, 18, 4, 22, 7, 19, 8, 37, 26, 56, 99, 24, 35, 14, 64, 81, 50, 92, 88, 82, 17, 53, 80, 61, 28, 69, 32, 85, 94, 33, 45, 48, 42, 6, 96, 74, 27, 31, 84, 89, 10, 71, 79, 72, 77, 15, 57, 68, 65, 93, 78, 49];
    //debug!("{}", vec.len());
    //let mut vec: Vec<u32> = (1..=64).rev().collect();


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
