use crate::config::{BLOCKSIZE, K};
use crate::classification::find_block;
use crate::permutation::compute_overflow_bucket;

pub fn cleanup(input: &mut [u32], boundaries: &[u32; K + 1], element_count: &[u32; K], pointers: &[(i32, i32); K], blocks: &mut Vec<Vec<u32>>, overflow_buffer: &mut Vec<u32>, from: usize, to: usize) {
    let mut sum = from as u32;

    let overflow_bucket = compute_overflow_bucket(element_count);

    for i in 0..K - 1 {
        let write_ptr = pointers[i].0;
        sum += element_count[i];


        if write_ptr < sum as i32 || write_ptr <= boundaries[i] as i32 || write_ptr > to as i32 {
            continue;
        }


        for j in 1..=(write_ptr - sum as i32) {
            // TODO: blocks can be full (>BLOCKSIZE), paper suggests using swap buffer
            let element = input[(write_ptr - j) as usize];
            blocks[i].push(element);

            // TODO: debug, remove later
            //assert!(blocks[i].len() <= BLOCKSIZE as usize, "Block size exceeded");
        }
    }

    // write block elements back to input
    sum = from as u32;
    for i in 0..K - 1 {

        // Overflow case:
        if i == overflow_bucket as usize {
            // write overflow block:
            // fill back:
            let mut len_back = to as i32 - pointers[overflow_bucket as usize].0;
            if len_back > 0 {
                for i in 0..len_back as usize {
                    if !blocks[overflow_bucket as usize].is_empty() {
                        input[to - i - 1] = blocks[overflow_bucket as usize].pop().unwrap();
                    }
                }
            } else {
                len_back = to as i32 - (pointers[overflow_bucket as usize].0 - BLOCKSIZE as i32);
                assert!(len_back > 0, "len_back is negative");
                for i in 0..len_back as usize {
                    if !overflow_buffer.is_empty() {
                        input[to - i - 1] = overflow_buffer.pop().unwrap();
                    }
                }
            }

            // fill front
            while !blocks[overflow_bucket as usize].is_empty() {
                input[sum as usize] = blocks[overflow_bucket as usize].pop().unwrap();
                sum += 1;
            }
            while !overflow_buffer.is_empty() {
                input[sum as usize] = overflow_buffer.pop().unwrap();
                sum += 1;
            }
            continue;
        }

        let mut start = sum;
        sum += element_count[i];

        // fill the back
        let mut write_idx = pointers[i].0 as usize;
        while write_idx < sum as usize {
            input[write_idx] = blocks[i].pop().unwrap();
            write_idx += 1;
        }

        // fill the front
        while !blocks[i].is_empty() {
            input[start as usize] = blocks[i].pop().unwrap();
            start += 1;
        }
    }
}


#[cfg(test)]
mod tests {
    use log::debug;
    use super::*;

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

    fn check_blockidx(input: &[u32], element_count: &[u32], decision_tree: &[u32]) {
        let mut sum: u32 = 0;
        for i in 0..K {
            let mut start = sum;
            sum += element_count[i];
            for j in start..sum {
                let block_idx = find_block(input[j as usize], &decision_tree);
                assert_eq!(block_idx, i);
            }
        }
    }

    #[test]
    fn test_small() {
        let mut input = [5, 9, 8, 7, 6, 4, 3, 2, 43, 42, 41, 40, 10, 13, 12, 11, 15, 18, 17, 16, 26, 25, 24, 23, 20, 29, 28, 27, 26, 25, 24, 23, 43, 42, 41, 40, 47, 46, 45, 44, 39, 38, 36, 35, 37, 54, 49, 48, 34, 33, 32, 50, 1, 14, 22, 21, 56, 62, 58, 60, 57, 59, 61, 55];
        let mut blocks: Vec<Vec<u32>> = vec![vec![1], vec![], vec![14], vec![22, 21, 19], vec![30, 31], vec![51, 52, 53], vec![], vec![63, 64]];
        let decision_tree = vec![29, 13, 54, 9, 18, 31, 62];
        let element_count = [9, 4, 5, 11, 2, 23, 8, 2];
        let boundaries = [0, 12, 16, 20, 32, 32, 56, 64, 64];
        let pointers = [(8, 0), (16, 12), (20, 16), (28, 24), (32, 28), (52, 48), (64, 48), (64, 48)];
        let mut overflow_buffer = vec![];
        cleanup(&mut input, &boundaries, &element_count, &pointers, &mut blocks, &mut overflow_buffer);

        check_blockidx(&input, &element_count, &decision_tree);

        check_range(&input, 1, 64);

        println!("{:?}", input)
    }

    #[test]
    fn test_big() {
        let mut input = [18, 27, 17, 12, 23, 24, 15, 25, 13, 19, 10, 1, 8, 20, 2, 26, 21, 16, 3, 9, 14, 4, 7, 0, 127, 117, 124, 116, 119, 114, 113, 111, 37, 45, 31, 40, 44, 34, 30, 32, 43, 36, 38, 41, 42, 29, 35, 33, 64, 71, 70, 73, 69, 55, 60, 68, 58, 75, 61, 74, 67, 51, 52, 56, 62, 63, 57, 59, 72, 66, 54, 53, 64, 71, 70, 73, 69, 55, 60, 68, 80, 86, 84, 81, 82, 76, 77, 78, 88, 97, 87, 90, 89, 93, 92, 95, 118, 112, 115, 125, 120, 123, 126, 121, 102, 110, 98, 109, 107, 99, 105, 104, 127, 117, 124, 116, 119, 114, 113, 111, 118, 112, 115, 125, 120, 123, 126, 121];

        let mut blocks: Vec<Vec<u32>> = vec![vec![22, 6, 5, 11], vec![39, 28], vec![48, 50, 49, 46, 47], vec![65], vec![79, 85, 83], vec![94, 91, 96], vec![108, 106, 103, 100, 101], vec![122]];
        let decision_tree = [75, 45, 97, 27, 50, 86, 110];
        let element_count = [28, 18, 5, 25, 11, 11, 13, 17];
        let boundaries = [0, 32, 48, 56, 80, 88, 104, 112, 128];
        let pointer = [(24, 0), (48, 32), (48, 40), (80, 72), (88, 80), (96, 88), (112, 96), (128, 96)];
        let mut overflow_buffer = vec![];
        cleanup(&mut input, &boundaries, &element_count, &pointer, &mut blocks, &mut overflow_buffer);
        println!("{:?}", input);

        check_blockidx(&input, &element_count, &decision_tree);

        check_range(&input, 0, 127);
    }
}


// 'outer: for i in 0..K {
//         sum += element_count[i];
//         let last_element = boundaries[i + 1] as usize;
//         let max = max(pointers[i].0, pointers[i].1 + BLOCKSIZE as i32);
//
//         // TODO: only for debug, remove later
//         if max < 0 {
//             panic!("Max is negative")
//         }
//         let mut umax = max as u32;
//
//         if umax <= last_element as u32 {
//             continue;
//         } else {
//             let bucket = find_block(input[umax as usize - 1 as usize], decision_tree);
//             if bucket != i {
//                 continue 'outer;
//             }
//             for j in 1..=(umax - sum) {
//                 let element = input[umax as usize - j as usize];
//                 blocks[i].push(element);
//
//                 // TODO: debug, remove later
//                 if blocks[i].len() > BLOCKSIZE as usize {
//                     panic!("Block size exceeded")
//                 }
//             }
//         }
//     }