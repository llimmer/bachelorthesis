use log::debug;
use crate::config::{BLOCKSIZE, K};
use crate::classification::find_block;

pub fn permutate_blocks(input: &mut [u32], decision_tree: &[u32], classified_elements: usize, element_count: &[u32], pointers: &mut [(i32, i32); K], boundaries: &mut [u32; K + 1], overflow_buffer: &mut Vec<u32>, from: usize, to: usize) {
    calculate_pointers(classified_elements, &element_count, pointers, boundaries, from, to);
    _permutate_blocks(input, decision_tree, pointers, overflow_buffer, 0, from, to);
}

fn _permutate_blocks(input: &mut [u32], decision_tree: &[u32], pointer: &mut [(i32, i32); K], overflow_buffer: &mut Vec<u32>, primary_bucket: u32, from: usize, to: usize) {
    let mut pb: u32 = primary_bucket;
    let mut swap_buffer = [[0; BLOCKSIZE]; 2];
    let mut swap_buffer_idx: usize = 0;

    // TODO: check if already in correct bucket, think of logic

    'outer: loop {

        // check if block is processed
        if pointer[pb as usize].1 < pointer[pb as usize].0 {
            pb = (pb + 1) % K as u32;
            // check if cycle is finished
            if pb == primary_bucket {
                break 'outer;
            }
            continue 'outer;
        }

        // decrement read pointer
        pointer[pb as usize].1 -= BLOCKSIZE as i32;


        // TODO: check if already in right bucket and read < write, skip in this case

        // read block into swap buffer
        for i in 0..BLOCKSIZE {
            swap_buffer[swap_buffer_idx][i] = input[(pointer[pb as usize].1 + BLOCKSIZE as i32 + i as i32) as usize];
        }

        'inner: loop {
            let mut bdest = find_block(swap_buffer[swap_buffer_idx][0], decision_tree) as u32;
            let mut wdest = &mut pointer[bdest as usize].0;
            let mut rdest = &mut pointer[bdest as usize].1;

            if *wdest <= *rdest {
                // increment wdest pointer
                *wdest += BLOCKSIZE as i32;

                // read block into second swap buffer and write first swap buffer
                let next_swap_buffer_idx = (swap_buffer_idx + 1) % 2;
                for i in 0..BLOCKSIZE {
                    swap_buffer[next_swap_buffer_idx][i] = input[*wdest as usize - BLOCKSIZE + i];
                    input[*wdest as usize - BLOCKSIZE + i] = swap_buffer[swap_buffer_idx][i];
                }
                swap_buffer_idx = next_swap_buffer_idx;
            } else {
                *wdest += BLOCKSIZE as i32;
                if *wdest > to as i32 {
                    // write to overflow buffer
                    debug!("Write to overflow buffer");
                    for i in 0..BLOCKSIZE {
                        overflow_buffer.push(swap_buffer[swap_buffer_idx][i]);
                    }
                    break 'inner;
                }
                // write swap buffer
                for i in 0..BLOCKSIZE {
                    input[*wdest as usize - BLOCKSIZE + i] = swap_buffer[swap_buffer_idx][i];
                }
                break 'inner;
            }
        }
    }
}

fn calculate_pointers(classified_elements: usize, element_count: &[u32], pointers: &mut [(i32, i32); K], boundaries: &mut [u32; K + 1], from: usize, to: usize) {
    boundaries[0] = from as u32;
    pointers[0].0 = from as i32;
    let mut sum = 0;

    for i in 0..K - 1 {
        // round up to next block
        // TODO: think about type of K and BLOCKSIZE
        sum += element_count[i];

        let mut tmp = sum;
        if tmp % BLOCKSIZE as u32 != 0 {
            tmp += BLOCKSIZE as u32 - (sum % BLOCKSIZE as u32);
        }
        boundaries[i + 1] = {
            if from as u32 + tmp <= to as u32 {
                from as u32 + tmp
            } else {
                to as u32
            }
        };
        pointers[i + 1].0 = from as i32 + tmp as i32;

        if sum <= classified_elements as u32 {
            pointers[i].1 = from as i32 + (tmp - BLOCKSIZE as u32) as i32;
            //pointers[i].1 = from as i32 + (tmp-BLOCKSIZE as u32) as i32;
        } else {
            pointers[i].1 = from as i32 + (classified_elements as i32 - BLOCKSIZE as i32 - (classified_elements % BLOCKSIZE) as i32);
            //pointers[i].1 = from as i32 + (classified_elements - BLOCKSIZE - classified_elements%BLOCKSIZE) as i32;
        }
    }
    boundaries[K] = from as u32 + sum + element_count[K - 1];
    pointers[K - 1].1 = {
        let index = from as i32 + (classified_elements as i32 - BLOCKSIZE as i32 - (classified_elements % BLOCKSIZE) as i32);
        if index < from as i32 {
            from as i32
        } else {
            index
        }
    };
    println!("Pointers before permutation: {:?}", pointers);
}

pub fn compute_overflow_bucket(element_count: &[u32]) -> u32 {
    for i in 1..=K {
        // TODO: check for > or >=
        if element_count[K - i] > BLOCKSIZE as u32 {
            return K as u32 - i as u32;
        }
    }
    return 0;
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(BLOCKSIZE, 4, "BLOCKSIZE 4 required for this test");
        assert_eq!(K, 8, "8 buckets required for this test");

        let mut input = [37, 54, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 36, 35, 56, 62, 58, 60, 34, 33, 32, 50, 20, 29, 28, 27, 26, 25, 24, 23, 15, 18, 17, 16, 10, 13, 12, 11, 57, 59, 61, 55, 5, 9, 8, 7, 6, 4, 3, 2, 1, 14, 22, 21, 19, 30, 31, 51, 52, 53, 63, 64];
        let decision_tree = [29, 13, 54, 9, 18, 31, 62];
        let classified_elements = 52;
        let element_count = [9, 4, 5, 11, 2, 23, 8, 2];
        let mut pointers = [(0, 0); K];
        let mut boundaries = [0; K + 1];
        let mut overflow_buffer = vec![];

        let length = input.len();
        permutate_blocks(&mut input, &decision_tree, classified_elements, &element_count, &mut pointers, &mut boundaries, &mut overflow_buffer, 0, length);

        //println!("Pointers: {:?}", pointers);
        let expected_pointers = [(8, -4), (16, 12), (20, 16), (28, 20), (32, 28), (52, 48), (64, 48), (64, 48)];
        for i in 0..K {
            assert_eq!(pointers[i], expected_pointers[i]);
        }
    }
}