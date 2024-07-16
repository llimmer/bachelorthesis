use log::debug;
use crate::config::{BLOCKSIZE, K};
use crate::classification::find_block;

pub fn permutate_blocks(input: &mut [u32], decision_tree: &[u32], classified_elements: usize, element_count: &[u32], pointers: &mut [(i32, i32); K], boundaries: &mut [u32; K+1]) {
    calculate_pointers(classified_elements, &element_count, pointers, boundaries);
    _permutate_blocks(input, decision_tree, pointers, 0);
}

fn _permutate_blocks(input: &mut [u32], decision_tree: &[u32], pointer: &mut [(i32, i32); K], primary_bucket: u32){
    let mut pb: u32 = primary_bucket;
    let mut swap_buffer = [[0; BLOCKSIZE]; 2];
    let mut swap_buffer_idx: usize = 0;

    // TODO: check if already in correct bucket, think of logic

    'outer: loop{

        // check if block is processed
        if pointer[pb as usize].1 < pointer[pb as usize].0{
            pb = (pb+1) % K as u32;
            // check if cycle is finished
            if pb == primary_bucket{
                break 'outer;
            }
            continue 'outer;
        }

        // decrement read pointer
        pointer[pb as usize].1 -= BLOCKSIZE as i32;


        // TODO: check if already in right bucket and read < write, skip in this case

        // read block into swap buffer
        for i in 0..BLOCKSIZE{
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
                for i in 0..BLOCKSIZE{
                    swap_buffer[next_swap_buffer_idx][i] = input[*wdest as usize - BLOCKSIZE + i];
                    input[*wdest as usize - BLOCKSIZE + i] = swap_buffer[swap_buffer_idx][i];
                }
                swap_buffer_idx = next_swap_buffer_idx;
            } else {
                *wdest += BLOCKSIZE as i32;
                // write swap buffer
                for i in 0..BLOCKSIZE{
                    input[*wdest as usize - BLOCKSIZE + i] = swap_buffer[swap_buffer_idx][i];
                }
                break 'inner;
            }
        }

    }
}

fn calculate_pointers(classified_elements: usize, element_count: &[u32], pointers: &mut [(i32, i32); K], boundaries: &mut [u32; K+1]) {
    boundaries[0] = 0;
    let mut sum = 0;

    for i in 0..K-1 {
        // round up to next block
        // TODO: think about type of K and BLOCKSIZE
        sum += element_count[i];

        let mut tmp = sum;
        if tmp % BLOCKSIZE as u32 != 0 {
            tmp += BLOCKSIZE as u32 - (sum % BLOCKSIZE as u32);
        }
        boundaries[i+1] = tmp;
        pointers[i+1].0 = tmp as i32;

        if sum <= classified_elements as u32 {
            pointers[i].1 = (tmp-BLOCKSIZE as u32) as i32;
        } else {
            pointers[i].1 = (classified_elements - BLOCKSIZE - classified_elements%BLOCKSIZE) as i32;
        }
    }
    boundaries[K] = sum+element_count[K-1];
    pointers[K-1].1 = (classified_elements - BLOCKSIZE - classified_elements%BLOCKSIZE) as i32;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut input = [37, 54, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 36, 35, 56, 62, 58, 60, 34, 33, 32, 50, 20, 29, 28, 27, 26, 25, 24, 23, 15, 18, 17, 16, 10, 13, 12, 11, 57, 59, 61, 55, 5, 9, 8, 7, 6, 4, 3, 2, 1, 14, 22, 21, 19, 30, 31, 51, 52, 53, 63, 64];
        let decision_tree = [29, 13, 54, 9, 18, 31, 62];
        let classified_elements = 52;
        let element_count = [9, 4, 5, 11, 2, 23, 8, 2];
        let mut pointers = [(0, 0); K];
        let mut boundaries = [0; K+1];

        permutate_blocks(&mut input, &decision_tree, classified_elements, &element_count, &mut pointers, &mut boundaries);

        //println!("Pointers: {:?}", pointers);
        let expected_pointers = [(8, -4), (16, 12), (20, 16), (28, 20), (32, 28), (52, 48), (64, 48), (64, 48)];
        for i in 0..K {
            assert_eq!(pointers[i], expected_pointers[i]);
        }

    }
}