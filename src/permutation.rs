use std::cmp::max;
use log::debug;
use crate::config::{BLOCKSIZE, K};
use crate::classification::find_block;
use crate::sorter::Sorter;

impl<'a> Sorter<'a> {
    fn calculate_pointers(&mut self) {
        self.boundaries[0] = 0;
        self.pointers[0].0 = 0;
        let mut sum = 0;

        for i in 0..K - 1 {
            // round up to next block
            // TODO: think about type of K and BLOCKSIZE
            sum += self.element_counts[i];

            let mut tmp = sum;
            if tmp % BLOCKSIZE as u64 != 0 {
                tmp += BLOCKSIZE as u64 - (sum % BLOCKSIZE as u64);
            }
            self.boundaries[i + 1] = {
                if tmp <= self.arr.len() as u64 {
                    tmp
                } else {
                    self.arr.len() as u64
                }
            };
            self.pointers[i + 1].0 = tmp as i64;

            if sum <= self.classified_elements as u64 {
                self.pointers[i].1 = (tmp - BLOCKSIZE as u64) as i64;
                //pointers[i].1 = from as i64 + (tmp-BLOCKSIZE as u64) as i64;
            } else {
                self.pointers[i].1 = (self.classified_elements as i64 - BLOCKSIZE as i64 - (self.classified_elements % BLOCKSIZE) as i64);
                //pointers[i].1 = from as i64 + (classified_elements - BLOCKSIZE - classified_elements%BLOCKSIZE) as i64;
            }
        }
        self.boundaries[K] = sum + self.element_counts[K - 1];
        self.pointers[K - 1].1 = max(self.classified_elements as i64 - BLOCKSIZE as i64 - (self.classified_elements % BLOCKSIZE) as i64, 0);
    }
    pub fn permutate_blocks(&mut self) {
        self.calculate_pointers();
        let mut pb: usize = self.primary_bucket;
        let mut swap_buffer = [[0; BLOCKSIZE]; 2];
        let mut swap_buffer_idx: usize = 0;

        // TODO: check if already in correct bucket, think of logic

        'outer: loop {

            // check if block is processed
            if self.pointers[pb].1 < self.pointers[pb].0 {
                pb = (pb+ 1usize) % K;
                // check if cycle is finished
                if pb == self.primary_bucket {
                    break 'outer;
                }
                continue 'outer;
            }

            // decrement read pointers
            self.pointers[pb as usize].1 -= BLOCKSIZE as i64;


            // TODO: check if already in right bucket and read < write, skip in this case

            // read block into swap buffer
            for i in 0..BLOCKSIZE {
                swap_buffer[swap_buffer_idx][i] = self.arr[(self.pointers[pb as usize].1 + BLOCKSIZE as i64 + i as i64) as usize];
            }

            'inner: loop {
                let mut bdest = find_block(swap_buffer[swap_buffer_idx][0], &self.decision_tree) as u64;
                let mut wdest = &mut self.pointers[bdest as usize].0;
                let mut rdest = &mut self.pointers[bdest as usize].1;

                if *wdest <= *rdest {
                    // increment wdest pointers
                    *wdest += BLOCKSIZE as i64;

                    // read block into second swap buffer and write first swap buffer
                    let next_swap_buffer_idx = (swap_buffer_idx + 1) % 2;
                    for i in 0..BLOCKSIZE {
                        swap_buffer[next_swap_buffer_idx][i] = self.arr[*wdest as usize - BLOCKSIZE + i];
                        self.arr[*wdest as usize - BLOCKSIZE + i] = swap_buffer[swap_buffer_idx][i];
                    }
                    swap_buffer_idx = next_swap_buffer_idx;
                } else {
                    *wdest += BLOCKSIZE as i64;
                    if *wdest > self.arr.len() as i64 {
                        // write to overflow buffer
                        debug!("Write to overflow buffer");

                        // TODO: debug, remove later
                        assert_eq!(bdest, compute_overflow_bucket(&self.element_counts) as u64, "Overflow bucket not correct");

                        for i in 0..BLOCKSIZE {
                            self.overflow_buffer.push(swap_buffer[swap_buffer_idx][i]);
                        }
                        self.overflow = true;
                        break 'inner;
                    }
                    // write swap buffer
                    for i in 0..BLOCKSIZE {
                        self.arr[*wdest as usize - BLOCKSIZE + i] = swap_buffer[swap_buffer_idx][i];
                    }
                    break 'inner;
                }
            }
        }
    }
}

pub fn compute_overflow_bucket(element_count: &[u64]) -> u64 {
    for i in 1..=K {
        // TODO: check for > or >=
        if element_count[K - i] >= BLOCKSIZE as u64 {
            return K as u64 - i as u64;
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
        let mut overflow_buffer: Vec<u64> = vec![];

        let length = input.len();
        // TODO: do with Sorter
        //permutate_blocks(&mut input, &decision_tree, classified_elements, &element_count, &mut pointers, &mut boundaries, &mut overflow_buffer, 0, length);

        //println!("Pointers: {:?}", pointers);
        let expected_pointers = [(8, -4), (16, 12), (20, 16), (28, 20), (32, 28), (52, 48), (64, 48), (64, 48)];
        for i in 0..K {
            assert_eq!(pointers[i], expected_pointers[i]);
        }
    }
}

/*pub fn sorter_permutate(sorter: &mut Sorter){
    permutate_blocks(sorter.arr, sorter.decision_tree, sorter.classified_elements as usize, sorter.element_count, sorter.pointers, sorter.boundaries, sorter.overflow_buffer, sorter.from, sorter.to);
}

pub fn permutate_blocks(input: &mut [u64], decision_tree: &[u64], classified_elements: usize, element_count: &[u64], pointers: &mut [(i64, i64); K], boundaries: &mut [u64; K + 1], overflow_buffer: &mut Vec<u64>, from: usize, to: usize) {
    calculate_pointers(classified_elements, &element_count, pointers, boundaries, from, to);
    _permutate_blocks(input, decision_tree, pointers, overflow_buffer, 0, from, to);
}

fn _permutate_blocks(input: &mut [u64], decision_tree: &[u64], pointers: &mut [(i64, i64); K], overflow_buffer: &mut Vec<u64>, primary_bucket: u64, from: usize, to: usize) {
    let mut pb: u64 = primary_bucket;
    let mut swap_buffer = [[0; BLOCKSIZE]; 2];
    let mut swap_buffer_idx: usize = 0;

    // TODO: check if already in correct bucket, think of logic

    'outer: loop {

        // check if block is processed
        if pointers[pb as usize].1 < pointers[pb as usize].0 {
            pb = (pb + 1) % K as u64;
            // check if cycle is finished
            if pb == primary_bucket {
                break 'outer;
            }
            continue 'outer;
        }

        // decrement read pointers
        pointers[pb as usize].1 -= BLOCKSIZE as i64;


        // TODO: check if already in right bucket and read < write, skip in this case

        // read block into swap buffer
        for i in 0..BLOCKSIZE {
            swap_buffer[swap_buffer_idx][i] = input[(pointers[pb as usize].1 + BLOCKSIZE as i64 + i as i64) as usize];
        }

        'inner: loop {
            let mut bdest = find_block(swap_buffer[swap_buffer_idx][0], decision_tree) as u64;
            let mut wdest = &mut pointers[bdest as usize].0;
            let mut rdest = &mut pointers[bdest as usize].1;

            if *wdest <= *rdest {
                // increment wdest pointers
                *wdest += BLOCKSIZE as i64;

                // read block into second swap buffer and write first swap buffer
                let next_swap_buffer_idx = (swap_buffer_idx + 1) % 2;
                for i in 0..BLOCKSIZE {
                    swap_buffer[next_swap_buffer_idx][i] = input[*wdest as usize - BLOCKSIZE + i];
                    input[*wdest as usize - BLOCKSIZE + i] = swap_buffer[swap_buffer_idx][i];
                }
                swap_buffer_idx = next_swap_buffer_idx;
            } else {
                *wdest += BLOCKSIZE as i64;
                if *wdest > to as i64 {
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
}*/