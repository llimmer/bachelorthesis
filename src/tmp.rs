use crate::config::{K, BLOCKSIZE};
use crate::sampling::{sorter_sample};
use crate::classification::{sorter_classify};
use crate::permutation::{sorter_permutate};
use crate::cleanup::{sorter_cleanup};


pub(crate) struct Sorter<'a> {
    pub arr: &'a mut [u32],
    pub from: usize,
    pub to: usize,
    pub decision_tree: &'a mut Vec<u32>,
    pub classified_elements: u32,
    pub pointers: &'a mut [(i32, i32); K],
    pub boundaries: &'a mut [u32; K + 1],

    // TODO: see how to handle threads here
    // local buffers
    pub blocks: &'a mut Vec<Vec<u32>>,
    pub element_count: &'a mut [u32; K],
    pub overflow_buffer: &'a mut Vec<u32>,
}

impl Sorter<'_> {
    pub fn make_sorter_bound(arr: &mut [u32], from: usize, to: usize) -> Box<Sorter> {
        let res = Sorter {
            arr,
            from,
            to,
            decision_tree: &mut vec![],
            classified_elements: 0,
            pointers: &mut [(0, 0); K],
            boundaries: &mut [0; K+1],
            blocks: &mut vec![vec![]; K],
            element_count: &mut [0; K],
            overflow_buffer: &mut vec![],
        };
        res.overflow_buffer.reserve(BLOCKSIZE);
        Box::new(res)
    }

    pub fn make_sorter(arr: &mut [u32]) -> Box<Sorter> {
        Self::make_sorter_bound(arr, 0, arr.len())
    }

    pub fn sort(&mut self) {
        sorter_sample(&mut self);
        self.classified_elements = sorter_classify(&mut self);
        sorter_permutate(&mut self);
        sorter_cleanup(&mut self);

        // Recursion
        // TODO: do smarter (only change from/to)

        // RECURSION:
        let mut sum: u32 = self.from as u32;
        for i in 0..K {
            let start = sum;
            sum += self.element_count[i];
            let end = sum;

            let mut new_sorter = Self::make_sorter_bound(self.arr, start as usize, end as usize);
            new_sorter.sort();
        }
    }
}