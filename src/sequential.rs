use log::{debug, info};
use crate::config::{K, THRESHOLD};
use crate::insertion_sort;
use crate::sorter::{IPS2RaSorter, IPS4oSorter};

impl<'a> IPS4oSorter<'a> {
    pub(crate) fn sort_sequential(&mut self) {
        if self.arr.len() as i64 <= THRESHOLD as i64 {
            debug!("Base case: {:?}", self.arr);
            insertion_sort(self.arr);
            return;
        }
        debug!("Input: {:?}", self.arr);

        self.sample();
        debug!("Array after sampling: {:?}", self.arr);
        info!("Decision Tree: {:?}", self.decision_tree);


        self.classify();
        debug!("Array after classification: {}", self);
        info!("Classified Elements: {}", self.classified_elements);
        info!("Element Count: {:?}", self.element_counts);
        info!("Blocks: {:?}", self.blocks);

        self.permutate_blocks();
        debug!("Array after permutation: {}", self);
        info!("Pointers: {:?}", self.pointers);
        info!("Boundaries: {:?}", self.boundaries);
        info!("Overflow Buffer: {:?}", self.overflow_buffer);

        self.cleanup();

        debug!("{}", self);


        // RECURSION:
        let mut sum = 0;
        for i in 0..K {
            let start = sum;
            sum += self.element_counts[i];
            let mut new_struct = IPS4oSorter::new_sequential(&mut self.arr[start as usize..sum as usize]);
            new_struct.sort_sequential();
        }
    }
}

impl<'a> IPS2RaSorter<'a> {
    pub(crate) fn sort_sequential(&mut self) {
        if self.arr.len() as i64 <= THRESHOLD as i64 {
            debug!("Base case: {:?}", self.arr);
            insertion_sort(self.arr);
            return;
        }
        debug!("Input: {:?}", self.arr);

        self.classify();
        debug!("Array after classification: {}", self);
        info!("Classified Elements: {}", self.classified_elements);
        info!("Element Count: {:?}", self.element_counts);
        info!("Blocks: {:?}", self.blocks);

        self.permutate_blocks();
        debug!("Array after permutation: {}", self);
        info!("Pointers: {:?}", self.pointers);
        info!("Boundaries: {:?}", self.boundaries);
        info!("Overflow Buffer: {:?}", self.overflow_buffer);

        self.cleanup();

        debug!("{}", self);


        // RECURSION:
        let mut sum = 0;
        for i in 0..K {
            let start = sum;
            sum += self.element_counts[i];
            let mut new_struct = IPS2RaSorter::new_sequential(&mut self.arr[start as usize..sum as usize], self.level+1);
            new_struct.sort_sequential();
        }
    }
}