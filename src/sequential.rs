use log::{debug, info};
use crate::config::{K, THRESHOLD};
use crate::insertion_sort;
use crate::sorter::{IPS2RaSorter, Task};

impl IPS2RaSorter {
    pub(crate) fn sort_sequential(&mut self, task: &mut Task) {
        if task.data.len() as i64 <= THRESHOLD as i64 {
            debug!("Base case: {:?}", task.data);
            insertion_sort(task.data);
            return;
        }
        debug!("Input: {:?}", task.data);

        unsafe{self.classify(task)};
        debug!("Array after classification: {:?}", task.data);
        debug!("Classified Elements: {}", self.classified_elements);
        debug!("Element Count: {:?}", self.element_counts);
        debug!("Blocks: {:?}", self.blocks);

        self.permutate_blocks(task);
        debug!("Array after permutation: {:?}", task.data);
        debug!("Pointers: {:?}", self.pointers);
        debug!("Boundaries: {:?}", self.boundaries);
        debug!("Overflow Buffer: {:?}", self.overflow_buffer);

        self.cleanup(task);

        debug!("{}", self.to_string(task));

        let element_counts_copy = self.element_counts.clone();
        // RECURSION:
        let mut sum = 0;
        for i in 0..K {
            let start = sum;
            sum += element_counts_copy[i];
            if element_counts_copy[i] <= 1 {
                continue;
            }
            let mut new_task = Task::new(&mut task.data[start as usize..sum as usize], task.level+1);
            self.clear();
            self.sort_sequential(&mut new_task);
        }
    }
}