use log::{debug, info};
use crate::config::{K, THRESHOLD};
use crate::insertion_sort;
use crate::sorter::{IPS2RaSorter, Task};

impl IPS2RaSorter {
    pub(crate) fn sort_sequential(&mut self, task: &mut Task) {
        if task.arr.len() as i64 <= THRESHOLD as i64 {
            debug!("Base case: {:?}", task.arr);
            insertion_sort(task.arr);
            return;
        }
        debug!("Input: {:?}", task.arr);

        unsafe{self.classify(task)};
        debug!("Array after classification: {:?}", task.arr);
        debug!("Classified Elements: {}", self.classified_elements);
        debug!("Element Count: {:?}", self.element_counts);
        debug!("Blocks: {:?}", self.blocks);

        self.permutate_blocks(task);
        debug!("Array after permutation: {:?}", task.arr);
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
            let mut new_task = Task::new(&mut task.arr[start as usize..sum as usize], task.level+1);
            info!("Added new task from {start} to {sum}, level: {}", task.level+1);
            self.clear();
            self.sort_sequential(&mut new_task);
        }
    }
}