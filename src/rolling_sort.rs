use std::error::Error;
use log::info;
use vroom::memory::Dma;
use vroom::{NvmeDevice, QUEUE_LENGTH};
use crate::{read_write_hugepage, u8_to_u64_slice, HUGE_PAGE_SIZE_1G, HUGE_PAGE_SIZE_2M, K, LBA_SIZE, THRESHOLD};
use crate::sorter::{DMATask, IPS2RaSorter, Task};

impl IPS2RaSorter{
    pub fn sequential_rolling_sort(&mut self, task: &mut DMATask) {
        info!("Sequential rolling sort with level {}", task.level);
        if task.size < THRESHOLD { //TODO: change to HUGE_PAGE_SIZE_1G/8
            info!("Task-Size < Hugepage-Size/8 => Sequential sort");
            // load data from ssd
            read_write_hugepage(&mut self.qpair.as_mut().unwrap(), task.start_lba, &mut self.sort_buffer.as_mut().unwrap(), false);
            let u64slice = u8_to_u64_slice(&mut self.sort_buffer.as_mut().unwrap()[0..task.size*8]);
            let mut task = Task::new(u64slice, task.level);
            self.sort_sequential(&mut task);
            return;
        }


        info!("Classification");
        self.classify_ext(task);

        info!("Permutation");
        self.permutate_blocks_ext(task);

        info!("Cleanup");
        self.cleanup_ext(task);


        let element_counts_copy = self.element_counts.clone();
        // Recursion
        let mut sum = 0;
        for i in 0..K {
            let new_size = element_counts_copy[i] as usize;
            if new_size <= 1 {
                continue;
            }
            let new_start_lba = task.start_lba + (task.offset + sum)*8/LBA_SIZE;
            let new_offset = (task.offset + sum)%(LBA_SIZE/8);
            let mut new_task = DMATask::new(new_start_lba, new_offset, new_size, task.level+1);
            info!("Added new task. Start LBA: {}, Offset: {}, Size: {}, Level: {}", new_start_lba, new_offset, new_size, task.level+1);
            self.clear();
            self.sequential_rolling_sort(&mut new_task);
        }
    }

    pub fn parallel_rolling_sort() {
        unimplemented!();
    }
}



