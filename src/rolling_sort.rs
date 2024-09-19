use std::error::Error;
use log::info;
use vroom::memory::Dma;
use vroom::{NvmeDevice, QUEUE_LENGTH};
use crate::{read_write_hugepage, u8_to_u64_slice, HUGE_PAGE_SIZE_1G, HUGE_PAGE_SIZE_2M, K, LBA_SIZE, THRESHOLD};
use crate::sorter::{DMATask, IPS2RaSorter, Task};

impl IPS2RaSorter{
    pub fn sequential_rolling_sort(&mut self, task: &mut DMATask) {
        if task.level == 0{
            println!("Sampling Task");
            self.sample(task);
        }

        println!("Sequential rolling sort: Start-LBA: {}, Offset: {}, Size: {}, Level: {} ", task.start_lba, task.offset, task.size, task.level);

        if task.size <= HUGE_PAGE_SIZE_1G/8 {
            println!("Task-Size < Hugepage-Size/8 => Sequential sort");
            let qpair = self.qpair.as_mut().unwrap();
            let sort_buffer = self.sort_buffer.as_mut().unwrap();
            read_write_hugepage(qpair, task.start_lba, sort_buffer, false);

            let u64slice= u8_to_u64_slice(&mut sort_buffer[0..task.size*8]);

            let mut new_task = Task::new(u64slice, task.level);

            let mut sorter = IPS2RaSorter::new_sequential(); // TODO: dont allocate new sorter, use self
            sorter.sort_sequential(&mut new_task);

            // write back to ssd
            read_write_hugepage(qpair, task.start_lba, sort_buffer, true);
            return;
        }


        println!("Classification");
        self.classify_ext(task);

        println!("Permutation");
        self.permutate_blocks_ext(task);

        println!("Cleanup");
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
            println!("Added new task. Start LBA: {}, Offset: {}, Size: {}, Level: {}", new_start_lba, new_offset, new_size, task.level+1);
            self.clear();
            self.sequential_rolling_sort(&mut new_task);
        }
    }

    pub fn parallel_rolling_sort() {
        unimplemented!();
    }
}



