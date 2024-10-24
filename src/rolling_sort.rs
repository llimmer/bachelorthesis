use crate::config::*;
use crate::conversion::*;
use crate::sort::{read_write_hugepage_1G};
use crate::sorter::{ExtTask, IPS2RaSorter, Task};


impl IPS2RaSorter{
    pub fn sequential_rolling_sort(&mut self, task: &mut ExtTask) {
        if task.level == 0{
            println!("Sampling Task");
            self.sample(task);
        }
        println!("Sequential rolling sort: Start-LBA: {}, Offset: {}, Size: {}, Level: {} ", task.start_lba, task.offset, task.size, task.level);

        //read_write_hugepage(self.qpair.as_mut().unwrap(), task.start_lba, self.sort_buffer.as_mut().unwrap(), false);
        //let u64slice= u8_to_u64_slice(&mut self.sort_buffer.as_mut().unwrap()[0..task.size*8]);
        //println!("Task before: {:?}", u64slice);

        // read line from stdin
        //let mut input = String::new();
        //io::stdin().read_line(&mut input).unwrap();

        if task.size <= HUGE_PAGE_SIZE_2M/8 {
            {
                println!("Task-Size < Hugepage-Size/8 => Sequential sort");
                let qpair = self.qpair.as_mut().unwrap();
                let sort_buffer = self.sort_buffer.as_mut().unwrap();
                read_write_hugepage_1G(qpair, task.start_lba, sort_buffer, false);

                let u64slice = u8_to_u64_slice(&mut sort_buffer[0..task.size * 8]);
                println!("Read: {:?}", u64slice);

                let mut new_task = Task::new(u64slice, task.level, task.level_end);

                let mut sorter = IPS2RaSorter::new_sequential(); // TODO: dont allocate new sorter, use self
                sorter.sequential_rec(&mut new_task);

                println!("After sort: {:?}", new_task.arr);
                // write back to ssd
                read_write_hugepage_1G(qpair, task.start_lba, sort_buffer, true);
                return;
            }
        }


        println!("Classification");
        self.classify_ext(task);
        println!("Classified elements: {}", self.classified_elements);

        println!("Permutation");
        self.permutate_blocks_ext(task);

        println!("Cleanup");
        self.cleanup_ext(task);

        //read_write_hugepage(self.qpair.as_mut().unwrap(), task.start_lba, self.sort_buffer.as_mut().unwrap(), false);
        //let u64slice= u8_to_u64_slice(&mut self.sort_buffer.as_mut().unwrap()[0..task.size*8]);
        //println!("Task after: {:?}", u64slice);

        if task.level + 1 == task.level_end {
            println!("Last level -> sorted");
            return;
        }

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
            let mut new_task = ExtTask::new(new_start_lba, new_offset, new_size, task.level+1, task.level_end);
            println!("Added new task. Start LBA: {}, Offset: {}, Size: {}, Level: {}", new_start_lba, new_offset, new_size, task.level+1);
            self.clear();
            self.sequential_rolling_sort(&mut new_task);
            sum += element_counts_copy[i] as usize;
        }
    }

    pub fn parallel_rolling_sort() {
        unimplemented!();
    }
}



