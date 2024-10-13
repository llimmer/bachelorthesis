use crate::config::*;
use crate::sorter::{IPS2RaSorter, Task};

impl IPS2RaSorter {
    pub fn sort_sequential(&mut self, task: &mut Task) {

        // partition
        self.classify(task);
        self.permutate_blocks(task);
        self.cleanup(task);

        if task.level + 1 == task.level_end {
            //println!("Last level -> sorted");
            return;
        }

        // RECURSION:
        let bucket_start = self.boundaries.clone();
        for i in 0..K {
            let start = bucket_start[i];
            let end = bucket_start[i + 1];
            if (end - start) > THRESHOLD as u64 {
                //println!("New task: start: {}, end: {}, level: {}", start, end, task.level + 1);
                let mut new_task = Task::new(&mut task.arr[start as usize..end as usize], task.level + 1, task.level_start, task.level_end);
                self.clear();
                self.sort_sequential(&mut new_task);
            }
        }
    }
}
