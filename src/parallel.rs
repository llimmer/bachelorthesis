use crate::base_case::insertion_sort;
use crate::sorter::{IPS2RaSorter, Task};
use std::cell::RefCell;
use rayon::scope;

thread_local! {
    static SORTER: RefCell<IPS2RaSorter> = RefCell::new(*IPS2RaSorter::new_parallel());
}

pub fn process_task(task: &mut Task) {
    if task.is_base_case() {
        insertion_sort(task.arr);
    } else {
        let element_counts = SORTER.with(
            |sorter| unsafe {
                let mut sorter = sorter.borrow_mut();
                sorter.clear();
                sorter.classify(task);
                sorter.permutate_blocks(task);
                sorter.cleanup(task);
                sorter.element_counts
            }
        );

        scope(|s| {
            for mut task in task.generate_subtasks(&element_counts){
                //println!("Thread {} spawning subtasks", rayon::current_thread_index().unwrap());
                s.spawn(move |_| {
                    //println!("Thread {} spawned", rayon::current_thread_index().unwrap());
                    process_task(&mut task);
                });
            }
        });
    }
}