use rayon::prelude::*;
use rayon::scope;
use std::cell::RefCell;
use std::rc::Rc;
use log::debug;
use thread_local::ThreadLocal;
use crate::base_case::insertion_sort;
use crate::sorter::{IPS2RaSorter, Task};

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
                debug!("Thread {} spawning subtasks {:?}", rayon::current_thread_index().unwrap(), task.arr);
                s.spawn(move |_| {
                    process_task(&mut task);
                });
            }
        });
    }
}