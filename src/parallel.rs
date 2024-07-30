use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use log::debug;
use crate::base_case::insertion_sort;
use crate::config::{K, THRESHOLD};
use crate::sorter::IPS4oSorter;

pub(crate) fn sort_parallel(task_queue: Arc<Mutex<VecDeque<Box<IPS4oSorter>>>>,
                            task_counter: Arc<AtomicUsize>,
                            thread_counter: Arc<AtomicUsize>) {
    'outer: loop {
        let task = {
            let mut queue = task_queue.lock().unwrap();
            queue.pop_front()
        };

        if let Some(mut task) = task {
            thread_counter.fetch_add(1, Ordering::SeqCst);

            // Process the task
            if task.arr.len() as i64 <= THRESHOLD as i64 {
                insertion_sort(task.arr);
                task_counter.fetch_sub(1, Ordering::SeqCst);
                thread_counter.fetch_sub(1, Ordering::SeqCst);
                debug!("{:?} processed base case {:?}", thread::current().id(), task.arr);
                continue 'outer;
            }
            task.sample();
            task.classify();
            task.permutate_blocks();
            task.cleanup();

            debug!("{:?} processed task {}", thread::current().id(), task);

            // Recursion:
            task_counter.fetch_add(K, Ordering::SeqCst);
            let mut sum = 0;

            // add new tasks to queue
            let mut all = task.arr;
            for i in 0..K - 1 {
                let (current, next) = all.split_at_mut(task.element_counts[i] as usize);
                all = next;
                debug!("{:?} adding task {:?} to queue", thread::current().id(), current);
                let mut new_struct = IPS4oSorter::new_parallel(current);
                {
                    let mut queue = task_queue.lock().unwrap();
                    queue.push_back(new_struct);
                }
            }
            debug!("{:?} adding task {:?} to queue", thread::current().id(), all);
            {
                let mut queue = task_queue.lock().unwrap();
                queue.push_back(IPS4oSorter::new_parallel(all));
            }


            task_counter.fetch_sub(1, Ordering::SeqCst);
            thread_counter.fetch_sub(1, Ordering::SeqCst);
        } else {
            if task_counter.load(Ordering::SeqCst) == 0 && thread_counter.load(Ordering::SeqCst) == 0 {
                break;
            }
            thread::yield_now();
        }
    }
}