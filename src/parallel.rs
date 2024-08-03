use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use log::{debug, info};
use crate::base_case::insertion_sort;
use crate::config::{K, THRESHOLD};
use crate::sorter::{IPS2RaSorter, Task};

pub(crate) fn _sort_parallel(task_queue: Arc<Mutex<VecDeque<Task>>>,
                             task_counter: Arc<AtomicUsize>,
                             thread_counter: Arc<AtomicUsize>) {
    info!("{:?} started", thread::current().id());
    let mut sorter = IPS2RaSorter::new_parallel();
    'outer: loop {
        let task = {
            let mut queue = task_queue.lock().unwrap();
            queue.pop_front()
        };

        if let Some(mut task) = task {
            thread_counter.fetch_add(1, Ordering::SeqCst);
            //let task_counter_num = task_counter.load(Ordering::SeqCst);
            //let thread_counter_num = thread_counter.load(Ordering::SeqCst);
            //info!("{:?} processing task of length {}, Task_Counter: {}, Thread_Counter: {}", thread::current().id(), task.arr.len(), task_counter_num, thread_counter_num);
            info!("{:?} processing task of length {}", thread::current().id(), task.arr.len());
            // Process the task
            if task.arr.len() as i64 <= THRESHOLD as i64 {
                insertion_sort(task.arr);
                task_counter.fetch_sub(1, Ordering::SeqCst);
                thread_counter.fetch_sub(1, Ordering::SeqCst);
                debug!("{:?} processed base case {:?}", thread::current().id(), task.arr);
                continue 'outer;
            }

            unsafe { sorter.classify(&mut task) };
            sorter.permutate_blocks(&mut task);
            sorter.cleanup(&mut task);

            debug!("{:?} processed task {}", thread::current().id(), sorter.to_string(&task));

            // Recursion:
            // add new tasks to queue
            let mut all = task.arr;
            for i in 0..K {
                let (current, next) = all.split_at_mut(sorter.element_counts[i] as usize);
                all = next;
                if current.len() <= 1 {
                    continue;
                }
                info!("{:?} adding task of length {:?} to queue", thread::current().id(), current.len());
                task_counter.fetch_add(1, Ordering::SeqCst);
                let new_task = Task::new(current, task.level + 1);
                {
                    let mut queue = task_queue.lock().unwrap();
                    queue.push_back(new_task);
                }
            }
            sorter.clear();

            task_counter.fetch_sub(1, Ordering::SeqCst);
            thread_counter.fetch_sub(1, Ordering::SeqCst);
        } else {
            let task_counter_num = task_counter.load(Ordering::SeqCst);
            let thread_counter_num = thread_counter.load(Ordering::SeqCst);
            info!("{:?} waiting for tasks: Task_Counter: {:?}, Thread_Counter: {:?}", thread::current().id(), task_counter_num, thread_counter_num);
            if task_counter.load(Ordering::SeqCst) == 0 && thread_counter.load(Ordering::SeqCst) == 0 {
                break;
            }
            thread::yield_now();
        }
    }
}