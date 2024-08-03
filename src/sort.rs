use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize};
use std::thread;
use log::{debug, error, info};
use crate::config::{BLOCKSIZE, K, NUM_THREADS, THRESHOLD};
use crate::parallel::_sort_parallel;
use crate::sorter::{IPS2RaSorter, Task};

pub fn sort(arr: &mut [u64]) {
    let mut s = IPS2RaSorter::new_sequential();
    let mut task = Task::new(arr, 0);
    task.sample();
    debug!("Task after sampling: {:?}", task.arr);
    info!("Level: {:?}", task.level);
    s.sort_sequential(&mut task);
}
pub fn sort_parallel(arr: &mut [u64]) {
    info!("Setting up parallel sort");
    let task_queue = Arc::new(Mutex::new(VecDeque::new()));
    let task_counter = Arc::new(AtomicUsize::new(1));
    let thread_counter = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    let mut first_task = Task::new(arr, 0);
    first_task.sample();
    {
        let mut queue = task_queue.lock().unwrap();
        queue.push_back(first_task);
    }

    for _ in 0..NUM_THREADS {
        let builder = thread::Builder::new();
        let task_queue = Arc::clone(&task_queue);
        let task_counter = Arc::clone(&task_counter);
        let thread_counter = Arc::clone(&thread_counter);
        let handler = unsafe {
            builder.spawn_unchecked(move || {
                _sort_parallel(task_queue, task_counter, thread_counter);
            }).unwrap()
        };
        handles.push(handler);
    }

    // Wait for all threads to finish processing
    for handle in handles {
        handle.join().unwrap();
    }
}


#[cfg(test)]
mod tests {
    use rand::prelude::StdRng;
    use rand::{Rng, SeedableRng};
    use super::*;

    #[test]
    fn small_sequential() {
        let mut rng = StdRng::seed_from_u64(12345);
        let n = rng.gen_range(512..1024);
        let mut arr: Vec<u64> = (0..n).map(|_| rng.gen_range(0..u64::MAX)).collect();

        sort(&mut arr);
        for i in 1..arr.len() {
            assert!(arr[i - 1] <= arr[i]);
        }
    }

    #[test]
    fn big_sequential() {
        let mut rng = StdRng::seed_from_u64(12345);
        for _ in 0..1024 {
            let n = rng.gen_range(512..1024);
            let mut arr: Vec<u64> = (0..n).map(|_| rng.gen_range(0..u64::MAX)).collect();

            sort(&mut arr);
            for i in 1..arr.len() {
                assert!(arr[i - 1] <= arr[i]);
            }
        }
    }
}
