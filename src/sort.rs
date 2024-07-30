use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use log::{debug, error, info};
use crate::base_case::insertion_sort;
use crate::config::{BLOCKSIZE, K, NUM_THREADS, THRESHOLD};
use crate::sorter::{IPS2RaSorter, IPS4oSorter};
use crate::parallel::sort_parallel;

pub fn sort(arr: &mut [u64], parallel: bool) {
    if !parallel {
        let mut s = IPS4oSorter::new_sequential(arr);
        s.sort_sequential();
    } else {

        let task_queue = Arc::new(Mutex::new(VecDeque::new()));
        let task_counter = Arc::new(AtomicUsize::new(1));
        let thread_counter = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];

        let mut s = IPS4oSorter::new_parallel(arr);
        {
            let mut queue = task_queue.lock().unwrap();
            queue.push_back(s);
        }

        for _ in 0..NUM_THREADS {
            let builder = thread::Builder::new();
            let task_queue = Arc::clone(&task_queue);
            let task_counter = Arc::clone(&task_counter);
            let thread_counter = Arc::clone(&thread_counter);
            let handler = unsafe {
                builder.spawn_unchecked(move || {
                    sort_parallel(task_queue, task_counter, thread_counter);
                }).unwrap()
            };
            handles.push(handler);
        }

        // Wait for all threads to finish processing
        for handle in handles {
            handle.join().unwrap();
        }
    }
    info!("Sorted array: {:?}", arr);
}

pub fn ips2ra_sort(arr: &mut [u64]) {
    let mut s = IPS2RaSorter::new_sequential(arr, 0);
    s.sample();
    debug!("Array after sampling: {:?}", s.arr);
    info!("Level: {:?}", s.level);
    s.sort_sequential();
}
