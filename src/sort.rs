use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use log::{debug, error, info};
use crate::base_case::insertion_sort;
use crate::config::{BLOCKSIZE, K, NUM_THREADS, THRESHOLD};
use crate::sorter::{IPS2RaSorter, Task};
use crate::parallel::sort_parallel;

pub fn sort(arr: &mut [u64], parallel: bool) {
    if !parallel {
        let mut s = IPS2RaSorter::new_sequential();
        let mut task = Task::new(arr, 0);
        task.sample();
        debug!("Task after sampling: {:?}", task.arr);
        info!("Level: {:?}", task.level);
        s.sort_sequential(&mut task);
    } else {
        //let task_queue = Arc::new(Mutex::new(VecDeque::new()));
        //let task_counter = Arc::new(AtomicUsize::new(1));
        //let thread_counter = Arc::new(AtomicUsize::new(0));
//
        //let mut handles = vec![];
//
        //let mut s = IPS2RaSorter::new_parallel(arr);
        //{
        //    let mut queue = task_queue.lock().unwrap();
        //    queue.push_back(s);
        //}
//
        //for _ in 0..NUM_THREADS {
        //    let builder = thread::Builder::new();
        //    let task_queue = Arc::clone(&task_queue);
        //    let task_counter = Arc::clone(&task_counter);
        //    let thread_counter = Arc::clone(&thread_counter);
        //    let handler = unsafe {
        //        builder.spawn_unchecked(move || {
        //            sort_parallel(task_queue, task_counter, thread_counter);
        //        }).unwrap()
        //    };
        //    handles.push(handler);
        //}
//
        //// Wait for all threads to finish processing
        //for handle in handles {
        //    handle.join().unwrap();
        //}
    }
    info!("Sorted array: {:?}", arr);
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
        arr.clear();
    }


}
