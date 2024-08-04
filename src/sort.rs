use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize};
use std::thread;
use log::{debug, error, info};
use crate::config::{BLOCKSIZE, K, NUM_THREADS, THRESHOLD};
use crate::parallel::process_task;
use crate::sorter::{IPS2RaSorter, Task};

pub fn sort(arr: &mut [u64]) {
    let mut s = IPS2RaSorter::new_sequential();
    let mut task = Task::new(arr, 0);
    task.sample();
    debug!("Task after sampling: {:?}", task.data);
    info!("Level: {:?}", task.level);
    s.sort_sequential(&mut task);
}
pub fn sort_parallel(arr: &mut [u64]) {
    if NUM_THREADS > 0 {
        rayon::ThreadPoolBuilder::new().num_threads(NUM_THREADS).build_global().unwrap();
    }
    let mut initial_task = Task::new(arr, 0);
    initial_task.sample();
    process_task(&mut initial_task);
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
