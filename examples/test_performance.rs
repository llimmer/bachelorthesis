use rand::prelude::{SliceRandom, StdRng};
use rand::SeedableRng;
use vroom::memory::HUGE_PAGE_SIZE_1G;
use bachelorthesis::{sort, sort_parallel};

pub fn main(){

    let mut data: Vec<u64> = (1..=HUGE_PAGE_SIZE_1G as u64/8).collect();
    let mut rng = StdRng::seed_from_u64(54321);
    data.shuffle(&mut rng);
    let mut data_copy = data.clone();

    let start = std::time::Instant::now();
    sort(&mut data);
    let duration = start.elapsed();
    println!("IPS2Ra sort: {:?}", duration);

    let start = std::time::Instant::now();
    data_copy.sort_unstable();
    let duration = start.elapsed();
    println!("Quicksort: {:?}", duration);

    verify_sorted(&data);
    verify_sorted(&data_copy);
}

fn verify_sorted(arr: &Vec<u64>) {
    for i in 1..arr.len() {
        assert!(arr[i - 1] <= arr[i]);
    }
}