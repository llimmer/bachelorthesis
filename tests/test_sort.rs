#[cfg(test)]
mod sequential_sort {
    use std::env;
    use rand::prelude::SliceRandom;
    use rand::rngs::{StdRng};
    use rand::{thread_rng, Rng, SeedableRng};
    use lazy_static::lazy_static;

    use bachelorthesis::{sort, HUGE_PAGE_SIZE_2M};

    lazy_static! {
        static ref SEED: u64 = initialize_seed();
        static ref NUM_RUNS: usize = get_num_runs();
        static ref MAX_ELEMENTS: usize = get_max_elements();
    }

    fn verify_sorted(arr: &Vec<u64>) {
        for i in 1..arr.len() {
            assert!(arr[i - 1] <= arr[i], "Array not sorted! {} (i={}) > {} (i={}). Seed: {}", arr[i - 1], i - 1, arr[i], i, *SEED);
        }
    }

    #[test]
    fn small_sequential() { // 1024 shuffled elements
        let mut arr: Vec<u64> = (1..=8192).collect();
        arr.shuffle(&mut StdRng::seed_from_u64(*SEED));
        sort(&mut arr);
        verify_sorted(&arr);
    }

    #[test]
    fn big_sequential() { // 1024 shuffled elements
        let mut arr: Vec<u64> = (1..=*MAX_ELEMENTS as u64).collect();
        arr.shuffle(&mut StdRng::seed_from_u64(*SEED));
        sort(&mut arr);
        verify_sorted(&arr);
    }

    #[test]
    fn random_sequential(){
        let mut rng = StdRng::seed_from_u64(*SEED);
        for i in 0..*NUM_RUNS {
            let n = rng.gen_range(1..*MAX_ELEMENTS);
            println!("i={i}, n={n}");
            let mut shuffel_rng = StdRng::seed_from_u64(*SEED + i as u64);
            let mut arr: Vec<u64> = (0..n).map(|_| shuffel_rng.gen_range(0..u64::MAX)).collect();
            sort(&mut arr);
            verify_sorted(&arr);
        }
    }

    fn initialize_seed() -> u64 {
        // Check for environment variables to control seed randomization
        let randomize_seed = env::var("RANDOMIZE_SEED")
            .map(|val| val == "true")
            .unwrap_or(false);

        if randomize_seed {
            println!("Randomizing seed");
            let seed: u64 = thread_rng().gen_range(0..u64::MAX);
            println!("Seed: {}", seed);
            seed
        } else {
            // Use a default seed or allow for an environment-set seed
            let seed = env::var("SEED")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(12345); // Default seed
            println!("Seed: {}", seed);
            seed
        }
    }

    fn get_num_runs() -> usize {
        env::var("NUM_RUNS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(4)
    }

    fn get_max_elements() -> usize {
        env::var("MAX_ELEMENTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(HUGE_PAGE_SIZE_2M/8)
    }
}