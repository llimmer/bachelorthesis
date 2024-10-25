mod sampling;
mod base_case;
mod classification;
mod config;
mod permutation;
mod cleanup;
mod sort;
mod sorter;
mod sequential;
mod parallel;
mod conversion;
mod setup;
mod parallel_sort_merge;
mod rolling_sort;
mod sequential_sort_merge;
use crate::config::*;
use vroom::memory::{DmaSlice};
use std::error::Error;
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use log::LevelFilter;
use bachelorthesis::{sort_merge, sort_parallel};

fn verify_sorted(arr: &[u64]) {
    for i in 1..arr.len() {
        assert!(arr[i - 1] <= arr[i], "Difference at i={i}. {} > {}", arr[i - 1], arr[i]);
    }
}

#[derive(Debug)]
pub struct testEntry {
    pub value: u64,
}
impl Eq for testEntry {}

impl PartialEq for testEntry {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Ord for testEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.value.cmp(&self.value) // -> Min-Heap
    }
}

impl PartialOrd for testEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

use rand::prelude::*;
use std::{env};
use std::time::Duration;
use rayon::prelude::ParallelSliceMut;
use crate::sort::prepare_benchmark;

//use tracing::{info, span, Level};
//use tracing_perfetto::PerfettoLayer;
//use tracing_subscriber::{registry::Registry, prelude::*};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Error)
        .init();

    //let file = std::sync::Mutex::new(std::fs::File::create("/home/l/test.json")?);
    //let perfetto_layer = PerfettoLayer::new(file);
//
    //let subscriber = Registry::default().with(perfetto_layer);
    //tracing::subscriber::set_global_default(subscriber)?;

    // Create a span to be captured in the trace
    //let span = span!(Level::INFO, "main_span");
    //let _enter = span.enter();

    let mut nvme = vroom::init("0000:00:04.0")?;
    nvme = prepare_benchmark(nvme, 9, 12345);

    sort_merge(nvme, 9 * HUGE_PAGE_SIZE_1G/8, true)?;




   /* // Add some events to the trace
    info!("Starting the sorting process");

    // Simulate parallel sort or other work
    let mut rng = rand::rngs::StdRng::seed_from_u64(12345);
    let mut data: Vec<u64> = generate_uniform(&mut rng, 50_000_000);

    sort_parallel(&mut data);*/


    return Ok(());





    println!("Starting benchmark");
    let mut args = env::args();
    args.next();
    let pci_addr = "0000:00:04.0".to_string();

    let iterations = 1;
    let seed = 12345;

    let hugepages = [9];

    let mut nvme = vroom::init(&pci_addr)?;
    let mut measurements: Vec<Duration> = Vec::with_capacity(hugepages.len());

    for i in 0..hugepages.len() {
        let mut local_measurements: Vec<Duration> = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            nvme = prepare_benchmark(nvme, hugepages[i], seed as usize);
            let mut start = std::time::Instant::now();
            nvme = sort_merge(nvme, hugepages[i] * HUGE_PAGE_SIZE_1G / 8, true)?;
            let duration = start.elapsed();
            local_measurements.push(duration);
        }
        let avg = local_measurements.iter().sum::<Duration>() / iterations as u32;
        measurements.push(avg);
    }
    // print as table

    println!("Number of hugepages: {:?}", hugepages);
    // print times in seconds
    println!("{:?}", measurements.iter().map(|d| d.as_secs_f64()).collect::<Vec<f64>>());


    /* let mut rng = StdRng::seed_from_u64(12345);
    let mut data = generate_eight_dup(100000);
    println!("Data: {:?}", data);
    let mut start = Instant::now();
    sort(&mut data);
    println!("Sequential: {:?}", start.elapsed());

    return Ok(());
*/

    /*// A `Group` lets us enable and disable several counters atomically.
    let mut group = Group::new()?;
    let cycles = group.add(&Builder::new(Hardware::CPU_CYCLES))?;
    let insns = group.add(&Builder::new(Hardware::INSTRUCTIONS))?;
    let misses = group.add(&Builder::new(Hardware::CACHE_MISSES))?;
    let page_faults = group.add(&Builder::new(Software::PAGE_FAULTS))?;
    let branch_misses = group.add(&Builder::new(Hardware::BRANCH_MISSES))?;
    //let stalled_cycles_backend = group.add(&Builder::new(Hardware::STALLED_CYCLES_BACKEND))?;


    let mut data: Vec<u64> = (1..=100_000_000u64).collect();
    let mut rng = StdRng::seed_from_u64(12345);
    data.shuffle(&mut rng);

    group.enable()?;
    data.sort_unstable();
    group.disable()?;

    let counts = group.read()?;
    println!(
        "cycles / instructions: {} / {} ({:.2} cpi)",
        counts[&cycles],
        counts[&insns],
        (counts[&cycles] as f64 / counts[&insns] as f64)
    );

    println!("cache misses: {} ", counts[&misses]);
    println!("branch misses: {} ", counts[&branch_misses]);
    println!("page faults: {} ", counts[&page_faults]);*/
    //println!("stalled cycles backend: {} ", counts[&stalled_cycles_backend]);


    /* let mut data: Vec<u64> = (1..=500_000_000u64).collect();
     let mut rng = StdRng::seed_from_u64(12345);
     data.shuffle(&mut rng);
     let mut data2 = data.clone();
     let mut data3 = data.clone();

     // Sequential
     let start = Instant::now();
     sort(&mut data);
     let duration = start.elapsed();
     println!("Sequential: {:?}", duration);

     // Parallel
     let start = Instant::now();
     sort_parallel(&mut data2);
     let duration = start.elapsed();
     println!("Parallel: {:?}", duration);

     // Quicksort
     let start = Instant::now();
     data3.sort_unstable();
     let duration = start.elapsed();
     println!("Quicksort: {:?}", duration);*/


    /*let mut nvme = vroom::init("0000:00:04.0")?;
    let mut qpair = nvme.create_io_queue_pair(QUEUE_LENGTH)?;

    let mut buffer_big = Dma::allocate(HUGE_PAGE_SIZE_1G)?;

    clear_chunks(262144, &mut qpair);

    let mut data: Vec<u64> = (0..HUGE_PAGE_SIZE_1G as u64 / 8).map(|x| 1 * x).collect();
    let mut data2: Vec<u64> = (0..HUGE_PAGE_SIZE_1G as u64 / 8).map(|x| 1 * x).collect();
    let mut data3: Vec<u64> = (0..HUGE_PAGE_SIZE_1G as u64 / 8).map(|x| 1 * x).collect();
    let mut data4: Vec<u64> = (0..HUGE_PAGE_SIZE_1G as u64 / 8).map(|x| 1 * x).collect();
    let mut data5: Vec<u64> = (0..HUGE_PAGE_SIZE_1G as u64 / 8).map(|x| 1 * x).collect();
    let mut data6: Vec<u64> = (0..HUGE_PAGE_SIZE_1G as u64 / 8).map(|x| 1 * x).collect();
    let mut data7: Vec<u64> = (0..HUGE_PAGE_SIZE_1G as u64 / 8).map(|x| 1 * x).collect();
    let mut data8: Vec<u64> = (0..HUGE_PAGE_SIZE_1G as u64 / 8).map(|x| 1 * x).collect();
    let mut data9: Vec<u64> = (0..HUGE_PAGE_SIZE_1G as u64 / 16).map(|x| 1 * x).collect();


    let mut rng = StdRng::seed_from_u64(12345);
    data.shuffle(&mut rng);
    data2.shuffle(&mut rng);
    data3.shuffle(&mut rng);
    data4.shuffle(&mut rng);
    data5.shuffle(&mut rng);
    data6.shuffle(&mut rng);
    data7.shuffle(&mut rng);
    data8.shuffle(&mut rng);
    data9.shuffle(&mut rng);
    println!("Data 9: {:?}", data9);


    buffer_big[0..data.len() * 8].copy_from_slice(u64_to_u8_slice(&mut data));
    read_write_hugepage_1G(&mut qpair, 0, &mut buffer_big, true);

    buffer_big[0..data2.len() * 8].copy_from_slice(u64_to_u8_slice(&mut data2));
    read_write_hugepage_1G(&mut qpair, LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G, &mut buffer_big, true);

    buffer_big[0..data3.len() * 8].copy_from_slice(u64_to_u8_slice(&mut data3));
    read_write_hugepage_1G(&mut qpair, 2 * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G, &mut buffer_big, true);

    buffer_big[0..data4.len() * 8].copy_from_slice(u64_to_u8_slice(&mut data4));
    read_write_hugepage_1G(&mut qpair, 3 * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G, &mut buffer_big, true);

    buffer_big[0..data5.len() * 8].copy_from_slice(u64_to_u8_slice(&mut data5));
    read_write_hugepage_1G(&mut qpair, 4 * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G, &mut buffer_big, true);

    buffer_big[0..data6.len() * 8].copy_from_slice(u64_to_u8_slice(&mut data6));
    read_write_hugepage_1G(&mut qpair, 5 * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G, &mut buffer_big, true);

    buffer_big[0..data7.len() * 8].copy_from_slice(u64_to_u8_slice(&mut data7));
    read_write_hugepage_1G(&mut qpair, 6 * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G, &mut buffer_big, true);

    buffer_big[0..data8.len() * 8].copy_from_slice(u64_to_u8_slice(&mut data8));
    read_write_hugepage_1G(&mut qpair, 7 * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G, &mut buffer_big, true);

    buffer_big[0..data9.len() * 8].copy_from_slice(u64_to_u8_slice(&mut data9));
    read_write_hugepage_1G(&mut qpair, 8 * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G, &mut buffer_big, true);


    // read line from stdin
    //let mut input = String::new();
    //io::stdin().read_line(&mut input)?;
    println!("Starting parallel sort merge");
    let start = Instant::now();
    parallel_sort_merge(nvme, HUGE_PAGE_SIZE_1G / 8 * 8 + HUGE_PAGE_SIZE_1G/16)?;
    let duration = start.elapsed();
    println!("Parallel sort merge: {:?}", duration);

    return Ok(());*/


    /*let mut nvme = vroom::init("0000:00:04.0")?;
    let mut qpair = nvme.create_io_queue_pair(QUEUE_LENGTH)?;

    clear_chunks(CHUNKS_PER_HUGE_PAGE_1G, &mut qpair);

    let mut sorter_qpair = nvme.create_io_queue_pair(QUEUE_LENGTH)?;
    let mut buffer_large: Dma<u8> = Dma::allocate(HUGE_PAGE_SIZE_1G)?;

    println!("Buffer size: {}", buffer_large.size);


    let mut buffers_small = Vec::new();
    buffers_small.push(Dma::allocate(HUGE_PAGE_SIZE_2M)?);
    buffers_small.push(Dma::allocate(HUGE_PAGE_SIZE_2M)?);
    buffers_small.push(Dma::allocate(HUGE_PAGE_SIZE_2M)?);
    buffers_small.push(Dma::allocate(HUGE_PAGE_SIZE_2M)?);
    let mut tmp = Dma::allocate(HUGE_PAGE_SIZE_2M)?;

    let mut sorter = IPS2RaSorter::new_sequential();
    let mut ext_sorter = IPS2RaSorter::new_ext_sequential(sorter_qpair, buffers_small, tmp);

    let len: usize = 1024;
    const offset: usize = 65;

    let mut data: Vec<u64> = vec![0u64; offset];
    let mut data2: Vec<u64> = (1..=len as u64).collect();
    let mut rng = StdRng::seed_from_u64(12345);
    data2.shuffle(&mut rng);

    for i in 0..len {
        data.push(data2[i]);
    }

    print!("Data: {:?}", data);



    // prepare data on ssd
    buffer_large[0..(len+offset)*8].copy_from_slice(u64_to_u8_slice(&mut data));
    read_write_hugepage(&mut qpair, 0, &mut buffer_large, true);


    // remove offset from original data
    data = data[offset..].to_vec();

    let mut task = Task::new(&mut data, 0, 0, 0);
    task.sample_untouched();
    let mut dma_task = DMATask::new(offset*8/LBA_SIZE, offset%(LBA_SIZE/8), len, task.level, task.level_start, task.level_end);



    sorter.classify(&mut task);
    ext_sorter.classify_ext(&mut dma_task);

    read_write_hugepage(&mut qpair, 0, &mut buffer_large, false);

    //println!("Array after classification: {:?}", task.arr);
    //println!("External array after classification: {:?}", u8_to_u64_slice(&mut buffer_large[0..(len*8)]));

    println!("Classified elements: {}, external = {}", sorter.classified_elements, ext_sorter.classified_elements);

    let slice = u8_to_u64_slice(&mut buffer_large[offset*8..(len+offset)*8]);
    for i in 0..len {
        if task.arr[i] != slice[i] {
            println!("Difference after classification at i = {}, task = {}, res = {}", i, task.arr[i], slice[i]);
            return Ok(());
        }
    }

    assert_eq!(sorter.classified_elements, ext_sorter.classified_elements, "Classified elements not equal");
    assert_eq!(task.arr[0..sorter.classified_elements], u8_to_u64_slice(&mut buffer_large[offset*8..(len+offset)*8])[0..ext_sorter.classified_elements], "Data not classified correctly");

    assert_eq!(task.arr, u8_to_u64_slice(&mut buffer_large[offset*8..(len+offset)*8]), "Data classified correctly but arr[classified_elements..] not equal. Further testing not possible");


    sorter.permutate_blocks(&mut task);
    ext_sorter.permutate_blocks_ext(&mut dma_task);


    read_write_hugepage(&mut qpair, 0, &mut buffer_large, false);
    assert_eq!(task.arr, u8_to_u64_slice(&mut buffer_large[offset*8..(len+offset)*8]), "Data not permutated correctly");

    println!("Overflows: {:?}, external = {:?}", sorter.overflow_buffer, ext_sorter.overflow_buffer);
    //println!("Array after permutation: {:?}", task.arr);
    //println!("SSD after permutation: {:?}", u8_to_u64_slice(&mut buffer_large[0..(len+offset)*8]));

    //println!("Sorter struct before cleanup: {:?}", sorter);
    //println!("External sorter struct before cleanup: {:?}", ext_sorter);

    //println!("Data before cleanup: {:?}", task.arr);
    //println!("External data before cleanup: {:?}", u8_to_u64_slice(&mut buffer_large[offset*8..(len+offset)*8]));

    sorter.cleanup(&mut task);
    ext_sorter.cleanup_ext(&mut dma_task);

    read_write_hugepage(&mut qpair, 0, &mut buffer_large, false);
    let res = u8_to_u64_slice(&mut buffer_large[0..(len+offset)*8]);

    //println!("Result: {:?}", task.arr);
    //println!("External Result: {:?}", res);
    for i in 0..len {
        if task.arr[i] != res[i+offset] {
            println!("Difference after cleanup at i = {}, task = {}, res = {}", i, task.arr[i], res[i]);
            return Ok(());
        }
    }

    println!("Sorting done.");

    return Ok(());*/

    /*for i in 0..100000 {
        let len: u64 = 8192+i;//8192+1024;//;
        println!("i = {}", i);
        sorter.clear();
        ext_sorter.clear();
        let mut data: Vec<u64> = (1..=len).collect();
        let mut rng = StdRng::seed_from_u64(i);
        data.shuffle(&mut rng);

        // write data to ssd
        buffer_large[0..(len * 8) as usize].copy_from_slice(u64_to_u8_slice(&mut data));
        read_write_hugepage(&mut qpair, 0, &mut buffer_large, true);

        let mut task = Task::new(&mut data, 0);
        task.sample();
        let mut dma_task = DMATask::new(0, 0, len as usize, task.level);

        println!("Starting classification");
        sorter.classify(&mut task);
        println!("Done\nStarting external classification");
        ext_sorter.classify_ext(&mut dma_task);
        println!("Done");

        // read to check if data is classified correctly
        read_write_hugepage(&mut qpair, 0, &mut buffer_large, false);

        println!("Classified elements: {}, external = {}", sorter.classified_elements, ext_sorter.classified_elements);
        assert_eq!(task.arr, u8_to_u64_slice(&mut buffer_large[0..(len * 8) as usize]), "Data not classified correctly");
        //println!("Data after classification: {:?}", task.arr);
        //println!("Data after external classification: {:?}", u8_to_u64_slice(&mut buffer_large[0..(len*8) as usize]));


        // permutation
        sorter.permutate_blocks(&mut task);
        ext_sorter.permutate_blocks_ext(&mut dma_task);

        // read to check if data is permutated correctly
        read_write_hugepage(&mut qpair, 0, &mut buffer_large, false);

        assert_eq!(task.arr, u8_to_u64_slice(&mut buffer_large[0..(len * 8) as usize]), "Data not permutated correctly");
        println!("Overflows: {:?}, external = {:?}", sorter.overflow_buffer, ext_sorter.overflow_buffer);
    }*/


    //sort_dma("0000:00:04.0", 0, false)?;

    /*let mut data: Vec<u64> = (1..=300_000_000u64).collect();
    let mut rng = StdRng::seed_from_u64(12345);
    data.shuffle(&mut rng);
    let mut data2 = data.clone();
    let mut data3 = data.clone();

    // Sequential
    let start = Instant::now();
    sort(&mut data);
    let duration = start.elapsed();
    println!("Sequential: {:?}", duration);

    // Parallel
    let start = Instant::now();
    sort_parallel(&mut data2);
    let duration = start.elapsed();
    println!("Parallel: {:?}", duration);

    // Quicksort
    let start = Instant::now();
    data3.sort_unstable();
    let duration = start.elapsed();
    println!("Quicksort: {:?}", duration);*/

    Ok(())
}

// exponential distribution.
fn generate_exponential(rng: &mut StdRng, n: usize) -> Vec<u64> {
    let log_n = (n as f64).log(2.0).ceil() as usize; // Calculate log base 2 of n
    (0..n).map(|i| {
        let i = (i % log_n) as f64; // i should be in [0, log_n)
        let lower_bound = (2f64.powf(i));
        let upper_bound = (2f64.powf(i + 1.0));
        rng.gen_range(lower_bound..upper_bound) as u64 // Select uniformly from [2^i, 2^(i+1))
    }).collect()
}

// rootDup distribution.
fn generate_root_dup(n: usize) -> Vec<u64> {
    let sqrt_n = (n as f64).sqrt() as usize; // Floor of the square root of n
    (0..n).map(|i| {
        let value = i % sqrt_n; // A[i] = i mod floor(sqrt(n))
        value as u64
    }).collect()
}

// twoDup distribution.
fn generate_two_dup(n: usize) -> Vec<u64> {
    (0..n).map(|i| {
        let value = (i * i + n / 2) % n; // A[i] = i^2 + n/2 mod n
        value as u64
    }).collect()
}

// eightDup distribution.
fn generate_eight_dup(n: usize) -> Vec<u64> {
    (0..n).map(|i| {
        let value = (i.pow(8) + n / 2) % n; // A[i] = i^8 + n/2 mod n
        value as u64
    }).collect()
}

// zipf distribution.
fn generate_zipf(rng: &mut StdRng, n: usize) -> Vec<u64> {
    let mut zipf_distribution: Vec<u64> = (1..=1000000).collect();
    let total_weight: f64 = zipf_distribution.iter().map(|k| 1.0 / (*k as f64).powf(0.75)).sum();

    (0..n).map(|_| {
        let rand_value = rng.gen_range(0.0..total_weight);
        let mut cumulative_weight = 0.0;

        for &k in &zipf_distribution {
            cumulative_weight += 1.0 / (k as f64).powf(0.75);
            if cumulative_weight >= rand_value {
                return k as u64; // Return the chosen k
            }
        }

        zipf_distribution[0] // Fallback in case of an error
    }).collect()
}

// 95% sorted
fn generate_almost_sorted(rng: &mut StdRng, length: usize) -> Vec<u64> {
    let mut data: Vec<u64> = (0..length as u64).collect();

    for _ in 0..(length / 20) { // swap 5% of data
        let i = rng.gen_range(0..length);
        let j = rng.gen_range(0..length);
        data.swap(i, j);
    }
    data
}

// uniform distribution
fn generate_uniform(rng: &mut StdRng, length: usize) -> Vec<u64> {
    (0..length)
        .map(|_| rng.gen::<u64>())
        .collect()
}

// range
fn generate_in_range(rng: &mut StdRng, length: usize, range: u64) -> Vec<u64> {
    (0..length)
        .map(|_| rng.gen_range(0..range))
        .collect()
}

