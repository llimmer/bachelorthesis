use crate::config::*;
use crate::conversion::*;
use crate::sort::{read_write_elements, read_write_hugepage_1G, read_write_hugepage_2M};
use crate::sorter::{IPS2RaSorter, Task};
use vroom::{NvmeDevice, NvmeQueuePair, QUEUE_LENGTH};
use vroom::memory::Dma;
use std::error::Error;
use std::cmp::{min};
use std::cell::RefCell;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::collections::{BinaryHeap};
use std::{mem};
use std::sync::{Arc, Mutex};
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use rayon::{ThreadPoolBuilder};
use log::debug;

thread_local! {
    static SORTER: RefCell<IPS2RaSorter> = RefCell::new(*IPS2RaSorter::new_parallel());
}

pub fn parallel_sort_merge(mut nvme: NvmeDevice, len: usize) -> Result<NvmeDevice, Box<dyn Error>> {
    let num_hugepages = (len + HUGE_PAGE_SIZE_1G / 8 - 1) / (HUGE_PAGE_SIZE_1G / 8);
    println!("Sorting and merging {} hugepages", num_hugepages);

    let max = (num_hugepages as f64).log((NUM_THREADS) as f64).ceil() as usize;
    let sort_offset =
        if max % 2 == 0 {
            0
        } else {
            num_hugepages * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G
        };
    let merge_offset =
        if max % 2 == 0 {
            num_hugepages * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G
        } else {
            0
        };

    let mut cleanup_qpair = nvme.create_io_queue_pair(QUEUE_LENGTH)?;
    let mut clenup_buffer = Dma::allocate(HUGE_PAGE_SIZE_2M)?;
    let nvme = initialize_thread_pool(nvme, num_hugepages);

    println!("Starting parallel sorting. Len: {}, Max: {}, output_offset: {}", len, max, sort_offset);
    let initial_separators = sort_parallel(len, num_hugepages, sort_offset);
    println!("Done");

    // read line from stdin
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();

    println!("Starting parallel merging");
    merge_parallel(&mut cleanup_qpair, &mut clenup_buffer, initial_separators, len, num_hugepages, max, sort_offset, merge_offset);
    println!("Done");

    Ok(nvme)
}

fn initialize_thread_pool(nvme: NvmeDevice, num_buffer: usize) -> NvmeDevice {
    assert!(NUM_THREADS * min(NUM_THREADS, num_buffer) <= HUGE_PAGES_2M, "Not enough 2MiB hugepages available for buffers");
    assert!(HUGE_PAGES_1G >= NUM_THREADS, "Not enough 1GiB hugepages available for buffers");
    ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS) // Customize the number of threads
        .build_global() // Replaces the default global thread pool
        .unwrap();

    let nvme_arc = Arc::new(Mutex::new(nvme));


    (0..NUM_THREADS).into_par_iter().for_each(|thread_id| {
        let nvme_clone = Arc::clone(&nvme_arc);

        let mut nvme = nvme_clone.lock().unwrap();
        let qpair = nvme.create_io_queue_pair(QUEUE_LENGTH).unwrap();

        // Allocate buffers
        let buffers: Vec<Dma<u8>> = (0..min(NUM_THREADS, num_buffer))
            .map(|_| Dma::allocate(HUGE_PAGE_SIZE_2M).unwrap())
            .collect();
        let sort_buffer = Dma::allocate(HUGE_PAGE_SIZE_1G).unwrap();

        // Initialize the SORTER for this thread
        SORTER.with(|sorter| {
            let mut sorter_ref = sorter.borrow_mut();
            *sorter_ref = *IPS2RaSorter::new_ext_sequential(qpair, buffers, sort_buffer);
        });

        println!("Thread {} initialized sorter", thread_id);
    });

    // Return the modified NVMe device
    match Arc::try_unwrap(nvme_arc) {
        Ok(mutex) => mutex.into_inner().unwrap(),
        Err(_) => panic!("There are still references to the Arc, unable to unwrap."),
    }
}

pub fn sort_parallel(len: usize, num_hugepages: usize, write_offset: usize) -> Vec<Vec<u64>> {
    let local_separators: Arc<Mutex<Vec<Vec<u64>>>> = Arc::new(Mutex::new(vec![Vec::new(); num_hugepages]));

    (0..num_hugepages).into_par_iter().for_each(|i| {
        SORTER.with(|sorter| {
            let mut sorter = sorter.borrow_mut();
            println!("Thread {} starting sort of hugepage {}.", rayon::current_thread_index().unwrap(), i);
            sorter.read_write_sort_buffer_1G(i * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G, false);

            let mut buffer = sorter.sort_buffer.take().unwrap();
            let u64slice = u8_to_u64_slice(&mut buffer[0..{
                if (i + 1) * HUGE_PAGE_SIZE_1G / 8 <= len {
                    HUGE_PAGE_SIZE_1G
                } else {
                    len - i * HUGE_PAGE_SIZE_1G / 8
                }
            }]);

            let mut task = Task::new(u64slice, 0, 0, 8);
            if !task.sample() {
                return;
            }
            sorter.sequential_rec(&mut task);

            let local_separator = compute_local_separators(u64slice, NUM_THREADS - 1);
            sorter.sort_buffer = Some(buffer);
            sorter.read_write_sort_buffer_1G(i * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G + write_offset, true);
            println!("Thread {} finished sorting hugepage {}. Writing to lba {}. Local separators: {:?}. First elements: {:?}", rayon::current_thread_index().unwrap(), i, i * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G + write_offset, local_separator, u8_to_u64_slice(&mut sorter.sort_buffer.as_mut().unwrap()[0..128]));

            // push to local separators at idx i.
            let mut local_separators_locked = local_separators.lock().unwrap();
            local_separators_locked[i] = local_separator;
            sorter.clear();
        });
    });

    let mut separators_guard = local_separators.lock().unwrap();
    mem::take(&mut *separators_guard)
}

pub fn merge_parallel(qpair: &mut NvmeQueuePair, buffer: &mut Dma<u8>, initial_separators: Vec<Vec<u64>>, len: usize, mut num_hugepages: usize, max: usize, mut start_lba: usize, mut output_lba: usize) {
    debug!("Total number of hugepages: {num_hugepages}, start_lba: {start_lba}, output_lba: {output_lba}");

    assert_eq!(initial_separators.len(), num_hugepages);

    let mut separators = initial_separators;

    for i in 0..max {
        println!("\n\ni: {i}, start_lba: {start_lba}, output_lba: {output_lba}");
        let input_length = NUM_THREADS.pow(i as u32);
        let result_length = input_length * NUM_THREADS;

        let remaining_hugepages = num_hugepages;
        let mut next_separators: Vec<Vec<u64>> = vec![];
        let mut flattened_separators: Vec<u64> = Vec::with_capacity((NUM_THREADS - 1) * min(NUM_THREADS, remaining_hugepages)); // TODO: double check

        for j in 0..(num_hugepages + result_length - 1) / result_length {
            println!("\nj: {j}, input_length: {input_length}, result_length: {result_length}");
            let cur_num_hugepages =
                if remaining_hugepages >= result_length {
                    result_length
                } else {
                    remaining_hugepages
                };

            if cur_num_hugepages <= 1 {
                break;
            }
            println!("Cur num hugepages: {cur_num_hugepages}");
            flattened_separators.clear();
            for vec in separators[j * NUM_THREADS..(j+1) * NUM_THREADS].iter() {
                flattened_separators.extend(vec);
            }
            flattened_separators.sort_unstable();
            debug!("Flattened separators: {:?}", flattened_separators);

            let global_separators = compute_local_separators(&flattened_separators, NUM_THREADS - 1);
            debug!("Global separators: {:?}", global_separators);

            prepare_thread_merge(qpair, buffer, &global_separators, start_lba, output_lba+j*result_length*CHUNKS_PER_HUGE_PAGE_1G*LBA_PER_CHUNK, input_length, cur_num_hugepages);
            next_separators.push(global_separators);
            debug!("Next separators: {:?}", next_separators);
        }
        separators = next_separators;
        let tmp = start_lba;
        start_lba = output_lba;
        output_lba = tmp;
        num_hugepages = (num_hugepages + NUM_THREADS - 1) / NUM_THREADS;
    }
}


fn prepare_thread_merge(qpair: &mut NvmeQueuePair, buffer: &mut Dma<u8>, global_separators: &Vec<u64>, start_lba: usize, write_lba: usize, input_length: usize, remaining_hugepages: usize) {
    println!("Preparing thread merge with global separators: {:?}, start_lba: {}, write_lba: {}, input_length: {}, remaining_hugepages: {}", global_separators, start_lba, write_lba, input_length, remaining_hugepages);
    let remainders: Arc<Mutex<Vec<Vec<u64>>>> = Arc::new(Mutex::new(vec![Vec::new(); NUM_THREADS]));

    let local_indices: Vec<Vec<usize>> = (0..remaining_hugepages).into_par_iter().map(|x| {
        SORTER.with(
            |sorter| unsafe {
                let mut sorter = sorter.borrow_mut();
                sorter.binary_search_indices(global_separators, start_lba + x * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G * input_length, input_length * HUGE_PAGE_SIZE_1G / 8)
            }
        )
    }).collect();
    debug!("Local indices: {:?}", local_indices);

    let ranges = transform_indices_to_ranges(&local_indices, input_length * HUGE_PAGE_SIZE_1G / 8, NUM_THREADS);
    debug!("Ranges: {:?}", ranges);

    //pre-compute total ranges
    let mut total_ranges: Vec<(usize, usize)> = Vec::with_capacity(NUM_THREADS);
    let mut sum: usize = 0;
    for i in 0..NUM_THREADS {
        let start = sum;
        sum += ranges[i].iter().map(|(start, end)| end - start).sum::<usize>();
        total_ranges.push((start, sum));
    }
    debug!("Total ranges: {:?}", total_ranges);

    // read line from stdin
    //let mut input = String::new();
    //std::io::stdin().read_line(&mut input).unwrap();

    (0..NUM_THREADS).into_par_iter().for_each(|thread_id| {
        let merge_result = SORTER.with(|sorter| {
            let mut sorter = sorter.borrow_mut();
            let output_lba = write_lba + thread_id * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G * input_length * (NUM_THREADS-1);
            debug!("Output lba for run {thread_id}: {output_lba}");

            sorter.thread_merge(
                &ranges[thread_id],
                start_lba,
                output_lba,
                total_ranges[thread_id].0 % (LBA_SIZE/8),
                total_ranges[thread_id].1 - total_ranges[thread_id].0,
                input_length * HUGE_PAGE_SIZE_1G)
        });

        // Store the result in the appropriate part of remainders
        let mut remainders_locked = remainders.lock().unwrap();
        remainders_locked[thread_id] = merge_result;
    });

    // Cleanup:
    let mut remainders_locked = remainders.lock().unwrap();
    let mut sum = 0;

    for i in 0..NUM_THREADS {
        let output_lba_offset = if i == 0 { 0 } else { total_ranges[i - 1].1 * 8 / LBA_SIZE };
        sum += total_ranges[i].1 - total_ranges[i].0;
        let tailsize = sum % (LBA_SIZE / 8);
        if tailsize > 0 {
            let lba = (sum - tailsize) / (LBA_SIZE / 8) + write_lba + output_lba_offset;
            println!("Writing {tailsize} remaining elements of merge {i} to lba {lba}");
            read_write_elements(qpair, buffer, lba, 0, LBA_SIZE / 8, false);
            buffer[0..tailsize * 8].copy_from_slice(&u64_to_u8_slice(&mut remainders_locked[i]));
            read_write_elements(qpair, buffer, lba, 0, LBA_SIZE / 8, true);
        }
    }
}

struct HeapEntry {
    value: u64,
    array: usize,
}

impl Eq for HeapEntry {}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.value.cmp(&self.value) // -> Min-Heap
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl IPS2RaSorter {
    pub fn thread_merge(&mut self, indices: &Vec<(usize, usize)>, start_lba: usize, output_lba: usize, output_offset: usize, total_length: usize, input_length_byte: usize) -> Vec<u64> {
        /*if indices[0].0 != 0 {
            debug!("Thread {} waiting for other threads to finish", rayon::current_thread_index().unwrap());
            // read line from stdin
            //let mut input = String::new();
            //std::io::stdin().read_line(&mut input).unwrap();

            debug!("Clearing first elements of output buffer");

            let mut output_buffer = self.sort_buffer.as_mut().unwrap();
            output_buffer[0..1024 * 8].copy_from_slice(&[0u8; 1024 * 8]);
        }*/


        assert!(self.qpair.is_some());
        assert!(self.buffers.is_some());
        assert!(self.sort_buffer.is_some());

        let qpair = self.qpair.as_mut().unwrap();
        let buffers = self.buffers.as_mut().unwrap();
        let mut output_buffer = self.sort_buffer.as_mut().unwrap();
        assert!(buffers.len() >= NUM_THREADS, "At least NUM_THREADS 2MiB buffers required for each parallel merge thread");

        let mut minHeap = BinaryHeap::new();
        let mut write_elements: Vec<usize> = vec![0; NUM_THREADS];
        let mut write_hugepages: Vec<usize> = vec![0; NUM_THREADS];

        let tailsize = (total_length + output_offset) % (LBA_SIZE / 8);
        println!("Thread {} starting thread merge with indices: {:?}, start_lba: {}, output_lba: {}, output_offset: {}, total_length: {}, input_length: {}, tailsize: {}", rayon::current_thread_index().unwrap(), indices, start_lba, output_lba, output_offset, total_length, input_length_byte, tailsize);

        // read first hugepages (2M) of each chunk
        for i in 0..NUM_THREADS {
            if indices[i].0 >= indices[i].1 {
                continue;
            }
            let lba = calculate_lba(indices[i].0, start_lba, i, input_length_byte);
            debug!("i={}, reading hugepage at lba={}", i, lba);
            read_write_hugepage_2M(qpair, lba, &mut buffers[i], false);
            //debug!("Buffer read: {:?}", u8_to_u64_slice(&mut buffers[i][0..1024 * 8]));
            // push first element into minHeap
            let idx = indices[i].0 % (HUGE_PAGE_SIZE_2M / 8);
            debug!("Pushing first element {} (Array: {}) to minHeap", u8_to_u64(&mut buffers[i][idx*8..idx*8 + 8]), i);
            minHeap.push(HeapEntry { value: u8_to_u64(&mut buffers[i][output_offset * 8..output_offset * 8 + 8]), array: i });

            write_elements[i] += 1;
        }

        let mut write_idx = output_offset;
        let mut written_lba = 0;

        loop {
            if let Some(HeapEntry { value, array }) = minHeap.pop() {
                debug!("Min: {}, Array: {}", value, array);
                let mut next_min = value;
                'inner: loop {
                    debug!("Writing {} (Array: {})to output buffer. Write index: {}", next_min, array, write_idx);
                    output_buffer[write_idx * 8..(write_idx + 1) * 8].copy_from_slice(&next_min.to_le_bytes()[0..8]); // TODO: double check
                    write_idx += 1;

                    if write_idx * 8 % HUGE_PAGE_SIZE_1G == 0 {
                        debug!("Output buffer is full. write_idx ({write_idx}) + written_lba ({written_lba}) * 8 / LBA_SIZE + tailsize ({tailsize}) <= total_length ({total_length})");
                        if write_idx + (written_lba*8/LBA_SIZE) + tailsize <= total_length {
                            debug!("Output buffer is full, writing whole hugepage to ssd");
                            read_write_hugepage_1G(qpair, output_lba + written_lba, &mut output_buffer, true);
                            written_lba += LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G;
                        } else {
                            // write all but tailsize elements to ssd
                            let elements_to_write = total_length - tailsize;
                            debug!("Output buffer is full, writing {elements_to_write} elements to ssd");
                            read_write_elements(qpair, &mut output_buffer, output_lba + written_lba, output_offset, elements_to_write, true);
                            written_lba += elements_to_write / LBA_SIZE;
                            assert_eq!((elements_to_write+output_offset) % LBA_SIZE, 0);
                        }
                        write_idx = 0;
                    }

                    let idx_write = write_elements[array] + indices[array].0;
                    debug!("idx_write: {} (write_elements[{}]: {} + indices[{}].0: {})", idx_write, array, write_elements[array], array, indices[array].0);
                    if idx_write < indices[array].1 {
                        // check if we need to load new hugepage
                        if idx_write * 8 % HUGE_PAGE_SIZE_2M == 0 {
                            let lba = calculate_lba(idx_write, start_lba, array, input_length_byte);
                            debug!("Reading new hugepage for array {} starting at lba {}", array, lba);
                            read_write_hugepage_2M(qpair, lba, &mut buffers[array], false);
                        }
                        let next_element = u8_to_u64(&mut buffers[array][(idx_write % (HUGE_PAGE_SIZE_2M / 8)) * 8..(idx_write % (HUGE_PAGE_SIZE_2M / 8)) * 8 + 8]);
                        write_elements[array] += 1;

                        // TODO: check if only elements of one array remaining.

                        if let Some(min) = minHeap.peek() { // check if new element is smaller than current min
                            if next_element <= min.value {
                                debug!("Next element {} <= current min {}", next_element, min.value);
                                next_min = next_element;
                                continue 'inner;
                            } else {
                                debug!("Next element {} > current min {} => Pushing to minheap", next_element, min.value);
                                minHeap.push(HeapEntry { value: next_element, array });
                                break 'inner;
                            }
                        } else { // TODO: maybe do smarter
                            next_min = next_element;
                            continue 'inner;
                        }
                    } else {
                        break 'inner;
                    }
                }
            } else {
                break;
            }
        }
        let mut elements_to_write = write_idx - tailsize;
        if elements_to_write > 0 {
            println!("Thread {}: final writing {elements_to_write} elements from output to lba {} (write_idx: {}, tailsize: {})", rayon::current_thread_index().unwrap(), output_lba + written_lba, write_idx, tailsize);
            read_write_elements(qpair, &mut output_buffer, output_lba + written_lba, 0, elements_to_write, true);
        }
        println!("Thread {}: remaining elements: {:?}", rayon::current_thread_index().unwrap(), u8_to_u64_slice(&mut output_buffer[elements_to_write * 8..write_idx * 8]));
        u8_to_u64_slice(&mut output_buffer[elements_to_write * 8..write_idx * 8]).to_vec()
    }
}

impl IPS2RaSorter {
    // Careful: returns #smaller elements, not index!
    pub fn binary_search_indices(&mut self, separators: &[u64], start_lba: usize, length: usize) -> Vec<usize> {
        debug!("Starting binary searching for {:?} from lba {} with length {}", separators, start_lba, length);
        separators.iter().map(|&sep| {
            match self.binary_search_ext(&sep, start_lba, length) {
                Ok(idx) => idx + 1,
                Err(idx) => {
                    debug!("Element {} not found directly. Using next smaller element at idx {}", sep, idx);
                    idx
                }
            }
        }).collect()
    }

    fn binary_search_ext(&mut self, element: &u64, start_lba: usize, length: usize) -> Result<usize, usize> {
        debug!("Thread {} binary searching for element {}. Start_lba: {}, length: {}", rayon::current_thread_index().unwrap(), element, start_lba, length);
        let mut size = length;
        let mut left = 0;
        let mut right = length;

        while left < right {
            let half = left + size / 2;
            let loaded_element = self.load_element(start_lba, half);
            //debug!("Element: {}, Half: {}, Left: {}, Right: {}, Loaded Element: {} (lba: {})", element, half, left, right, loaded_element, start_lba);
            match loaded_element.cmp(element) {
                Equal => {
                    debug!("Thread {} found element {} at index {}", rayon::current_thread_index().unwrap() , element, half);
                    return Ok(half);
                }
                Less => {
                    left = half + 1;
                }
                Greater => {
                    right = half;
                }
            }
            size = right - left;
        }
        Err(left)
    }

    fn load_element(&mut self, start_lba: usize, idx: usize) -> u64 {
        assert!(self.qpair.is_some());
        assert!(self.sort_buffer.is_some());
        let lba = idx * 8 / LBA_SIZE + start_lba;
        let offset = idx % (LBA_SIZE / 8);
        read_write_elements(self.qpair.as_mut().unwrap(), self.sort_buffer.as_mut().unwrap(), lba, offset, 1, false);
        //debug!("start_lba: {}, offset: {}, read: {:?}", lba, offset, u8_to_u64(&mut self.sort_buffer.as_mut().unwrap()[offset*8..offset*8 + 8]));
        u8_to_u64(&mut self.sort_buffer.as_mut().unwrap()[offset * 8..offset * 8 + 8])
    }
}
//vec![vec![2048, 4096, 6144], vec![4096, 8192, 12288]];

pub fn compute_local_separators(input: &[u64], num_separators: usize) -> Vec<u64> {
    let chunk_size = input.len() / (num_separators + 1);
    (1..=num_separators)
        // Pick equidistant separators from local array
        .map(|i| input[i * chunk_size])
        .collect()
}

pub fn transform_indices_to_ranges(local_indices: &Vec<Vec<usize>>, array_len: usize, num_threads: usize) -> Vec<Vec<(usize, usize)>> {
    let num_arrays = local_indices.len();
    let mut ranges: Vec<Vec<(usize, usize)>> = vec![vec![(0, 0); num_arrays]; num_threads];

    // Iterate over the number of threads
    for thread in 0..num_threads {
        // Iterate over the arrays being merged
        for array in 0..num_arrays {
            // The start of the range is 0 for the first thread, otherwise it's the previous separator
            let start = if thread == 0 {
                0
            } else {
                local_indices[array][thread - 1]
            };

            // The end of the range is the current separator for intermediate threads,
            // or the array length for the last thread
            let end = if thread == num_threads - 1 {
                array_len
            } else {
                local_indices[array][thread]
            };

            // Assign the start and end as the range for this thread and array
            ranges[thread][array] = (start, end);
        }
    }

    ranges
}

fn calculate_lba(idx: usize, start_lba: usize, i: usize, input_length: usize) -> usize {
    assert_eq!(input_length % LBA_SIZE, 0, "Input length must be a multiple of LBA_SIZE");
    //debug!("Calculating lba: i={}, input_length={}, start_lba={}, idx={} => i*input_length/LBA_SIZE + start_lba + idx*8/LBA_SIZE = {}", i, input_length, start_lba, idx, i * input_length / LBA_SIZE + start_lba + idx * 8 / LBA_SIZE);
    i * input_length / LBA_SIZE + start_lba + idx * 8 / LBA_SIZE
}








