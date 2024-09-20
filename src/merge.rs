use std::collections::BinaryHeap;
use std::io;
use std::time::Duration;
use log::{debug, info};
use vroom::memory::Dma;
use vroom::NvmeQueuePair;
use crate::config::{CHUNKS_PER_HUGE_PAGE_1G, HUGE_PAGES_1G, HUGE_PAGE_SIZE_1G, LBA_PER_CHUNK};
use crate::conversion::u8_to_u64_slice;
use crate::sort::read_write_hugepage;

struct HeapEntry{
    value: u64,
    hugepage_idx: usize,
    element_idx: usize,
    remaining: usize,
}

impl Eq for HeapEntry {}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.value.cmp(&self.value)
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub fn merge_sequential(qpair: &mut NvmeQueuePair, len: usize, buffer: &mut Vec<Dma<u8>>, output_buffer: &mut Dma<u8>) {
    assert_eq!(buffer.len(), HUGE_PAGES_1G - 1);


    let mut output = u8_to_u64_slice(&mut output_buffer[0..HUGE_PAGE_SIZE_1G]);

    let total_number_hugepages = (len + HUGE_PAGE_SIZE_1G / 8 - 1) / (HUGE_PAGE_SIZE_1G / 8);
    let last_hugepage_size = (len-1) % (HUGE_PAGE_SIZE_1G / 8) + 1;

    let mut read_offset = 0;
    let mut write_offset = total_number_hugepages;
    let mut last_write_offset = write_offset;

    let mut timeForIO= Duration::new(0,0);

    let max = (total_number_hugepages as f64).log((HUGE_PAGES_1G - 1) as f64).ceil() as usize;
    info!("Total number of hugepages: {total_number_hugepages}, max runs: {max}");

    for i in 0..max {
        let input_length = (HUGE_PAGES_1G - 1).pow(i as u32);
        let result_length = input_length * (HUGE_PAGES_1G - 1);
        info!("i = {i}, input length = {input_length}, result length = {result_length}, read offset = {read_offset}, write offset = {write_offset}\n");
        info!("j = (0..{})", (total_number_hugepages+result_length-1) / result_length);
        for j in 0..(total_number_hugepages+result_length-1) / result_length {
            info!("i = {i}, j = {j}\n");
            let mut write_idx = 0;
            let mut written_hugepages = 0;
            let mut hugepage_increments = [0; HUGE_PAGES_1G - 1];
            let mut min_heap = BinaryHeap::with_capacity(HUGE_PAGES_1G - 1);

            // Load first HUGE_PAGES-1 hugepages and push first elements to heap
            for k in 0..HUGE_PAGES_1G - 1 {
                info!("Initial read: hugepage: {} (offset: {}), index: {k}", j*result_length + k*input_length + read_offset, read_offset);

                let start = std::time::Instant::now();
                read_write_hugepage(qpair, (j*result_length + k*input_length + read_offset) * LBA_PER_CHUNK*CHUNKS_PER_HUGE_PAGE_1G, &mut buffer[k], false);
                let duration = start.elapsed();
                timeForIO+=duration;

                info!("Hugepeage read: {:?}", u8_to_u64_slice(&mut buffer[k][0..HUGE_PAGE_SIZE_1G]));

                { // scope to avoid borrowing issues
                    let slice = u8_to_u64_slice(&mut buffer[k][0..HUGE_PAGE_SIZE_1G]);

                    if (j * result_length + (k+1)*input_length) >= total_number_hugepages {
                        // last hugepage block

                        let block_length = total_number_hugepages - j * result_length - k * input_length;
                        let len = {
                            if block_length > 1 {
                                HUGE_PAGE_SIZE_1G / 8
                            } else {
                                last_hugepage_size
                            }
                        };
                        info!("Last hugepage block detected! Block length: {}, slice length: {}, index: {}", block_length, len, k);

                        // TODO: check if len-1 or len
                        min_heap.push(HeapEntry {
                            value: slice[0],
                            hugepage_idx: k,
                            element_idx: 0,
                            remaining: len-1,
                        });
                        break;
                    }
                    // Push the first element from the slice into the heap
                    min_heap.push(HeapEntry {
                        value: slice[0],
                        hugepage_idx: k,
                        element_idx: 0,
                        remaining: slice.len()-1,
                    });
                }
            }

            // check if min_heap is not empty
            while let Some(HeapEntry { value, hugepage_idx, element_idx, remaining }) = min_heap.pop() {
                info!("Current min: {value}, hugepage_idx: {hugepage_idx}, element_idx: {element_idx}, remaining: {remaining}, output after: {}", write_idx+1);
                // Write the value to the output buffer
                output[write_idx] = value;
                write_idx += 1;

                // If the output buffer is full, write to SSD and reset index
                if write_idx % (HUGE_PAGE_SIZE_1G / 8) == 0 {
                    info!("Output buffer full, writing to SSD hugepage {} (written hugepages: {written_hugepages}):", j * result_length + write_offset + written_hugepages);

                    let start = std::time::Instant::now();
                    read_write_hugepage(qpair, (j * result_length + write_offset + written_hugepages)*LBA_PER_CHUNK*CHUNKS_PER_HUGE_PAGE_1G, output_buffer, true);
                    let duration = start.elapsed();
                    timeForIO+=duration;

                    info!("Hugepage written: {:?}", u8_to_u64_slice(&mut output_buffer[0..HUGE_PAGE_SIZE_1G]));
                    write_idx = 0;
                    written_hugepages += 1;
                    last_write_offset = write_offset;

                    // Recreate the output slice after writing to SSD
                    output = u8_to_u64_slice(&mut output_buffer[0..HUGE_PAGE_SIZE_1G]);
                }

                // Read the next element from same slice
                if remaining > 0 {
                    let next_value = {
                        let slice = u8_to_u64_slice(&mut buffer[hugepage_idx][0..HUGE_PAGE_SIZE_1G]);
                        slice[element_idx + 1]
                    };

                    // Push next element
                    min_heap.push(HeapEntry {
                        value: next_value,
                        hugepage_idx,
                        element_idx: element_idx + 1,
                        remaining: remaining - 1,
                    });
                } else {
                    info!("Hugepage_idx {hugepage_idx} exhausted");
                    // Check if there are more hugepages in same run
                    if hugepage_increments[hugepage_idx] + 1 < (result_length/(HUGE_PAGES_1G - 1)) {
                        info!("Reading next hugepage. Increment is now: {}, resultlength/hugepages-1: {}", hugepage_increments[hugepage_idx] + 1, (result_length/(HUGE_PAGES_1G - 1)));
                        hugepage_increments[hugepage_idx] += 1;

                        // check if already last hugepage //TODO: double check
                        if j * result_length + hugepage_idx*input_length + hugepage_increments[hugepage_idx] >= total_number_hugepages {
                            info!("Last hugepage, not adding any more hugepages");
                            continue;
                        }


                        info!("Index: {}, Reading hugepage {} (offset: {})", hugepage_idx, j * result_length + hugepage_idx*input_length + hugepage_increments[hugepage_idx] + read_offset, read_offset);
                        // Read the next hugepage into the buffer

                        let start = std::time::Instant::now();
                        read_write_hugepage(qpair, (j * result_length + hugepage_idx*input_length + hugepage_increments[hugepage_idx] + read_offset)*LBA_PER_CHUNK*CHUNKS_PER_HUGE_PAGE_1G, &mut buffer[hugepage_idx], false);
                        let duration = start.elapsed();
                        timeForIO+=duration;

                        info!("Hugepeage read: {:?}", u8_to_u64_slice(&mut buffer[hugepage_idx][0..HUGE_PAGE_SIZE_1G]));

                        let next_value = {
                            let slice = u8_to_u64_slice(&mut buffer[hugepage_idx][0..HUGE_PAGE_SIZE_1G]);
                            slice[0]
                        };

                        //check if newly read hugepage is last hugepage
                        if j * result_length + hugepage_idx*input_length + hugepage_increments[hugepage_idx] + 1 >= total_number_hugepages {
                            info!("Last hugepage detected! Slice length: {},index: {}", last_hugepage_size, hugepage_idx);
                            // Push the first element of new hugepage
                            min_heap.push(HeapEntry {
                                value: next_value,
                                hugepage_idx,
                                element_idx: 0,
                                remaining: last_hugepage_size - 1,
                            });
                        } else {
                            // Push the first element of new hugepage
                            min_heap.push(HeapEntry {
                                value: next_value,
                                hugepage_idx,
                                element_idx: 0,
                                remaining: HUGE_PAGE_SIZE_1G / 8-1,
                            });
                        }
                    } else {
                        info!("No more hugepages to read");
                    }
                }
            }

            // read line from stdin
            //let mut input = String::new();
            //io::stdin().read_line(&mut input).unwrap();

            if write_idx > 0 {
                info!("Output buffer not empty at end, writing {} elements to SSD hugepage {} (written hugepages: {written_hugepages}):", write_idx, j * result_length + write_offset + written_hugepages);
                let start = std::time::Instant::now();
                read_write_hugepage(qpair, (j * result_length + write_offset + written_hugepages)*LBA_PER_CHUNK*CHUNKS_PER_HUGE_PAGE_1G, output_buffer, true);
                let duration = start.elapsed();
                info!("Hugepage written: {:?}", u8_to_u64_slice(&mut output_buffer[0..HUGE_PAGE_SIZE_1G]));
                write_idx = 0;
                written_hugepages += 1;
                last_write_offset = write_offset;

                // Recreate the output slice after writing to SSD
                output = u8_to_u64_slice(&mut output_buffer[0..HUGE_PAGE_SIZE_1G]);
            }
        }

        info!("Swapping read and write offset");
        let tmp = read_offset;
        read_offset = write_offset;
        write_offset = tmp;
    }

    info!("Last write offset: {last_write_offset}");
    if last_write_offset != 0 { // TODO: do more efficient or avoid in sorting.
        // copying all hugepages to the beginning
        println!("Merge: Copy needed!");
        for i in 0..total_number_hugepages{
            read_write_hugepage(qpair, (i + last_write_offset)*LBA_PER_CHUNK*CHUNKS_PER_HUGE_PAGE_1G, output_buffer, false);
            read_write_hugepage(qpair, i*LBA_PER_CHUNK*CHUNKS_PER_HUGE_PAGE_1G, output_buffer, true);
        }
    } else {
        println!("Merge: No Copy needed!");
    }
    println!("Time for IO: {:?}", timeForIO);
}

#[cfg(test)]
mod tests {
    use vroom::memory::DmaSlice;
    use vroom::QUEUE_LENGTH;
    use crate::config::CHUNK_SIZE;
    use crate::conversion::u64_to_u8_slice;
    use crate::setup::clear_chunks;
    use super::*;

    #[test]
    fn test_merge_sequential() {
        let mut nvme = vroom::init("0000:00:04.0").unwrap();
        let mut qpair = nvme.create_io_queue_pair(QUEUE_LENGTH).unwrap();
        let mut buffer = Dma::allocate(HUGE_PAGE_SIZE_1G).unwrap();
        // Prepare data: //todo: remove
        info!("Clearing hugepages");
        clear_chunks(CHUNKS_PER_HUGE_PAGE_1G *1024+10, &mut qpair);
        info!("Done");
        // prepare first 4 hugepages
        let len = HUGE_PAGE_SIZE_1G /8;
        let total_length = len*5 - 1000;
        let number_cunks = (total_length+len-1)/len;
        for i in 0..number_cunks-1 {
            let mut data: Vec<u64> = (0..len as u64).map(|x| x*number_cunks as u64+(i) as u64).collect();
            buffer[0..HUGE_PAGE_SIZE_1G].copy_from_slice(u64_to_u8_slice(&mut data));
            let tmp = qpair.submit_io(&mut buffer.slice(0..HUGE_PAGE_SIZE_1G), (i*LBA_PER_CHUNK* CHUNKS_PER_HUGE_PAGE_1G) as u64, true);
            qpair.complete_io(tmp);
            info!("Input {i}: {:?}", data);
            //assert_eq!(tmp, 256);
        }
        // prepare last hugepage
        let mut data: Vec<u64> = (0..(total_length%len) as u64).map(|x| x*number_cunks as u64+(number_cunks-1) as u64).collect();
        buffer[0..HUGE_PAGE_SIZE_1G].copy_from_slice(&[0u8; HUGE_PAGE_SIZE_1G]);
        buffer[0..data.len()*8].copy_from_slice(u64_to_u8_slice(&mut data));
        let tmp = qpair.submit_io(&mut buffer.slice(0..data.len()*8), ((number_cunks-1)*LBA_PER_CHUNK* CHUNKS_PER_HUGE_PAGE_1G) as u64, true);
        qpair.complete_io(tmp);
        info!("Input {}: {:?}", number_cunks-1, data);

        let mut buffers: Vec<Dma<u8>> = Vec::with_capacity(HUGE_PAGES_1G - 1);
        for i in 0..HUGE_PAGES_1G - 1 {
            buffers.push(Dma::allocate(HUGE_PAGE_SIZE_1G).unwrap());
        }
        let mut output_buffer = Dma::allocate(HUGE_PAGE_SIZE_1G).unwrap();
        merge_sequential(&mut qpair, total_length, &mut buffers, &mut output_buffer);



        let mut big_hugepage: Dma<u8> = Dma::allocate(1024*1024*1024).unwrap();
        // read first len*number_chunks elements
        let bytes_to_read = total_length*8;
        for i in 0..(bytes_to_read+CHUNK_SIZE-1)/CHUNK_SIZE{
            let tmp = qpair.submit_io(&mut big_hugepage.slice(i*CHUNK_SIZE..(i+1)*CHUNK_SIZE), (i*LBA_PER_CHUNK) as u64, false);
            qpair.complete_io(tmp);
        }
        let slice = u8_to_u64_slice(&mut big_hugepage[0..bytes_to_read]);
        info!("\n\nResult: {:?}", slice);

        for i in 1..slice.len() {
            assert_ne!(slice[i - 1], slice[i], "Duplicate elements at {} and {}", i - 1, i);
        }
        info!("Test passed");
    }
}




