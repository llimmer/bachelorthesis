use std::error::Error;
use vroom::{NvmeDevice, QUEUE_LENGTH};
use vroom::memory::Dma;
use crate::{read_write_hugepage, u8_to_u64_slice, CHUNKS_PER_HUGE_PAGE_1G, HUGE_PAGES_1G, HUGE_PAGE_SIZE_1G, LBA_PER_CHUNK};
use crate::merge::merge_sequential;
use crate::sorter::{IPS2RaSorter, Task};


pub fn sequential_sort_merge(mut nvme: NvmeDevice, len: usize) -> Result<NvmeDevice, Box<dyn Error>> { // TODO: check if maybe better as impl IPS2RaSorter

    let mut qpair = nvme.create_io_queue_pair(QUEUE_LENGTH)?;
    let mut sort_buffer = Dma::allocate(HUGE_PAGE_SIZE_1G)?;

    let mut buffers: Vec<Dma<u8>> = Vec::new();
    for _ in 0..HUGE_PAGES_1G - 1 {
        buffers.push(Dma::allocate(HUGE_PAGE_SIZE_1G)?);
    }

    let mut sorter = IPS2RaSorter::new_sequential();

    let mut remaining = len;
    for i in 0..((len + HUGE_PAGE_SIZE_1G / 8 - 1) / (HUGE_PAGE_SIZE_1G / 8)) {
        // read hugepage from ssd
        println!("Reading hugepage {i}");
        read_write_hugepage(&mut qpair, i * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G, &mut sort_buffer, false);

        println!("Done");

        let u64slice = u8_to_u64_slice(&mut sort_buffer[0..{
            if remaining > HUGE_PAGE_SIZE_1G / 8 {
                remaining -= HUGE_PAGE_SIZE_1G / 8;
                HUGE_PAGE_SIZE_1G
            } else {
                let res = remaining;
                remaining = 0;
                res * 8
            }
        }]);
        println!("Creating and sampling task of length {}", u64slice.len());
        let mut task = Task::new(u64slice, 0);
        task.sample();
        println!("Done");

        println!("Sorting hugepage {i}");
        sorter.sort_sequential(&mut task);
        println!("Done");

        println!("Writing hugepage {i}");
        read_write_hugepage(&mut qpair, i * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G, &mut sort_buffer, true);
        println!("Done");

    }

    println!("Starting merge");
    merge_sequential(&mut qpair, len, &mut buffers, &mut sort_buffer);
    println!("Done");

    Ok(nvme)
}

pub fn parallel_sort_merge() {
    unimplemented!();
}



