use std::process::exit;
use log::debug;
use crate::config::{K, BLOCKSIZE, LBA_SIZE, DMA_BUFFERS};
use crate::conversion::{u64_to_u8_slice, u8_to_u64};
use crate::sorter::{DMATask, IPS2RaSorter, IPS2RaSorterDMA, Task};

impl IPS2RaSorter {
    pub unsafe fn classify(&mut self, task: &mut Task) {
        let mut write_idx = 0;

        for i in 0..task.arr.len() {
            let element = task.arr.get_unchecked(i);
            let block_idx = find_bucket_ips2ra(*element, task.level);
            *self.element_counts.get_unchecked_mut(block_idx) += 1;

            println!("i = {i} element = {element} -> Bucket {block_idx}");

            // TODO: paper suggests to check if full first, then insert. Maybe change.
            *self.blocks[block_idx].get_unchecked_mut(self.block_counts[block_idx]) = *element;
            *self.block_counts.get_unchecked_mut(block_idx) += 1;

            if *self.block_counts.get_unchecked(block_idx) == BLOCKSIZE {
                println!("Block {block_idx} full, writing to disk: {:?}", self.blocks[block_idx]);
                let target_slice = &mut task.arr[write_idx..write_idx + BLOCKSIZE];
                target_slice.copy_from_slice(&self.blocks[block_idx]);
                write_idx += BLOCKSIZE;
                *self.block_counts.get_unchecked_mut(block_idx) = 0;
            }
        }



        self.classified_elements = write_idx;
    }
}


impl IPS2RaSorterDMA {
    pub unsafe fn classify(&mut self, task: &mut DMATask) {
        println!("Starting DMA classification");
        let mut lba = 0;
        let mut write_idx = 0;

        // load first DMA_BUFFERS into memory
        for i in 0..DMA_BUFFERS {
            self.nvme.read(&mut self.dma_blocks[i], i as u64).unwrap();
        }

        for i in 0..task.size {
            let idx = i % BLOCKSIZE;
            let cur_lba = calculate_lba(i);

            let element = u8_to_u64(&(&self.dma_blocks[cur_lba])[idx*8..idx*8+8]);


            let block_idx = find_bucket_ips2ra(element, task.level);
            *self.element_counts.get_unchecked_mut(block_idx) += 1;

            //println!("i = {i}, LBA = {cur_lba}, idx = {idx}, element = {element} -> Bucket {block_idx}");

            *self.blocks[block_idx].get_unchecked_mut(self.block_counts[block_idx]) = element;
            *self.block_counts.get_unchecked_mut(block_idx) += 1;

            if *self.block_counts.get_unchecked(block_idx) == BLOCKSIZE {
                println!("Block {block_idx} full, writing to disk: {:?}", self.blocks[block_idx]);
                let target_slice = &mut self.dma_blocks[lba%DMA_BUFFERS][..BLOCKSIZE*8];
                target_slice.copy_from_slice(u64_to_u8_slice(&mut self.blocks[block_idx]));

                write_idx+=BLOCKSIZE;
                *self.block_counts.get_unchecked_mut(block_idx) = 0;


                //write lba back to disk
                self.nvme.write(&self.dma_blocks[lba%DMA_BUFFERS], lba as u64).unwrap();

                // read next lba from behind:
                if (lba+DMA_BUFFERS-1)*BLOCKSIZE < task.size {
                    self.nvme.read(&mut self.dma_blocks[lba%DMA_BUFFERS], (lba+DMA_BUFFERS) as u64).unwrap();
                }
                lba += 1;
            }
        }

        self.classified_elements = write_idx;
    }
}


pub fn find_bucket_ips2ra(input: u64, level: usize) -> usize {
    let number_bits = (K as u64).ilog2() as usize;
    let start = 64 - (number_bits * (level + 1));
    let mask = (1 << number_bits) - 1;
    ((input >> start) & mask) as usize
}

fn calculate_lba(input: usize) -> usize {
    let res = input / (LBA_SIZE / 64);
    res % DMA_BUFFERS
}


