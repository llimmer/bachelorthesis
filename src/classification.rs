use log::{debug, info};
use vroom::memory::DmaSlice;
use crate::config::{K, BLOCKSIZE, CHUNKS_PER_HUGE_PAGE_2M, CHUNK_SIZE, LBA_PER_CHUNK, ELEMENTS_PER_CHUNK, HUGE_PAGE_SIZE_2M};
use crate::conversion::{u64_to_u8_slice, u8_to_u64};
use crate::sorter::{DMATask, IPS2RaSorter, Task};

impl IPS2RaSorter {
    pub fn classify(&mut self, task: &mut Task) {
        let mut write_idx = 0;

        for i in 0..task.arr.len() {
            unsafe {
                let element = task.arr.get_unchecked(i);
                let block_idx = find_bucket_ips2ra(*element, task.level);

                *self.element_counts.get_unchecked_mut(block_idx) += 1;
        
            
                debug!("i = {i} element = {element} -> Bucket {block_idx}");

                // TODO: paper suggests to check if full first, then insert. Maybe change.
                *self.blocks[block_idx].get_unchecked_mut(self.block_counts[block_idx]) = *element;
                *self.block_counts.get_unchecked_mut(block_idx) += 1;

                if *self.block_counts.get_unchecked(block_idx) == BLOCKSIZE {
                    debug!("Block {block_idx} full, writing to disk: {:?}", self.blocks[block_idx]);
                    let target_slice = &mut task.arr[write_idx..write_idx + BLOCKSIZE];
                    target_slice.copy_from_slice(&self.blocks[block_idx]);
                    write_idx += BLOCKSIZE;
                    *self.block_counts.get_unchecked_mut(block_idx) = 0;
                }
            }
        }

        self.classified_elements = write_idx;
    }

    pub fn classify_ext(&mut self, task: &mut DMATask) {
        // using 2M hugepages
        debug!("Starting DMA classification: level {}, Chunks/HP: {}, tmp: {}", task.level, CHUNKS_PER_HUGE_PAGE_2M, ELEMENTS_PER_CHUNK* CHUNKS_PER_HUGE_PAGE_2M);
        let mut write_hugepage = 0;
        let mut write_chunk = 0;
        let mut write_idx = 0;

        assert!(self.qpair.is_some(), "Cannot classify_in_out without qpair");
        assert!(self.buffers.is_some(), "Cannot classify_in_out without buffers");




        let qpair = self.qpair.as_mut().unwrap();
        let buffer = self.buffers.as_mut().unwrap();
        let num_buffers = buffer.len();

        let max_buffered_elements = K*BLOCKSIZE;
        let max_storage = num_buffers*HUGE_PAGE_SIZE_2M/8;
        assert!(max_buffered_elements <= max_storage, "Not enough storage for classification: {} > {}", max_buffered_elements, max_storage);


        //assert!(task.size > 4*CHUNK_SIZE/8, "Task size too small for DMA classification");
        // Load first HugePage
        for i in 0..CHUNKS_PER_HUGE_PAGE_2M {
            qpair.submit_io(&mut buffer[0].slice(i*CHUNK_SIZE..(i+1)*CHUNK_SIZE), (i*LBA_PER_CHUNK) as u64, false);
        }

        // DEBUG only
        //self.qpair.complete_io(CHUNKS_PER_HUGE_PAGE);
        // print hugepage
        //debug!("Hugepage 0: {:?}", u8_to_u64_slice(&mut self.dma_blocks[0][..CHUNKS_PER_HUGE_PAGE*CHUNK_SIZE]));


        let mut cur_hugepage = 0;
        let mut cur_chunk = 0;
        for i in 0..task.size {

            // update current indices
            let idx = i % (ELEMENTS_PER_CHUNK* CHUNKS_PER_HUGE_PAGE_2M);

            if i % ELEMENTS_PER_CHUNK == 0 {
                debug!("i = {i}, idx = {idx}, cur_hugepage = {cur_hugepage}, cur_chunk = {cur_chunk}");
                qpair.complete_io(1);
                if i != 0 {
                    cur_chunk = (cur_chunk + 1) % CHUNKS_PER_HUGE_PAGE_2M;
                    if i%(HUGE_PAGE_SIZE_2M /8) == 0{
                        cur_hugepage += 1;
                    }
                }
                // Load next chunk
                qpair.submit_io(&mut buffer[(cur_hugepage+1)%num_buffers].slice(cur_chunk*CHUNK_SIZE..(cur_chunk+1)*CHUNK_SIZE), (((cur_hugepage+1)* CHUNKS_PER_HUGE_PAGE_2M *LBA_PER_CHUNK)+cur_chunk*LBA_PER_CHUNK) as u64, false);
                debug!("Current Hugepage: {}, Current Chunk: {}, Loading LBA {} to hugepage {}, chunk {}", cur_hugepage, cur_chunk, ((cur_hugepage+1)*CHUNKS_PER_HUGE_PAGE_2M*LBA_PER_CHUNK)+cur_chunk*LBA_PER_CHUNK, (cur_hugepage+1)%num_buffers, cur_chunk);
            }

            let element = u8_to_u64(&(&buffer[cur_hugepage%num_buffers])[idx*8..idx*8+8]);

            let block_idx = find_bucket_ips2ra(element, task.level);
            unsafe {
                *self.element_counts.get_unchecked_mut(block_idx) += 1;
                debug!("i = {}, idx = {}, cur_hugepage = {}, cur_chunk = {}, element = {}, bucket = {}", i, idx, cur_hugepage, cur_chunk, element, block_idx);

                *self.blocks[block_idx].get_unchecked_mut(self.block_counts[block_idx]) = element;
                *self.block_counts.get_unchecked_mut(block_idx) += 1;

                if *self.block_counts.get_unchecked(block_idx) == BLOCKSIZE {
                    debug!("Block {block_idx} full, writing {BLOCKSIZE} elements to buffer {}: {:?}", write_hugepage % num_buffers, self.blocks[block_idx]);
                    let target_slice = &mut buffer[write_hugepage % num_buffers][write_chunk * CHUNK_SIZE..(write_chunk + 1) * CHUNK_SIZE];

                    let offset = write_idx % ELEMENTS_PER_CHUNK;
                    debug!("Write_idx: {write_idx}, offset: {}, target slice: ({}..{})", offset, offset*8, (offset+BLOCKSIZE)*8);
                    target_slice[offset*8..(offset+BLOCKSIZE)*8].copy_from_slice(u64_to_u8_slice(&mut self.blocks[block_idx]));

                    write_idx += BLOCKSIZE;
                    *self.block_counts.get_unchecked_mut(block_idx) = 0;

                    // write to disk if chunk is full
                    if write_idx % ELEMENTS_PER_CHUNK == 0 {
                        let wi = write_hugepage % num_buffers;
                        debug!("Writing hugepage {}, chunk {} to LBA {}", wi, write_chunk, write_hugepage*CHUNKS_PER_HUGE_PAGE_2M*LBA_PER_CHUNK+write_chunk*LBA_PER_CHUNK);
                        qpair.submit_io(&mut buffer[wi].slice(write_chunk * CHUNK_SIZE..(write_chunk + 1) * CHUNK_SIZE), ((write_hugepage * CHUNKS_PER_HUGE_PAGE_2M * LBA_PER_CHUNK) + write_chunk * LBA_PER_CHUNK) as u64, true);
                        write_chunk = (write_chunk + 1) % CHUNKS_PER_HUGE_PAGE_2M;
                        if write_chunk == 0 {
                            write_hugepage += 1;
                        }

                        qpair.complete_io(1);
                    }

                }
            }
        }

        // check for unwritten chunk
        if write_idx % ELEMENTS_PER_CHUNK != 0 {
            debug!("Final writing hugepage {}, chunk {} to LBA {}", write_hugepage, write_chunk, write_hugepage*CHUNKS_PER_HUGE_PAGE_2M*LBA_PER_CHUNK+write_chunk*LBA_PER_CHUNK);
            qpair.submit_io(&mut buffer[write_hugepage % num_buffers].slice(write_chunk*CHUNK_SIZE..(write_chunk+1)*CHUNK_SIZE), ((write_hugepage* CHUNKS_PER_HUGE_PAGE_2M *LBA_PER_CHUNK)+cur_chunk*LBA_PER_CHUNK) as u64, true);
            qpair.complete_io(1);
        }

        qpair.complete_io(CHUNKS_PER_HUGE_PAGE_2M);
        self.classified_elements = write_idx;
    }
}


pub fn find_bucket_ips2ra(input: u64, level: usize) -> usize {
    let number_bits = (K as u64).ilog2() as usize;
    //debug!("Number of bits: {:?}", number_bits);
    //debug!("Level: {:?}", level);
    let start = 64 - (number_bits * (level + 1));
    let mask = (1 << number_bits) - 1;
    ((input >> start) & mask) as usize
}




