use crate::config::*;
use crate::conversion::*;
use crate::sort::find_bucket_ips2ra;
use crate::sorter::*;
use vroom::memory::DmaSlice;
use log::{debug, info};

impl IPS2RaSorter {
    pub fn classify(&mut self, task: &mut Task) {
        let mut write_idx = 0;
        unsafe {
            for i in 0..task.arr.len() {
                let element = *task.arr.get_unchecked(i);
                let block_idx = find_bucket_ips2ra(element, task.level);

                debug!("i = {i} element = {element} -> Bucket {block_idx}");

                if *self.block_counts.get_unchecked(block_idx) == BLOCKSIZE {
                    debug!("Block {block_idx} full, writing to disk: {:?}", self.blocks[block_idx]);
                    let target_slice = &mut task.arr[write_idx..write_idx + BLOCKSIZE];
                    target_slice.copy_from_slice(&self.blocks[block_idx]);
                    write_idx += BLOCKSIZE;

                    *self.element_counts.get_unchecked_mut(block_idx) += BLOCKSIZE as u64;
                    *self.block_counts.get_unchecked_mut(block_idx) = 0;
                }


                *self.blocks[block_idx].get_unchecked_mut(self.block_counts[block_idx]) = element;
                *self.block_counts.get_unchecked_mut(block_idx) += 1;
            }

            // check for partially filled blocks
            for i in 0..K {
                *self.element_counts.get_unchecked_mut(i) += self.block_counts[i] as u64;
            }
        }

        self.classified_elements = write_idx;
    }

    pub fn classify_ext(&mut self, task: &mut ExtTask) {
        // using 2M hugepages
        debug!("Starting DMA classification: level {}, Chunks/HP: {}, tmp: {}", task.level, CHUNKS_PER_HUGE_PAGE_2M, ELEMENTS_PER_CHUNK* CHUNKS_PER_HUGE_PAGE_2M);
        let mut write_hugepage = task.start_lba / (CHUNKS_PER_HUGE_PAGE_2M * LBA_PER_CHUNK);
        let mut write_chunk = (task.start_lba % (CHUNKS_PER_HUGE_PAGE_2M * LBA_PER_CHUNK)) / LBA_PER_CHUNK;
        let mut write_idx = task.offset;

        assert!(self.qpair.is_some(), "Cannot classify_in_out without qpair");
        assert!(self.buffers.is_some(), "Cannot classify_in_out without buffers");


        let qpair = self.qpair.as_mut().unwrap();
        let buffer = self.buffers.as_mut().unwrap();
        let num_buffers = buffer.len();

        let max_buffered_elements = K * BLOCKSIZE + task.offset;
        let max_storage = num_buffers * HUGE_PAGE_SIZE_2M / 8;
        assert!(max_buffered_elements <= max_storage, "Not enough storage for classification: {} > {}", max_buffered_elements, max_storage);


        // TODO: change to load as many hugepages as supported by QUEUE_LENGTH
        debug!("Loading first hugepage:");
        for i in 0..CHUNKS_PER_HUGE_PAGE_2M {
            debug!("Loading chunk {} (LBA: {})", i, i*LBA_PER_CHUNK + task.start_lba);
            qpair.submit_io(&mut buffer[0].slice(i * CHUNK_SIZE..(i + 1) * CHUNK_SIZE), ((i * LBA_PER_CHUNK) + task.start_lba) as u64, false);
        }


        let mut cur_hugepage = 0;
        let mut cur_chunk = 0;
        for i in task.offset..task.size + task.offset {

            // update current indices
            let idx = i % (ELEMENTS_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_2M);

            if i % ELEMENTS_PER_CHUNK == 0 || i == task.offset {
                debug!("i: {i}, idx: {idx}, cur_hugepage: {cur_hugepage}, cur_chunk: {cur_chunk}");
                qpair.complete_io(1);
                if i != task.offset {
                    cur_chunk = (cur_chunk + 1) % CHUNKS_PER_HUGE_PAGE_2M;
                    if i % (HUGE_PAGE_SIZE_2M / 8) == 0 {
                        cur_hugepage += 1;
                    }
                }
                // Load next chunk
                // TODO: only load if elements remaining
                qpair.submit_io(&mut buffer[(cur_hugepage + 1) % num_buffers].slice(cur_chunk * CHUNK_SIZE..(cur_chunk + 1) * CHUNK_SIZE), (((cur_hugepage + 1) * CHUNKS_PER_HUGE_PAGE_2M * LBA_PER_CHUNK) + cur_chunk * LBA_PER_CHUNK + task.start_lba) as u64, false);
                debug!("Current Hugepage: {}, Current Chunk: {}, Loading LBA {} to hugepage {}, chunk {}", cur_hugepage, cur_chunk, ((cur_hugepage+1)*CHUNKS_PER_HUGE_PAGE_2M*LBA_PER_CHUNK)+cur_chunk*LBA_PER_CHUNK + task.start_lba, (cur_hugepage+1)%num_buffers, cur_chunk);
            }

            let element = u8_to_u64(&(&buffer[cur_hugepage % num_buffers])[idx * 8..idx * 8 + 8]);
            let block_idx = find_bucket_ips2ra(element, task.level);
            unsafe {
                debug!("i = {}, idx = {}, cur_hugepage = {}, cur_chunk = {}, element = {}, bucket = {}", i, idx, cur_hugepage, cur_chunk, element, block_idx);

                if *self.block_counts.get_unchecked(block_idx) == BLOCKSIZE {
                    debug!("Block {block_idx} full, writing {BLOCKSIZE} elements to buffer {}: {:?}", write_hugepage % num_buffers, self.blocks[block_idx]);
                    *self.element_counts.get_unchecked_mut(block_idx) += BLOCKSIZE as u64;

                    let offset = write_idx % ELEMENTS_PER_CHUNK;
                    let mut remaining = CHUNK_SIZE / 8 - offset;
                    debug!("Offset: {}, remaining: {}", offset, remaining);

                    if remaining >= BLOCKSIZE {
                        let target_slice = &mut buffer[write_hugepage % num_buffers][write_chunk * CHUNK_SIZE..(write_chunk + 1) * CHUNK_SIZE];
                        debug!("Write_idx: {write_idx}, offset: {}, target slice: ({}..{})", offset, offset*8, (offset+BLOCKSIZE)*8);
                        target_slice[offset * 8..(offset + BLOCKSIZE) * 8].copy_from_slice(u64_to_u8_slice(&mut self.blocks[block_idx]));

                        write_idx += BLOCKSIZE;
                        *self.block_counts.get_unchecked_mut(block_idx) = 0;

                        // write to disk if chunk is full
                        if write_idx % ELEMENTS_PER_CHUNK == 0 {
                            let wi = write_hugepage % num_buffers;
                            debug!("Writing hugepage {}, chunk {} to LBA {}", wi, write_chunk, write_hugepage*CHUNKS_PER_HUGE_PAGE_2M*LBA_PER_CHUNK+write_chunk*LBA_PER_CHUNK + task.start_lba);
                            qpair.submit_io(&mut buffer[wi].slice(write_chunk * CHUNK_SIZE..(write_chunk + 1) * CHUNK_SIZE), ((write_hugepage * CHUNKS_PER_HUGE_PAGE_2M * LBA_PER_CHUNK) + write_chunk * LBA_PER_CHUNK + task.start_lba) as u64, true);
                            write_chunk = (write_chunk + 1) % CHUNKS_PER_HUGE_PAGE_2M;
                            if write_chunk == 0 {
                                write_hugepage += 1;
                            }

                            qpair.complete_io(1);
                        }
                    } else {
                        // remaining <= BLOCKSIZE
                        let target_slice1 = &mut buffer[write_hugepage % num_buffers][write_chunk * CHUNK_SIZE..(write_chunk + 1) * CHUNK_SIZE];
                        target_slice1[offset * 8..(offset + remaining) * 8].copy_from_slice(u64_to_u8_slice(&mut self.blocks[block_idx][0..remaining]));
                        assert_eq!(offset + remaining, ELEMENTS_PER_CHUNK, "Not enough space in buffer for block"); //todo: remove after debug

                        // write to disk
                        let wi = write_hugepage % num_buffers;
                        debug!("Writing hugepage {}, chunk {} to LBA {}", wi, write_chunk, write_hugepage*CHUNKS_PER_HUGE_PAGE_2M*LBA_PER_CHUNK+write_chunk*LBA_PER_CHUNK + task.start_lba);
                        qpair.submit_io(&mut buffer[wi].slice(write_chunk * CHUNK_SIZE..(write_chunk + 1) * CHUNK_SIZE), ((write_hugepage * CHUNKS_PER_HUGE_PAGE_2M * LBA_PER_CHUNK) + write_chunk * LBA_PER_CHUNK + task.start_lba) as u64, true);
                        debug!("Wrote: {:?}", u8_to_u64_slice(&mut buffer[wi][write_chunk * CHUNK_SIZE..(write_chunk + 1) * CHUNK_SIZE]));
                        write_chunk = (write_chunk + 1) % CHUNKS_PER_HUGE_PAGE_2M;
                        if write_chunk == 0 {
                            write_hugepage += 1;
                        }

                        qpair.complete_io(1);

                        let target_slice2 = &mut buffer[write_hugepage % num_buffers][write_chunk * CHUNK_SIZE..(write_chunk + 1) * CHUNK_SIZE];
                        target_slice2[0..(BLOCKSIZE - remaining) * 8].copy_from_slice(u64_to_u8_slice(&mut self.blocks[block_idx][remaining..BLOCKSIZE]));
                        debug!("Wrote: {:?} to next chunk", &mut self.blocks[block_idx][remaining..BLOCKSIZE]);


                        write_idx += BLOCKSIZE;
                        *self.block_counts.get_unchecked_mut(block_idx) = 0;
                    }
                }
                *self.blocks[block_idx].get_unchecked_mut(self.block_counts[block_idx]) = element;
                *self.block_counts.get_unchecked_mut(block_idx) += 1;
            }
        }
        let remaining_elements = write_idx % ELEMENTS_PER_CHUNK;
        debug!("Classification done. {} elements remaining in buffer", remaining_elements);
        // check for unwritten chunk
        if write_idx % ELEMENTS_PER_CHUNK != 0 {
            debug!("Last chunk: {:?}", u8_to_u64_slice(&mut buffer[write_hugepage % num_buffers][write_chunk * CHUNK_SIZE..(write_chunk + 1) * CHUNK_SIZE]));
            let num_lba = (remaining_elements * 8 + LBA_SIZE - 1) / LBA_SIZE;
            let tmp = qpair.submit_io(&mut buffer[write_hugepage % num_buffers].slice(write_chunk * CHUNK_SIZE..write_chunk * CHUNK_SIZE + num_lba * LBA_SIZE), ((write_hugepage * CHUNKS_PER_HUGE_PAGE_2M * LBA_PER_CHUNK) + write_chunk * LBA_PER_CHUNK + task.start_lba) as u64, true);
            assert_eq!(tmp, 1);
            qpair.complete_io(1);
        }

        // check for partially filled blocks
        unsafe {
            for i in 0..K {
                *self.element_counts.get_unchecked_mut(i) += self.block_counts[i] as u64;
            }
        }


        write_idx -= task.offset;

        debug!("Completing last SQEs");
        qpair.complete_io(CHUNKS_PER_HUGE_PAGE_2M); // TODO: remove after bounds check
        debug!("Done");
        self.classified_elements = write_idx;
    }
}




