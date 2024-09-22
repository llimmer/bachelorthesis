#[cfg(test)]
mod sequential_merge{
    use log::info;
    use vroom::memory::{Dma, DmaSlice};
    use vroom::QUEUE_LENGTH;
    use bachelorthesis::{clear_chunks, merge_sequential, u64_to_u8_slice, u8_to_u64_slice, CHUNKS_PER_HUGE_PAGE_1G, CHUNK_SIZE, HUGE_PAGES_1G, HUGE_PAGE_SIZE_1G, LBA_PER_CHUNK};

    #[test]
    fn small_sequential(){
        /*
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
        info!("Test passed");*/
    }
}