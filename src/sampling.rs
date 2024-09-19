use crate::config::{K};
use crate::{read_write_hugepage, u8_to_u64_slice, CHUNKS_PER_HUGE_PAGE_1G, HUGE_PAGES_1G, HUGE_PAGE_SIZE_1G, LBA_PER_CHUNK};
use crate::sorter::{DMATask, IPS2RaSorter, Task};

impl<'a> Task<'_> {
    pub fn sample(&mut self) {
        // TODO: implement correctly
        let max = self.arr.iter().max().unwrap();
        let lz = max.leading_zeros();
        let klog2 = (K as u64).ilog2();
        let zero_blocks = (lz as f64 / klog2 as f64).floor() as u32;
        self.level = zero_blocks as usize;
    }
}

impl IPS2RaSorter {
    pub fn sample(&mut self, task: &mut DMATask) {
        let mut max = u64::MAX;
        let mut remaining = task.size;
        for i in 0..task.size / (HUGE_PAGE_SIZE_1G/8) {
            read_write_hugepage(self.qpair.as_mut().unwrap(), task.start_lba + i * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G, self.sort_buffer.as_mut().unwrap(), false);
            let u64slice = u8_to_u64_slice(&mut self.sort_buffer.as_mut().unwrap()[0..HUGE_PAGE_SIZE_1G]);
            let tmp_max = u64slice[{
                if remaining >= HUGE_PAGES_1G / 8 {
                    remaining -= HUGE_PAGES_1G / 8;
                    0..HUGE_PAGES_1G
                } else {
                    let res = remaining;
                    remaining = 0;
                    0..res*8
                }
            }].iter().max().unwrap();
            if *tmp_max < max {
                max = *tmp_max;
            }
            remaining -= HUGE_PAGES_1G / 8;
        }
        let lz = max.leading_zeros();
        let klog2 = (K as u64).ilog2();
        let zero_blocks = (lz as f64 / klog2 as f64).floor() as u32;
        task.level = zero_blocks as usize;
    }
}

