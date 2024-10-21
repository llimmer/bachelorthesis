use crate::config::*;
use crate::conversion::*;
use crate::sort::{read_write_hugepage_1G};
use crate::sorter::{ExtTask, IPS2RaSorter, Task};
use std::cmp::{max, min};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};


impl<'a> Task<'_> {
    pub fn sample(&mut self) -> bool {
        let (level_begin, level_end) = self.sequential_get_levels();
        if level_begin == 0 && level_end == 0 {
            return false;
        }
        self.level_end = level_end;
        self.level = level_begin;
        true
    }

    pub fn sequential_get_levels(&mut self) -> (usize, usize){
        if self.arr.len() == 0 {
            return (0, 0);
        }

        let (level_begin, level_end) = self.sample_levels();

        if level_begin != 0 || level_end != 8 {
            let mut reference = self.arr[0];
            let mut differing_bits: u64 = 0;

            if self.arr[0] <= self.arr[self.arr.len()-1] {
                let mut sorted: bool = true;
                for i in 1..self.arr.len()-1 {
                    differing_bits |= reference ^ self.arr[i];
                    sorted &= reference <= self.arr[i];
                }

                if sorted {
                    return (0, 0);
                }
                differing_bits |= reference ^ self.arr[self.arr.len()-1];
            } else {
                let mut reverse_sorted: bool = true;
                for i in 1..self.arr.len()-1 {
                    differing_bits |= reference ^ self.arr[i];
                    reverse_sorted &= reference >= self.arr[i];
                }

                if reverse_sorted {
                    self.arr.reverse();
                    return (0, 0);
                }
                differing_bits |= reference ^ self.arr[self.arr.len()-1];
            }

            let lz = differing_bits.leading_zeros() as usize;
            let tz = differing_bits.trailing_zeros() as usize;
            (lz/8, 8 - tz/8)
        } else {
            (level_begin, level_end)
        }
    }

    pub fn sample_levels(&mut self) -> (usize, usize) {
        let nlogn = self.arr.len().ilog2() as usize;
        let oversampling: usize = max(1, nlogn/4);
        let buckets = min(max(1, nlogn), 256);

        let mut num_samples = oversampling*buckets;
        assert!(num_samples <= self.arr.len());


        self.select_sample(num_samples);

        let reference = self.arr[0];
        let mut differing_bits: u64 = 0;
        for i in 1..num_samples {
            let xor = reference ^ self.arr[i];
            differing_bits |= xor;
        }

        let lz = differing_bits.leading_zeros() as usize;
        let tz = differing_bits.trailing_zeros() as usize;

        (lz/8, 8 - tz/8)
    }

    pub fn select_sample(&mut self, mut num_samples: usize) {
        let len = self.arr.len();
        let mut write = 0;
        let mut rng = StdRng::seed_from_u64(12345);
        while num_samples > 0 {
            let sample = rng.gen_range(write..len);
            self.arr.swap(write, sample);
            write += 1;
            num_samples -= 1;
        }
    }

    pub fn sample_untouched(&mut self) { // FOR DEBUG ONLY
        let max = self.arr.iter().max().unwrap();
        let lz = max.leading_zeros();
        let klog2 = (K as u64).ilog2();
        let zero_blocks = (lz as f64 / klog2 as f64).floor() as u32;
        self.level = zero_blocks as usize;
        self.level_end = 8;
    }
}


impl IPS2RaSorter {
    pub fn sample(&mut self, task: &mut ExtTask) {
        let mut max = u64::MAX;
        let mut remaining = task.size;
        for i in 0..task.size / (HUGE_PAGE_SIZE_1G/8) {
            read_write_hugepage_1G(self.qpair.as_mut().unwrap(), task.start_lba + i * LBA_PER_CHUNK * CHUNKS_PER_HUGE_PAGE_1G, self.sort_buffer.as_mut().unwrap(), false);
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

