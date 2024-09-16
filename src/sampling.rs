use crate::config::{K};
use crate::sorter::{IPS2RaSorter, Task};

impl <'a> Task<'_>{
    pub fn sample(&mut self) {
        // TODO: implement correctly
        let max = self.arr.iter().max().unwrap();
        let lz = max.leading_zeros();
        let klog2 = (K as u64).ilog2();
        let zero_blocks = (lz as f64 /klog2 as f64).floor() as u32;
        self.level = zero_blocks as usize;
    }
}

