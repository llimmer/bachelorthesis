use crate::config::*;
use vroom::memory::Dma;
use vroom::{NvmeQueuePair};
use std::fmt;
use std::fmt::{Debug, Display};
#[derive(Debug)]
pub struct Task<'a> {
    pub arr: &'a mut [u64],
    pub level: usize,
    pub level_end: usize,
}

impl Task<'_> {
    pub fn new(arr: &mut [u64], level: usize, level_end: usize) -> Task { // TODO: check level start + end
        Task {
            arr,
            level,
            level_end,
        }
    }
    pub fn is_base_case(&self) -> bool {
        self.arr.len() <= THRESHOLD
    }

    pub fn generate_subtasks(&mut self, element_counts: &[u64; K]) -> Vec<Task> {
        let mut res = Vec::with_capacity(K);
        let (first, mut rest) = self.arr.split_at_mut(element_counts[0] as usize);
        if first.len() > 1 {
            res.push(Task::new(first, self.level + 1, self.level_end));
        }
        for i in 1..K {
            let (left, right) = rest.split_at_mut(element_counts[i] as usize);
            rest = right;
            if left.len() > 1 {
                res.push(Task::new(left, self.level + 1, self.level_end));
            }

        }
        res
    }
}



pub struct ExtTask<> {
    pub start_lba: usize,
    pub offset: usize,
    pub size:  usize,
    pub level: usize,
    pub level_end: usize,
}

impl ExtTask {
    pub fn new(start_lba: usize, offset: usize, size: usize, level: usize, level_end: usize) -> ExtTask {
        ExtTask {
            start_lba,
            offset,
            size,
            level,
            level_end,
        }
    }
}



pub struct IPS2RaSorter {
    pub block_counts: [usize; K],
    pub element_counts: [u64; K],

    pub classified_elements: usize,
    pub pointers: [(i64, i64); K],
    pub boundaries: [u64; K + 1],
    pub primary_bucket: usize,

    // local buffers
    pub blocks: [[u64; BLOCKSIZE]; K],
    pub overflow: bool,
    pub overflow_buffer: [u64; BLOCKSIZE],
    pub swap_buffer: [[u64; BLOCKSIZE]; 2],

    pub parallel: bool,

    // DMA
    pub qpair: Option<NvmeQueuePair>,
    pub buffers: Option<Vec<Dma<u8>>>,
    pub sort_buffer: Option<Dma<u8>>

}
impl IPS2RaSorter {
    pub fn new_sequential() -> Box<IPS2RaSorter> {
        Box::new(IPS2RaSorter {
            classified_elements: 0,
            pointers: [(0, 0); K],
            boundaries: [0; K + 1],
            primary_bucket: 0,
            blocks: [[0; BLOCKSIZE]; K],
            block_counts: [0; K],
            element_counts: [0; K],
            overflow: false,
            overflow_buffer: [0; BLOCKSIZE],
            swap_buffer: [[0u64; BLOCKSIZE]; 2],
            parallel: false,
            qpair: None,
            buffers: None,
            sort_buffer: None,
        })
    }

    pub fn clear(&mut self) {
        for i in self.block_counts.iter_mut() {
            *i = 0;
        }
        for i in self.element_counts.iter_mut() {
            *i = 0;
        }
        for i in self.overflow_buffer.iter_mut(){
            *i = 0;
        }
        self.primary_bucket = 0;
        self.overflow = false;
    }

    pub fn new_parallel() -> Box<Self> {
        println!("Creating new parallel sorter");
        //read line from stdin
        //let mut line = String::new();
        //std::io::stdin().read_line(&mut line).unwrap();
        Box::new(Self {
            classified_elements: 0,
            pointers: [(0, 0); K],
            boundaries: [0; K + 1],
            primary_bucket: 0,
            blocks: [[0; BLOCKSIZE]; K],
            block_counts: [0; K],
            element_counts: [0; K],
            overflow: false,
            overflow_buffer: [0; BLOCKSIZE],
            swap_buffer: [[0u64; BLOCKSIZE]; 2],
            parallel: true,
            qpair: None,
            buffers: None,
            sort_buffer: None,
        })
    }

    pub fn new_ext_sequential(qpair: NvmeQueuePair, buffers: Vec<Dma<u8>>, sort_buffer: Dma<u8>) -> Box<Self> {
        Box::new(Self {
            classified_elements: 0,
            pointers: [(0, 0); K],
            boundaries: [0; K + 1],
            primary_bucket: 0,
            blocks: [[0; BLOCKSIZE]; K],
            block_counts: [0; K],
            element_counts: [0; K],
            overflow: false,
            overflow_buffer: [0; BLOCKSIZE],
            swap_buffer: [[0u64; BLOCKSIZE]; 2],
            parallel: false,
            qpair: Some(qpair),
            buffers: Some(buffers),
            sort_buffer: Some(sort_buffer),
        })
    }

    pub fn to_string(&self, task: &Task) -> String {
        let mut res: String = String::new();
        let red = "\x1b[35m";
        let white = "\x1b[32m";
        let mut current: bool = true;
        let mut sum = 0;
        for i in 0..K {
            let mut start = sum;
            sum += self.element_counts[i];
            res.push_str(&format!("{}[", { if current { red } else { white } }));
            while (start as i64) < (sum as i64) - 1 {
                res.push_str(&format!("{} ", task.arr[start as usize]));
                start += 1;
            }
            if start != sum {
                res.push_str(&format!("{}]", task.arr[start as usize]));
            } else {
                res.push_str("]");
            }
            res.push_str(" ");
            current = !current;
        }
        res.push_str("\x1b[0m");
        res
    }
}

impl Debug for IPS2RaSorter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IPS2RaSorter:\n  \
            classified_elements: {}\n  \
            pointers: {:?}\n\
            boundaries: {:?}\n\
            primary_bucket: {}\n\
            blocks: {:?}\n\
            block_counts: {:?}\n\
            element_counts: {:?}\n\
            overflow: {}\n\
            overflow_buffer: {:?}\n\
            parallel: {}\n\
            external: {:?}",
           self.classified_elements,
           self.pointers,
           self.boundaries,
           self.primary_bucket,
           self.blocks,
           self.block_counts,
           self.element_counts,
           self.overflow,
           self.overflow_buffer,
           self.parallel,
           { if self.qpair.is_some() {
                "True"
           } else {
                "False"
           }})
    }
}
