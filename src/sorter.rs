use std::fmt;
use std::fmt::Display;
use vroom::memory::Dma;
use vroom::NvmeDevice;
use crate::config::{K, BLOCKSIZE, DMA_BUFFERS, LBA_SIZE};
pub struct Task<'a> {
    pub arr: &'a mut [u64],
    pub level: usize,
}

impl Task<'_> {
    pub fn new(arr: &mut [u64], level: usize) -> Task {
        Task {
            arr,
            level,
        }
    }
}

pub struct DMATask<> {
    pub start_lba: usize,
    pub offset: usize,
    pub size:  usize,
    pub level: usize,
}

impl DMATask {
    pub fn new(start_lba: usize, offset: usize, size: usize, level: usize) -> DMATask {
        DMATask {
            start_lba,
            offset,
            size,
            level,
        }
    }
}



#[derive(Debug)]
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
    pub overflow_buffer: Vec<u64>,

    pub parallel: bool,
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
            overflow_buffer: Vec::new(),
            parallel: false,
        })
    }

    pub fn clear(&mut self) {
        for i in self.block_counts.iter_mut() {
            *i = 0;
        }
        for i in self.element_counts.iter_mut() {
            *i = 0;
        }
        self.primary_bucket = 0;
        self.overflow = false;
        self.overflow_buffer.clear();
    }

    pub fn new_parallel() -> Box<Self> {
        Box::new(Self {
            classified_elements: 0,
            pointers: [(0, 0); K],
            boundaries: [0; K + 1],
            primary_bucket: 0,
            blocks: [[0; BLOCKSIZE]; K],
            block_counts: [0; K],
            element_counts: [0; K],
            overflow: false,
            overflow_buffer: Vec::new(),
            parallel: true,
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

// DMA:
pub struct IPS2RaSorterDMA {
    pub block_counts: [usize; K],
    pub element_counts: [u64; K],

    pub classified_elements: usize,
    pub pointers: [(i64, i64); K],
    pub boundaries: [u64; K + 1],
    pub primary_bucket: usize,

    // local buffers
    pub blocks: [[u64; BLOCKSIZE]; K],
    pub overflow: bool,
    pub overflow_buffer: Vec<u64>,

    // DMA:
    pub nvme: NvmeDevice,
    pub dma_blocks: Vec<Dma<u8>>,

    pub parallel: bool,
}

impl IPS2RaSorterDMA {
    pub fn new(nvme: NvmeDevice, parallel: bool) -> Box<Self> {
        let mut dmas: Vec<Dma<u8>> = vec![];
        for _ in 0..DMA_BUFFERS {
            dmas.push(Dma::allocate(LBA_SIZE).unwrap());
        }

        Box::new( Self {
            classified_elements: 0,
            pointers: [(0, 0); K],
            boundaries: [0; K + 1],
            primary_bucket: 0,
            blocks: [[0; BLOCKSIZE]; K],
            block_counts: [0; K],
            element_counts: [0; K],
            overflow: false,
            overflow_buffer: Vec::new(),
            nvme,
            dma_blocks: dmas,
            parallel,
        })
    }

    pub fn new_sequential(nvme_device: NvmeDevice) -> Box<Self> {
        Self::new(nvme_device, false)
    }

    pub fn new_parallel(nvme_device: NvmeDevice) -> Box<Self> {
        Self::new(nvme_device, true)
    }

    pub fn clear(&mut self) {
        for i in self.block_counts.iter_mut() {
            *i = 0;
        }
        for i in self.element_counts.iter_mut() {
            *i = 0;
        }
        self.primary_bucket = 0;
        self.overflow = false;
        self.overflow_buffer.clear();
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

impl fmt::Debug for IPS2RaSorterDMA {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IPS2RaSorterDMA")
            .field("classified_elements", &self.classified_elements)
            .field("pointers", &self.pointers)
            .field("boundaries", &self.boundaries)
            .field("primary_bucket", &self.primary_bucket)
            .field("blocks", &self.blocks)
            .field("block_counts", &self.block_counts)
            .field("element_counts", &self.element_counts)
            .field("overflow", &self.overflow)
            .field("overflow_buffer", &self.overflow_buffer)
            .field("parallel", &self.parallel)
            .finish()
    }
}