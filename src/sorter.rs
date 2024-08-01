use std::fmt::Display;
use crate::config::{K, BLOCKSIZE};
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
#[derive(Debug)]
pub struct IPS2RaSorter {
    pub classified_elements: usize,
    pub pointers: [(i64, i64); K],
    pub boundaries: [u64; K + 1],
    pub primary_bucket: usize,

    // local buffers
    pub blocks: Vec<Vec<u64>>,
    pub element_counts: [u64; K],
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
            blocks: vec![Vec::with_capacity(BLOCKSIZE); K],
            element_counts: [0; K],
            overflow: false,
            overflow_buffer: Vec::new(),
            parallel: false,
        })
    }

    pub fn clear(&mut self) {
        for i in 0..K {
            self.blocks[i].clear();
            self.element_counts[i] = 0;
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
            blocks: vec![Vec::new(); K],
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


//impl Display for IPS2RaSorter {
//    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//        let red = "\x1b[35m";
//        let white = "\x1b[32m";
//        let mut current: bool = true;
//        let mut sum = 0;
//        for i in 0..K {
//            let mut start = sum;
//            sum += self.element_counts[i];
//            write!(f, "{}[", { if current { red } else { white } })?;
//            while (start as i64) < (sum as i64) - 1 {
//                write!(f, "{} ", self.arr[start as usize])?;
//                start += 1;
//            }
//            if start != sum {
//                write!(f, "{}]", self.arr[start as usize])?;
//            } else {
//                write!(f, "]")?;
//            }
//            write!(f, " ")?;
//            current = !current;
//        }
//        write!(f, "\x1b[0m")?;
//        Ok(())
//    }
//}