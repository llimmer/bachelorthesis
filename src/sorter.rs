use std::fmt::Display;
use crate::config::{K, BLOCKSIZE};

#[derive(Debug)]
pub(crate) struct Sorter<'a> {
    pub arr: &'a mut [u64],
    pub decision_tree: [u64; K-1],
    pub classified_elements: usize,
    pub pointers: [(i64, i64); K],
    pub boundaries: [u64; K + 1],
    pub primary_bucket: usize,

    // TODO: see how to handle threads here
    // local buffers
    pub blocks: Vec<Vec<u64>>,
    pub element_counts: [u64; K],
    pub overflow: bool,
    pub overflow_buffer: Vec<u64>,
}

impl<'a> Sorter<'a> {
    pub fn new(arr: &mut [u64]) -> Box<Sorter> {
        Box::new(Sorter {
            arr,
            decision_tree: [0; K - 1],
            classified_elements: 0,
            pointers: [(0, 0); K],
            boundaries: [0; K + 1],
            primary_bucket: 0,
            blocks: vec![Vec::new(); K],
            element_counts: [0; K],
            overflow: false,
            overflow_buffer: Vec::new(),
        })
    }

    pub fn new_(arr: &mut [u64], decision_tree: [u64; K-1], classified_elements: usize, pointers: [(i64, i64); K], boundaries: [u64; K+1], primary_bucket: usize, blocks: Vec<Vec<u64>>, element_counts: [u64;K], overflow: bool, overflow_buffer: Vec<u64>) -> Box<Sorter> {
         Box::new(Sorter {
            arr,
            decision_tree,
            classified_elements,
            pointers,
            boundaries,
            primary_bucket,
            blocks,
            element_counts,
            overflow,
            overflow_buffer
        })
    }

}

impl Display for Sorter<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let red = "\x1b[35m";
        let white = "\x1b[32m";
        let mut current: bool = true;
        let mut sum = 0;
        for i in 0..K {
            let mut start = sum;
            sum += self.element_counts[i];
            write!(f, "{}[", {if current {red} else {white}})?;
            while (start as i64) < (sum as i64) -1 {
                write!(f, "{} ", self.arr[start as usize])?;
                start += 1;
            }
            if start != sum{
                write!(f,"{}]", self.arr[start as usize])?;
            } else {
                write!(f, "]")?;
            }
            write!(f, " ")?;
            current = !current;
        }
        write!(f, "\x1b[0m")?;
        Ok(())
    }
}