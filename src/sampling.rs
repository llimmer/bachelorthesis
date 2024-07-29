use log::debug;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use crate::config::{ALPHA, K};
use crate::base_case::*;
use crate::sorter::{Sorter};


impl<'a> Sorter<'a> {
    pub fn sample(&mut self) {
        let n = self.arr.len();
        let num_samples = (K as f64 * ALPHA).ceil() as usize;
        //debug!("Number of samples: {}", num_samples);
        // Step 1: Sample k*alpha elements in place
        let mut rng = StdRng::seed_from_u64(12345);


        for i in 0..num_samples {
            let j = rng.gen_range(i ..n);
            self.arr.swap(i, j as usize);
        }

        insertion_sort(&mut self.arr[0..num_samples]);
        //debug!("Sorted Sample: {:?}", &input[..num_samples]);

        let mut splitters = vec![0; K - 1];
        for i in 1..K {
            let idx = (i * num_samples / K);
            splitters[i - 1] = self.arr[idx];
        }

        //remove duplicates
        splitters.dedup();
        let num_unique_splitters = splitters.len();


        create_decision_tree(&mut self.decision_tree, &splitters);
    }
}


fn create_decision_tree(tree: &mut [u64; K-1], splitters: &[u64]) {

    // TODO: think of equality buckets

    let len = splitters.len();
    if len == 0 {
        return;
    }

    let mut indices = vec![(0, len, 0)]; // (start, end, index in tree)

    while let Some((start, end, index)) = indices.pop() {
        if start >= end || index >= len {
            continue;
        }

        let mid = (start + end) / 2;
        tree[index] = splitters[mid];

        // Push right child first, then left one (left one gets processed first)
        indices.push((mid + 1, end, 2 * index + 2));
        indices.push((start, mid, 2 * index + 1));
    }
}




