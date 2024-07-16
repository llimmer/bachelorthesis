use log::debug;
use rand::{Rng, thread_rng};

use crate::config::{ALPHA, K};
use crate::base_case::*;

pub fn sample(input: &mut [u32], decision_tree: &mut Vec<u32>) {
    let n = input.len();
    let num_samples = (K as f64 * ALPHA).ceil() as usize;
    //debug!("Number of samples: {}", num_samples);
    // Step 1: Sample k*alpha elements in place
    let mut rng = thread_rng();

    for i in 0..num_samples {
        let j = rng.gen_range(i..n);
        input.swap(i, j);
    }

    insertion_sort_bound(input, 0, num_samples);
    input[..num_samples].sort_unstable();
    //debug!("Sorted Sample: {:?}", &input[..num_samples]);

    let mut splitters = vec![0; K - 1];
    for i in 1..K {
        let idx = i * num_samples / K;
        splitters[i - 1] = input[idx];
    }

    splitters.dedup();
    let num_unique_splitters = splitters.len();

    // TODO: do better
    for i in 0..num_unique_splitters {
        decision_tree.push(0);
    }
    create_decision_tree(decision_tree, &splitters);
}

fn create_decision_tree(tree: &mut [u32], splitters: &[u32]) {

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




