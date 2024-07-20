use log::debug;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use crate::config::{ALPHA, K};
use crate::base_case::*;

pub fn sample(input: &mut [u32], decision_tree: &mut Vec<u32>, from: usize, to: usize) {
    let n = to;
    let num_samples = (K as f64 * ALPHA).ceil() as usize;
    //debug!("Number of samples: {}", num_samples);
    // Step 1: Sample k*alpha elements in place
    let mut rng = StdRng::seed_from_u64(12345);


    for i in 0..num_samples {
        let j = rng.gen_range(i+from..n);
        input.swap(i+from, j as usize);
    }

    insertion_sort_bound(input, from, num_samples+from);
    //debug!("Sorted Sample: {:?}", &input[..num_samples]);

    let mut splitters = vec![0; K - 1];
    for i in 1..K {
        let idx = from + (i * num_samples / K);
        splitters[i - 1] = input[idx];
    }

    //remove duplicates
    // TODO: think of equality buckets
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




