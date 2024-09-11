#![feature(thread_spawn_unchecked)]

use log::LevelFilter;
use log::{debug, info, warn, error};
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng, thread_rng};
use std::cmp::max;
use std::io;
use std::time::Instant;
use rand::seq::index::sample;
use std::error::Error;
use vroom::memory::{Dma, DmaSlice};
use vroom::QUEUE_LENGTH;

mod sampling;
mod base_case;
mod classification;
mod config;
mod permutation;
mod cleanup;
mod sort;
mod sorter;
mod sequential;
mod parallel;
mod conversion;
mod setup;

use crate::base_case::insertion_sort;
use crate::config::{HUGE_PAGES, HUGE_PAGE_SIZE};
use crate::conversion::{u64_to_u8_slice, u8_to_u64, u8_to_u64_slice};
use crate::permutation::calculate_hugepage_chunk_block;
use crate::setup::{clear, setup_array};
use crate::sort::{sort, sort_dma};
use crate::sorter::{DMATask, IPS2RaSorter, Task};

fn verify_sorted(arr: &Vec<u64>) {
    for i in 1..arr.len() {
        assert!(arr[i - 1] <= arr[i]);
    }
}

fn main() -> Result<(), Box<dyn Error>>{
    env_logger::builder()
        .filter_level(LevelFilter::Error)
        .init();

    sort_dma("0000:00:04.0", 0, false)?;

    Ok(())
}


