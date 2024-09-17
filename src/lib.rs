#![feature(thread_spawn_unchecked)]
extern crate core;

pub mod sort;
pub mod base_case;
mod classification;
mod cleanup;
mod config;
mod permutation;
mod sampling;
mod sorter;
mod sequential;
mod parallel;
mod conversion;
mod setup;
mod merge;

pub use sort::*;
pub use base_case::insertion_sort;
pub use setup::{clear_chunks, setup_array};
pub use config::*;