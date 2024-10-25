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
mod parallel_sort_merge;
mod rolling_sort;
mod sequential_sort_merge;

pub use sort::*;
pub use parallel_sort_merge::{benchmark_parallel_sort_merge};
pub use base_case::insertion_sort;
pub use setup::{clear_chunks, setup_array};
pub use config::*;
pub use conversion::*;