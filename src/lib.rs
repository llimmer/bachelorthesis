#![feature(thread_spawn_unchecked)]

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

pub use sort::sort;
pub use base_case::insertion_sort;