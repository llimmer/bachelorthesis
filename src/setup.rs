use vroom::NvmeDevice;
use crate::config::LBA_SIZE;
use crate::conversion::u64_to_u8_slice;

pub fn setup_array(arr: &mut [u64], nvme: &mut NvmeDevice) {
    let u8_arr = u64_to_u8_slice(arr);

    // write slices of 512B to disk
    for i in 0..u8_arr.len() / (LBA_SIZE/8) {
        let slice = &u8_arr[i*(LBA_SIZE/8)..(i+1)*(LBA_SIZE/8)];
        nvme.write_copied(slice, i as u64).unwrap();
    }

    // write the last slice, fill up with zeros
    let last_slice = &u8_arr[(u8_arr.len() / (LBA_SIZE/8))*(LBA_SIZE/8)..];
    let mut last_slice_padded = [0; LBA_SIZE/8];
    last_slice_padded[..last_slice.len()].copy_from_slice(last_slice);
    nvme.write_copied(&last_slice_padded, (u8_arr.len() / (LBA_SIZE/8)) as u64).unwrap();
}

pub fn clear(till: usize, nvme: &mut NvmeDevice) {
    for i in 0..till {
        nvme.write_copied(&[0; LBA_SIZE/8], i as u64).unwrap();
    }
}