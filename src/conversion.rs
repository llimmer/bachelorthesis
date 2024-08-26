use std::slice;

pub fn u64_to_u8_slice(arr: &mut [u64]) -> &mut [u8] {
    let len = arr.len() * size_of::<u64>();
    unsafe {
        slice::from_raw_parts_mut(arr.as_mut_ptr() as *mut u8, len)
    }
}

pub fn u8_to_u64_slice(bytes: &mut [u8]) -> &mut [u64] {
    assert_eq!(bytes.len() % 8, 0, "Buffer size must be a multiple of 8");
    assert_eq!(bytes.as_ptr().align_offset(align_of::<u64>()), 0, "Buffer is not properly aligned");

    unsafe {
        slice::from_raw_parts_mut(
            bytes.as_mut_ptr() as *mut u64,
            bytes.len() / 8,
        )
    }
}

pub fn u8_to_u64(bytes: &[u8]) -> u64 {
    assert_eq!(bytes.len(), 8, "Buffer size must be 8 bytes");

    let array: [u8; 8] = bytes.try_into().expect("slice with incorrect length");
    u64::from_le_bytes(array)
}