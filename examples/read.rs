use std::slice;
use vroom::HUGE_PAGE_SIZE_2M;
use vroom::memory::Dma;

pub fn main(){
    let mut nvme = vroom::init("0000:03:00.0").unwrap();

    let mut buffer: [u8; 4096] = [0; 4096];
    nvme.read_copied(&mut buffer, 0).unwrap();

    println!("read: {:?}", u8_to_u64_slice(&mut buffer));
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