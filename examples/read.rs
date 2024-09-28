use std::{env, process, slice};

pub fn main(){
    let mut args = env::args();
    args.next();

    let pci_addr = match args.next() {
        Some(arg) => arg,
        None => {
            eprintln!("Usage: cargo run --example read <pci bus id> <length?> <start_lba?>");
            process::exit(1);
        }
    };

    let len = match args.next() {
        Some(arg) => arg.parse::<usize>().unwrap(),
        None => {
            eprintln!("Usage: cargo run --example read <pci bus id> <length?> <start_lba?>\nNo length provided. Defaulting to 1024 elements");
            1024
        }
    };

    let start_lba = match args.next() {
        Some(arg) => arg.parse::<usize>().unwrap(),
        None => {
            eprintln!("Usage: cargo run --example read <pci bus id> <length?> <start_lba?>\nNo start_lba provided. Defaulting to 0");
            0
        }
    };

    let mut nvme = vroom::init(&pci_addr).unwrap();

    let mut buffer: Vec<u8> = vec![0u8; len*8];
    nvme.read_copied(&mut buffer, start_lba as u64).unwrap();

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