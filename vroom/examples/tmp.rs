use std::fs::OpenOptions;
use std::os::fd::AsRawFd;
use std::ptr;
use libc;

fn main() {
    let path = "/mnt/huge/test_hugepage";
    let size = 1 << 30; // 1GiB

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
        .expect("Failed to open hugepage file");

    file.set_len(size as u64).expect("Failed to set file size");

    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_HUGETLB,
            file.as_raw_fd(),
            0,
        )
    };

    if ptr == libc::MAP_FAILED {
        eprintln!("Failed to mmap huge page");
    } else {
        println!("Successfully mapped huge page");
    }
}
