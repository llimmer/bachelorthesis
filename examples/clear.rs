use bachelorthesis::{clear_chunks, CHUNKS_PER_HUGE_PAGE_1G};

pub fn main(){
    let mut nvme = vroom::init("0000:03:00.0").unwrap();
    let mut qpair = nvme.create_io_queue_pair(vroom::QUEUE_LENGTH).unwrap();
    clear_chunks(CHUNKS_PER_HUGE_PAGE_1G*9, &mut qpair);
    println!("Cleared 9 hugepages");
}