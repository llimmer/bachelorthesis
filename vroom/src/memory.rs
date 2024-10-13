use lazy_static::lazy_static;
use std::slice;
// use std::rc::Rc;
// use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::io::{self, Read, Seek};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::{fs, mem, process, ptr};
use std::ops::{Deref, DerefMut, Index, IndexMut, Range, RangeTo, RangeFull};

// from https://www.kernel.org/doc/Documentation/x86/x86_64/mm.txt
const X86_VA_WIDTH: u8 = 47;

const HUGE_PAGE_BITS_2M: u32 = 21;
pub const HUGE_PAGE_SIZE_2M: usize = 1 << HUGE_PAGE_BITS_2M;

const HUGE_PAGE_BITS_1G: u32 = 30;
pub const HUGE_PAGE_SIZE_1G: usize = 1 << HUGE_PAGE_BITS_1G;

pub const IOVA_WIDTH: u8 = X86_VA_WIDTH;

static HUGEPAGE_ID: AtomicUsize = AtomicUsize::new(0);

pub(crate) static mut VFIO_CONTAINER_FILE_DESCRIPTOR: Option<RawFd> = None;

lazy_static! {
    pub(crate) static ref VFIO_GROUP_FILE_DESCRIPTORS: Mutex<HashMap<i32, RawFd>> =
        Mutex::new(HashMap::new());
}

pub struct Dma<T> {
    pub virt: *mut T,
    pub phys: usize,
    pub size: usize,
}

// should be safe
impl<T> Deref for Dma<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe {
            &*self.virt
        }
    }
}

impl<T> DerefMut for Dma<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            &mut *self.virt
        }
    }
}

// Trait for types that can be viewed as DMA slices
pub trait DmaSlice {
    type Item;

    fn chunks(&self, bytes: usize) -> DmaChunks<u8>;
    fn slice(&self, range: Range<usize>) -> Self::Item;
}

// mildly overengineered lol
pub struct DmaChunks<'a, T> {
    current_offset: usize,
    chunk_size: usize,
    dma: &'a Dma<T>,
}

impl<'a, T> Iterator for DmaChunks<'a, T> {
    type Item = DmaChunk<'a, T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_offset >= self.dma.size {
            None
        } else {
            let chunk_phys_addr = self.dma.phys + self.current_offset * std::mem::size_of::<T>();
            let offset_ptr = unsafe { self.dma.virt.add(self.current_offset) };
            let len = std::cmp::min(self.chunk_size, (self.dma.size - self.current_offset) / std::mem::size_of::<T>());

            self.current_offset += len;

            Some(DmaChunk {
                phys_addr: chunk_phys_addr,
                slice: unsafe { std::slice::from_raw_parts_mut(offset_ptr, len) },
            })
        }
    }
}

// Represents a chunk obtained from a Dma<T>, with physical address and slice.
pub struct DmaChunk<'a, T> {
    pub phys_addr: usize,
    pub slice: &'a mut [T],
}

impl DmaSlice for Dma<u8> {
    type Item = Dma<u8>;
    fn chunks(&self, bytes: usize) -> DmaChunks<u8> {
        DmaChunks {
            current_offset: 0,
            chunk_size: bytes,
            dma: self,
        }
    }

    fn slice(&self, index: Range<usize>) -> Self::Item {
        assert!(index.end <= self.size, "Index out of bounds: Index end: {} must be <= Size: {}", index.end, self.size);

        unsafe {
            Dma {
                virt: self.virt.add(index.start),
                phys: self.phys + index.start,
                size: (index.end - index.start)
            }
        }

    }
}

impl Index<Range<usize>> for Dma<u8> {
    type Output = [u8];

    fn index(&self, index: Range<usize>) -> &Self::Output {
        assert!(index.end <= self.size, "Index out of bounds");

        unsafe {
            slice::from_raw_parts(self.virt.add(index.start), index.end - index.start)
        }
    }
}

impl IndexMut<Range<usize>> for Dma<u8> {
    fn index_mut(&mut self, index: Range<usize>) -> &mut Self::Output {
        assert!(index.end <= self.size, "Index out of bounds");
        unsafe {
            slice::from_raw_parts_mut(self.virt.add(index.start), index.end - index.start)
        }
    }
}

impl Index<RangeTo<usize>> for Dma<u8> {
    type Output = [u8];

    fn index(&self, index: RangeTo<usize>) -> &Self::Output {
        &self[0..index.end]
    }
}

impl IndexMut<RangeTo<usize>> for Dma<u8> {
    fn index_mut(&mut self, index: RangeTo<usize>) -> &mut Self::Output {
        &mut self[0..index.end]
    }
}

impl Index<RangeFull> for Dma<u8> {
    type Output = [u8];

    fn index(&self, _: RangeFull) -> &Self::Output {
        &self[0..self.size]
    }
}

impl IndexMut<RangeFull> for Dma<u8> {
    fn index_mut(&mut self, _: RangeFull) -> &mut Self::Output {
        let len = self.size;
        &mut self[0..len]

    }
}

impl<T> Dma<T> {
    pub fn allocate(size: usize) -> Result<Dma<T>, Box<dyn std::error::Error>> {
        // Choose the page size based on the requested size
        let (huge_page_size, page_size_str) = if size <= HUGE_PAGE_SIZE_2M {
            (HUGE_PAGE_SIZE_2M, "2M")
        } else {
            (HUGE_PAGE_SIZE_1G, "1G")
        };

        let size = if size % huge_page_size != 0 {
            ((size >> huge_page_size.trailing_zeros()) + 1) << huge_page_size.trailing_zeros()
        } else {
            size
        };

        //println!("Allocating DMA memory of size: {} (input: {}) with page size: {}", size, size, page_size_str);

        let id = HUGEPAGE_ID.fetch_add(1, Ordering::SeqCst);
        // Path for 1 GiB huge pages
        let path = {
            if size <= HUGE_PAGE_SIZE_2M {
                format!("/mnt/huge2M/nvme-{}-{}", process::id(), id)
            } else {
                format!("/mnt/huge1G/nvme-{}-{}", process::id(), id)
            }
        };


        // Create the file with the correct size
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        // Set the file size to the allocated size
        file.set_len(size as u64)?;

        let fd = file.as_raw_fd();
        let ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_HUGETLB | match huge_page_size {
                    HUGE_PAGE_SIZE_2M => libc::MAP_HUGETLB,
                    HUGE_PAGE_SIZE_1G => libc::MAP_HUGETLB,
                    _ => 0,
                },
                fd,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err("failed to mmap huge page - are huge pages enabled and free?".into());
        }

        // Lock the memory
        if unsafe { libc::mlock(ptr, size) } != 0 {
            return Err("failed to memory lock huge page".into());
        }

        Ok(Dma {
            virt: ptr as *mut T,
            phys: virt_to_phys(ptr as usize)?, // Implement this function as needed
            size,
        })
    }

    pub fn free(&self) -> Result<(), Box<dyn Error>> {
        unsafe {
            if libc::munmap(self.virt as *mut libc::c_void, self.size) != 0 {
                return Err("failed to munmap huge page".into());
            }
        }

        Ok(())
    }
}

/// Translates a virtual address to its physical counterpart
pub(crate) fn virt_to_phys(addr: usize) -> Result<usize, Box<dyn Error>> {
    let pagesize = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;

    let mut file = fs::OpenOptions::new()
        .read(true)
        .open("/proc/self/pagemap")?;

    file.seek(io::SeekFrom::Start(
        (addr / pagesize * mem::size_of::<usize>()) as u64,
    ))?;

    let mut buffer = [0; mem::size_of::<usize>()];
    file.read_exact(&mut buffer)?;

    let phys = unsafe { mem::transmute::<[u8; mem::size_of::<usize>()], usize>(buffer) };
    Ok((phys & 0x007F_FFFF_FFFF_FFFF) * pagesize + addr % pagesize)
}

#[allow(unused)]
pub fn vfio_enabled() -> bool {
    unsafe { VFIO_CONTAINER_FILE_DESCRIPTOR.is_some() }
}
