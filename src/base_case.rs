pub fn insertion_sort(arr: &mut [u32]) {
    insertion_sort_bound(arr, 0, arr.len());
}

pub fn insertion_sort_bound(arr: &mut [u32], left: usize, right: usize) {
    for j in (left+1)..right {
        let mut i: usize = left;
        while arr[j] > arr[i]{
            i += 1;
        }
        let key = arr[j];
        for k in 0..(j-i){
            arr[j-k] = arr[j-k-1];
        }
        arr[i] = key;
    }
}

