pub fn insertion_sort(arr: &mut [u64]) {
    for j in 1..arr.len() {
        let mut i: usize = 0;
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

