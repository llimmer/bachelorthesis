pub fn insertion_sort2(arr: &mut [u64]) {
    // TODO: enhance performance
    arr.sort_unstable();
}

pub fn insertion_sort(arr: &mut [u64]) {
    for j in 1..arr.len() {
        let mut i: usize = 0;
        while arr[j] > arr[i] {
            i += 1;
        }
        let key = arr[j];
        for k in 0..(j - i) {
            arr[j - k] = arr[j - k - 1];
        }
        arr[i] = key;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insertion_sort_small() {
        let mut arr = vec![3, 2, 1, 8, 6, 0, 7, 5, 9, 4];
        insertion_sort(&mut arr);
        assert_eq!(arr, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_insertion_big() {
        let mut arr = (0..1000).collect::<Vec<u64>>();
        arr.reverse();
        insertion_sort(&mut arr);
        assert_eq!(arr, (0..1000).collect::<Vec<u64>>());
    }
}

