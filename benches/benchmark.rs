use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use rand::SeedableRng;
use bachelorthesis::{sort, sort_parallel};

fn benchmark_quicksort(c: &mut Criterion) {
    let mut data: Vec<u64> = (0..134217728/2).collect(); // Example data
    let mut rng = StdRng::seed_from_u64(12345);
    data.shuffle(&mut rng);
    c.bench_function("Quicksort 1/2 GiB", |b| {
        data.shuffle(&mut rng);
        b.iter(|| {
            black_box(data.sort_unstable());
        })
    });
}

fn benchmark_ips2ra_seq(c: &mut Criterion) {
    let mut data: Vec<u64> = (0..134217728/2).collect(); // Example data
    let mut rng = StdRng::seed_from_u64(12345);
    data.shuffle(&mut rng);
    c.bench_function("IPS2Ra 1/2 GiB", |b| {
        data.shuffle(&mut rng);
        b.iter(|| {
            sort(black_box(&mut data));
        })
    });
}

fn benchmark_ips2ra_par(c: &mut Criterion) {
    let mut data: Vec<u64> = (0..134217728/2).collect(); // Example data
    let mut rng = StdRng::seed_from_u64(12345);
    data.shuffle(&mut rng);
    c.bench_function("IPS2Ra 1/2 GiB", |b| {
        data.shuffle(&mut rng);
        b.iter(|| {
            sort_parallel(black_box(&mut data));
        })
    });
}



criterion_group!(name = benches;
    config = Criterion::default().sample_size(10);
    targets = benchmark_quicksort, benchmark_ips2ra_seq, benchmark_ips2ra_par);
criterion_main!(benches);
