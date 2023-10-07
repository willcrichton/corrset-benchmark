use std::time::Duration;

use corrset::Row;
// use corrset::CorrSet;
use criterion::{
  criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, BenchmarkId, Criterion, SamplingMode,
};
use itertools::Itertools;

fn corrset(g: &mut BenchmarkGroup<'_, WallTime>, data: Vec<Row>, max_k: usize, min_method: usize) {
  let methods = corrset::methods();
  let method_names = methods
    .keys()
    .sorted()
    .filter(|name| {
      let (n, _) = name.split_once("_").unwrap();
      n.parse::<usize>().unwrap() >= min_method
    })
    .collect_vec();
  for k in 1..=max_k {
    for method in &method_names {
      g.bench_with_input(BenchmarkId::new(*method, k), &k, |b, k| {
        b.iter(|| methods[*method].k_sets(&data, *k))
      });
    }
  }
}

fn corrset_small(c: &mut Criterion) {
  let mut group = c.benchmark_group("corrset-small");
  group
    .measurement_time(Duration::from_secs(5))
    .sample_size(10);

  let data_small = corrset::load_rows("data/data-small.json").unwrap();
  corrset(&mut group, data_small, 4, 0);

  group.finish();
}

fn corrset_medium(c: &mut Criterion) {
  let mut group = c.benchmark_group("corrset-medium");
  group
    .measurement_time(Duration::from_secs(10))
    .sample_size(10)
    .sampling_mode(SamplingMode::Flat);

  let data_small = corrset::load_rows("data/data-medium.json").unwrap();
  corrset(&mut group, data_small, 2, 3);

  group.finish();
}

fn corrset_large(c: &mut Criterion) {
  let mut group = c.benchmark_group("corrset-large");
  group
    .measurement_time(Duration::from_secs(10))
    .sample_size(10)
    .sampling_mode(SamplingMode::Flat);

  let data_small = corrset::load_rows("data/data-large.json").unwrap();
  corrset(&mut group, data_small, 2, 5);

  group.finish();
}

criterion_group!(benches, corrset_small, corrset_medium, corrset_large);
criterion_main!(benches);
