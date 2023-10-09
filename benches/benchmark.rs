use std::time::Duration;

use corrset::{
  dispatch_inner_method, outer::serial::CorrSetSerial, CorrSetInner, CorrSetOuter, Row,
};
use criterion::{
  criterion_group, criterion_main, measurement::WallTime, Bencher, BenchmarkGroup, BenchmarkId,
  Criterion,
};
use itertools::Itertools;

#[derive(Clone, Copy)]
enum BenchKind {
  Full,
  InnerLoop,
}

fn corrset(
  g: &mut BenchmarkGroup<'_, WallTime>,
  data: Vec<Row>,
  max_k: usize,
  min_method: usize,
  kind: BenchKind,
) {
  let mut impl_names = corrset::inner_names();
  impl_names.retain(|name| {
    let (n, _) = name.split_once("_").unwrap();
    n.parse::<usize>().unwrap() >= min_method
  });

  for k in 5..=max_k {
    for impl_name in &impl_names {
      g.bench_with_input(BenchmarkId::new(impl_name, k), &k, |b, k| {
        fn run<'a, T: CorrSetInner<'a>>(
          b: &mut Bencher,
          data: &'a [Row],
          k: usize,
          kind: BenchKind,
        ) {
          match kind {
            BenchKind::Full => {
              b.iter(|| {
                let cs = T::build(data);
                CorrSetSerial.k_sets(&cs, k);
              });
            }
            BenchKind::InnerLoop => {
              let cs = T::build(data);
              let qs = cs.iter_qs().combinations(k).next().unwrap();
              let mut scratch = cs.init_scratch();
              b.iter(|| cs.corr_set(&mut scratch, &qs));
            }
          }
        }

        dispatch_inner_method!(impl_name.as_str(), run, b, &data, *k, kind);
      });
    }
  }
}

fn mkgroup<'a>(c: &'a mut Criterion, name: &str) -> BenchmarkGroup<'a, WallTime> {
  let mut group = c.benchmark_group(name);
  group
    .measurement_time(Duration::from_secs(1))
    .warm_up_time(Duration::from_secs(1))
    .sample_size(10);
  group
}

fn corrset_full_small(c: &mut Criterion) {
  let mut group = mkgroup(c, "corrset-small");
  let data_small = corrset::load_rows("data/data-small.json").unwrap();
  corrset(&mut group, data_small, 4, 0, BenchKind::Full);
  group.finish();
}

fn corrset_full_medium(c: &mut Criterion) {
  let mut group = mkgroup(c, "corrset-medium");
  let data_small = corrset::load_rows("data/data-medium.json").unwrap();
  corrset(&mut group, data_small, 2, 3, BenchKind::Full);
  group.finish();
}

fn corrset_full_large(c: &mut Criterion) {
  let mut group = mkgroup(c, "corrset-large");
  let data_small = corrset::load_rows("data/data-large.json").unwrap();
  corrset(&mut group, data_small, 2, 5, BenchKind::Full);
  group.finish();
}

fn corrset_inner_large(c: &mut Criterion) {
  let mut group = c.benchmark_group("corrset-inner-large");
  group
    .measurement_time(Duration::from_secs(5))
    .sample_size(50);
  let data_small = corrset::load_rows("data/data-large.json").unwrap();
  corrset(&mut group, data_small, 5, 0, BenchKind::InnerLoop);
  group.finish();
}

criterion_group!(
  benches,
  corrset_full_small,
  corrset_full_medium,
  corrset_full_large,
  corrset_inner_large
);
criterion_main!(benches);
