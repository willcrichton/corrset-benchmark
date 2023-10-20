use std::time::Duration;

use corrset::{
  dispatch_inner_method, dispatch_outer_method, inner::alloc::AllocCorrSet, CorrSetInner,
  CorrSetOuter, Row,
};
use criterion::{criterion_group, criterion_main, Bencher, Criterion};

const K: usize = 5;

fn corrset_outer(c: &mut Criterion) {
  let mut g = c.benchmark_group("corrset-outer");
  g.measurement_time(Duration::from_secs(10))
    .warm_up_time(Duration::from_secs(1))
    .sample_size(10);
  let data = corrset::load_rows("data/data-large.json").unwrap();

  const NUM_COMBS: usize = 5_000_000;

  for impl_name in corrset::outer_names() {
    g.bench_function(&impl_name, |b| {
      fn run<T: CorrSetOuter>(b: &mut Bencher, data: &[Row]) {
        let outer = T::new();
        let inner = AllocCorrSet::build(&data);
        b.iter(|| {
          let combs = inner.combinations(K);
          outer.k_set(&inner, combs.take(NUM_COMBS));
        });
      }

      dispatch_outer_method!(impl_name.as_str(), run, b, &data);
    });
  }

  g.finish();
}

fn corrset_inner(c: &mut Criterion) {
  let mut g = c.benchmark_group("corrset-inner");  
  let data = corrset::load_rows("data/data-large.json").unwrap();

  for impl_name in corrset::inner_names() {
    g.bench_function(&impl_name, |b| {
      fn run<'a, T: CorrSetInner<'a>>(b: &mut Bencher, data: &'a [Row]) {
        let cs = T::build(data);
        let qs = cs.combinations(K).next().unwrap();
        let mut scratch = cs.init_scratch();
        b.iter(|| cs.corr_set(&mut scratch, &qs));
      }

      dispatch_inner_method!(impl_name.as_str(), run, b, &data);
    });
  }

  g.finish();
}

criterion_group!(benches, corrset_outer, corrset_inner);
criterion_main!(benches);
