use arrayvec::ArrayVec;
use fxhash::FxHashMap as HashMap;
use std::hash::Hash;

pub fn correlation(a: &[f64], b: &[f64]) -> f64 {
  let n = a.len();
  let mean_a = a[..n].iter().sum::<f64>() / (n as f64);
  let mean_b = b[..n].iter().sum::<f64>() / (n as f64);
  let numer = a
    .iter()
    .zip(b.iter())
    .map(|(a_i, b_i)| (a_i - mean_a) * (b_i - mean_b))
    .sum::<f64>();
  let a_var = a
    .iter()
    .map(|a_i| (a_i - mean_a).powi(2))
    .sum::<f64>()
    .sqrt();
  let b_var = b
    .iter()
    .map(|b_i| (b_i - mean_b).powi(2))
    .sum::<f64>()
    .sqrt();
  let denom = a_var * b_var;
  numer / denom
}

#[allow(unused)]
pub fn with_pb<I>(n: usize, k: usize, it: impl Iterator<Item = I>) -> impl Iterator<Item = I> {
  #[cfg(feature = "progress")]
  {
    use indicatif::{ProgressBar, ProgressIterator, ProgressStyle};

    fn n_choose_k(n: usize, k: usize) -> usize {
      ((n - k + 1)..=n).product::<usize>() / (1..=k).product::<usize>()
    }

    let pb = ProgressBar::new(n_choose_k(n, k) as u64).with_style(
      ProgressStyle::with_template("{elapsed_precise} [{wide_bar:.cyan/blue}] {pos}/{len} {eta}")
        .unwrap()
        .progress_chars("#>-"),
    );

    it.progress_with(pb)
  }

  #[cfg(not(feature = "progress"))]
  {
    it
  }
}
pub fn group_by<K1: Eq + Hash, K2: Eq + Hash, V>(
  kvs: impl IntoIterator<Item = (K1, K2, V)>,
) -> HashMap<K1, HashMap<K2, V>> {
  let mut map = HashMap::default();
  for (k1, k2, v) in kvs.into_iter() {
    map.entry(k1).or_insert_with(HashMap::default).insert(k2, v);
  }
  map
}

pub trait Captures<'a> {}
impl<'a, T: ?Sized> Captures<'a> for T {}

pub struct Chunked<const N: usize, I: Iterator> {
  iter: I,
}

pub trait IteratorChunkedExt: Sized + Iterator {
  fn chunked<const N: usize>(self) -> Chunked<N, Self>;
}

impl<I: Sized + Iterator> IteratorChunkedExt for I {
  fn chunked<const N: usize>(self) -> Chunked<N, Self> {
    Chunked { iter: self }
  }
}

impl<const N: usize, I: Iterator> Iterator for Chunked<N, I> {
  type Item = ArrayVec<I::Item, N>;

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    let chunk = ArrayVec::from_iter((&mut self.iter).take(N));
    (!chunk.is_empty()).then_some(chunk)
  }
}
