use anyhow::Result;
use bchecks::BchecksCorrSet;
use bitset::BitsetCorrSet;
use fxhash::FxHashMap;
use indexed::IndexedCorrSet;
use indicatif::{ProgressBar, ProgressStyle};
use ivec::IvecCorrSet;
use naive::NaiveCorrSet;
use rayon::RayonCorrSet;
use serde::{Deserialize, Serialize};
use simd::SimdCorrSet;
use std::{fs::File, io::BufReader, path::Path};

pub mod bchecks;
pub mod bitset;
pub mod indexed;
pub mod ivec;
pub mod naive;
pub mod rayon;
pub mod simd;

#[derive(PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Debug)]
pub struct User(pub String);

#[derive(PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Debug)]
pub struct Question(pub String);

#[derive(Serialize, Deserialize, Debug)]
pub struct Row {
  pub user: User,
  pub question: Question,
  pub score: u32,
}

pub fn load_rows(path: impl AsRef<Path>) -> Result<Vec<Row>> {
  Ok(serde_json::from_reader(BufReader::new(File::open(path)?))?)
}

pub trait CorrSet {
  fn k_sets(&self, data: &[Row], k: usize) -> Vec<Vec<Question>>;
}

pub(crate) const TOP_N: usize = 10;

pub(crate) fn correlation(a: &[f64], b: &[f64]) -> f64 {
  correlation_n(a, b, a.len())
}

pub(crate) fn correlation_n(a: &[f64], b: &[f64], n: usize) -> f64 {
  rgsl::statistics::correlation(a, 1, b, 1, n)
}

pub(crate) fn pb(n: usize, k: usize) -> ProgressBar {
  pb_chunked(n, k, 1)
}

pub(crate) fn pb_chunked(n: usize, k: usize, chk: usize) -> ProgressBar {
  fn n_choose_k(n: usize, k: usize) -> usize {
    ((n - k + 1)..=n).product::<usize>() / (1..=k).product::<usize>()
  }

  ProgressBar::new((n_choose_k(n, k) / chk) as u64).with_style(
    ProgressStyle::with_template("{elapsed_precise} [{wide_bar:.cyan/blue}] {pos}/{len} {eta}")
      .unwrap()
      .progress_chars("#>-"),
  )
}

pub fn methods() -> FxHashMap<String, Box<dyn CorrSet>> {
  let mut map: FxHashMap<String, Box<dyn CorrSet>> = FxHashMap::default();
  map.insert("0_naive".into(), Box::new(NaiveCorrSet));
  map.insert("1_indexed".into(), Box::new(IndexedCorrSet));
  map.insert("2_ivec".into(), Box::new(IvecCorrSet));
  map.insert("3_bchecks".into(), Box::new(BchecksCorrSet));
  map.insert("4_bitset".into(), Box::new(BitsetCorrSet));
  map.insert("5_simd".into(), Box::new(SimdCorrSet));
  map.insert("6_rayon".into(), Box::new(RayonCorrSet));
  map
}
