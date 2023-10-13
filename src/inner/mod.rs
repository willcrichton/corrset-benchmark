use crate::{utils, Question, Row};
use itertools::Itertools;

pub mod alloc;
pub mod bchecks;
pub mod bitset;
pub mod indexed;
pub mod ivec;
pub mod naive;
#[cfg(test)]
mod test_utils;

pub trait CorrSetInner<'a>: Send + Sync + Sized {
  type Q: Send + Clone;
  type Scratch;
  fn build(data: &'a [Row]) -> Self;
  fn iter_qs(&self) -> impl Iterator<Item = Self::Q> + Send + '_;
  fn to_question(&self, q: Self::Q) -> &'a Question;
  fn init_scratch(&self) -> Self::Scratch;
  fn corr_set(&self, scratch: &mut Self::Scratch, qs: &[Self::Q]) -> f64;
  fn combinations<'b>(
    &'b self,
    k: usize,
  ) -> impl Iterator<Item = Vec<<Self as CorrSetInner<'a>>::Q>> + Send + 'b
  where
    'a: 'b,
  {
    utils::with_pb(self.iter_qs().count(), k, self.iter_qs().combinations(k))
  }
}

#[macro_export]
macro_rules! dispatch_inner_method {
  ($key:expr, $f:ident, $($arg:expr),*) => {{
    match $key {
      "0_naive" => $f::<$crate::inner::naive::NaiveCorrSet>($($arg),*),
      "1_indexed" => $f::<$crate::inner::indexed::IndexedCorrSet>($($arg),*),
      "2_ivec" => $f::<$crate::inner::ivec::IvecCorrSet>($($arg),*),
      "3_bchecks" => $f::<$crate::inner::bchecks::BchecksCorrSet>($($arg),*),
      "4_bitset" => $f::<$crate::inner::bitset::BvecCorrSet>($($arg),*),
      "5_simd" => $f::<$crate::inner::bitset::SimdCorrSet>($($arg),*),
      "6_alloc" => $f::<$crate::inner::alloc::AllocCorrSet>($($arg),*),
      k => unimplemented!("{k}"),
    }
  }};
}

pub fn inner_names() -> Vec<String> {
  vec![
    "0_naive".into(),
    "1_indexed".into(),
    "2_ivec".into(),
    "3_bchecks".into(),
    "4_bitset".into(),
    "5_simd".into(),
    "6_alloc".into(),
  ]
}
