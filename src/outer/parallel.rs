use crate::{CorrSetInner, CorrSetOuter, Question};
use float_ord::FloatOrd;

use itertools::Itertools;
use rayon::prelude::*;

pub struct CorrSetParallel;

impl CorrSetOuter for CorrSetParallel {
  fn new() -> Self {
    CorrSetParallel
  }

  #[inline]
  fn k_set<'a, T: CorrSetInner<'a>>(
    &self,
    corrset: &T,
    combs: impl Iterator<Item = Vec<T::Q>> + Send,
  ) -> Vec<&'a Question> {
    let (qs, r) = combs
      .par_bridge()
      .map_init(
        || corrset.init_scratch(),
        |scratch, qs: Vec<T::Q>| {
          let r = corrset.corr_set(scratch, &qs);
          (!r.is_nan()).then_some((qs, r))
        },
      )
      .filter_map(|x| x)
      .max_by_key(|(_, r)| FloatOrd(*r))
      .unwrap();
    println!("r={r:?}");
    qs.into_iter().map(|q| corrset.to_question(q)).collect_vec()
  }
}
