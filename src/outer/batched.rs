use crate::{utils::IteratorBatchedExt, CorrSetInner, CorrSetOuter, Question};
use float_ord::FloatOrd;

use itertools::Itertools;
use rayon::prelude::*;

pub struct CorrSetBatched;

impl CorrSetOuter for CorrSetBatched {
  fn new() -> Self {
    CorrSetBatched
  }

  #[inline]
  fn k_set<'a, T: CorrSetInner<'a>>(
    &self,
    corrset: &T,
    combs: impl Iterator<Item = Vec<T::Q>> + Send,
  ) -> Vec<&'a Question> {
    combs
      .batched::<1024>()
      .par_bridge()
      .map_init(
        || corrset.init_scratch(),
        |scratch, qs_batch| {
          qs_batch
            .into_iter()
            .filter_map(|qs| {
              let r = corrset.corr_set(scratch, &qs);
              (!r.is_nan()).then_some((qs, r))
            })
            .collect_vec()
        },
      )
      .flatten()
      .max_by_key(|(_, r)| FloatOrd(*r))
      .unwrap()
      .0
      .into_iter()
      .map(|q| corrset.to_question(q))
      .collect_vec()
  }
}
