use crate::{
  utils::{self, IteratorChunkedExt},
  CorrSetInner, CorrSetOuter, Question,
};
use float_ord::FloatOrd;

use itertools::Itertools;
use rayon::prelude::*;

pub struct CorrSetChunked;

impl CorrSetOuter for CorrSetChunked {
  fn new() -> Self {
    CorrSetChunked
  }

  fn k_sets<'a, T: CorrSetInner<'a>>(&self, corrset: &T, k: usize) -> Vec<Vec<&'a Question>> {
    let q_combs = utils::with_pb(
      corrset.iter_qs().count(),
      k,
      corrset.iter_qs().combinations(k),
    )
    .chunked::<1024>();
    let q_corrs = q_combs
      .par_bridge()
      .map_init(
        || corrset.init_scratch(),
        |scratch, qs_chunk| {
          qs_chunk
            .into_iter()
            .filter_map(|qs| {
              let r = corrset.corr_set(scratch, &qs);
              (!r.is_nan()).then_some((qs, r))
            })
            .collect_vec()
        },
      )
      .reduce(Vec::new, |mut v1, v2| {
        v1.extend(v2);
        v1.sort_by_key(|(_, r)| FloatOrd(*r));
        v1.truncate(super::TOP_N);
        v1
      });

    q_corrs
      .into_iter()
      .map(|(qs, _)| qs.into_iter().map(|q| corrset.to_question(q)).collect_vec())
      .collect_vec()
  }
}
