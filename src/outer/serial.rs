use float_ord::FloatOrd;

use itertools::Itertools;

use crate::{inner::CorrSetInner, utils, Question};

use super::CorrSetOuter;

pub struct CorrSetSerial;

impl CorrSetOuter for CorrSetSerial {
  fn new() -> Self {
    CorrSetSerial
  }

  fn k_sets<'a, T: CorrSetInner<'a>>(&self, corrset: &T, k: usize) -> Vec<Vec<&'a Question>> {
    let q_combs = utils::with_pb(
      corrset.iter_qs().count(),
      k,
      corrset.iter_qs().combinations(k),
    );
    let mut scratch = corrset.init_scratch();
    q_combs
      .filter_map(|qs| {
        let r = corrset.corr_set(&mut scratch, &qs);
        (!r.is_nan()).then_some((qs, r))
      })
      .sorted_by_key(|(_, r)| FloatOrd(*r))
      .take(super::TOP_N)
      .map(|(qs, _)| qs.into_iter().map(|q| corrset.to_question(q)).collect_vec())
      .collect_vec()
  }
}
