use float_ord::FloatOrd;

use itertools::Itertools;

use crate::{inner::CorrSetInner, Question};

use super::CorrSetOuter;

pub struct CorrSetSerial;

impl CorrSetOuter for CorrSetSerial {
  fn new() -> Self {
    CorrSetSerial
  }

  #[inline]
  fn k_set<'a, T: CorrSetInner<'a>>(
    &self,
    corrset: &T,
    combs: impl Iterator<Item = Vec<T::Q>> + Send,
  ) -> Vec<&'a Question> {
    let mut scratch = corrset.init_scratch();
    let (qs, _) = combs
      .filter_map(|qs| {
        let r = corrset.corr_set(&mut scratch, &qs);
        (!r.is_nan()).then_some((qs, r))
      })
      .max_by_key(|(_, r)| FloatOrd(*r))
      .unwrap();
    qs.into_iter().map(|q| corrset.to_question(q)).collect_vec()
  }
}
