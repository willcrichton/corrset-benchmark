use super::{
  imap::{QuestionMap, UserMap},
  indexed::{QuestionIdx, QuestionRef, UserRef},
  CorrSetInner,
};
use crate::{utils, utils::Captures, Question, Row};
use fxhash::FxHashSet as HashSet;
use indexical::{bitset::simd::SimdBitset, pointer::ArcFamily, IndexSet, IndexedDomain};
use std::sync::Arc;

pub type UserSet<'a> = IndexSet<'a, UserRef<'a>, SimdBitset<u64, 16>, ArcFamily>;
pub struct AllocCorrSet<'a> {
  pub questions: Arc<IndexedDomain<QuestionRef<'a>>>,
  pub users: Arc<IndexedDomain<UserRef<'a>>>,
  pub bitset: QuestionMap<'a, UserSet<'a>>,
  pub scores: UserMap<'a, QuestionMap<'a, u32>>,
  grand_totals: UserMap<'a, u32>,
}

impl<'a> AllocCorrSet<'a> {
  #[inline]
  pub fn corr_set_score(
    &self,
    qs_scores: &mut [f64],
    grand_scores: &mut [f64],
    users: &UserSet<'a>,
    qs: &[QuestionIdx],
  ) -> f64 {
    let mut n = 0;
    for (i, u) in users.indices().enumerate() {
      let scores = unsafe { self.scores.get_unchecked(u) };
      let total = qs
        .iter()
        .map(|q| unsafe { *scores.get_unchecked(*q) })
        .sum::<u32>();
      let grand_total = unsafe { *self.grand_totals.get_unchecked(u) };
      unsafe {
        *qs_scores.get_unchecked_mut(i) = total as f64;
        *grand_scores.get_unchecked_mut(i) = grand_total as f64;
      }
      n += 1;
    }
    utils::correlation(&qs_scores[..n], &grand_scores[..n])
  }
}

impl<'a> CorrSetInner<'a> for AllocCorrSet<'a> {
  type Q = QuestionIdx;
  type Scratch = (
    Vec<f64>,
    Vec<f64>,
    IndexSet<'a, UserRef<'a>, SimdBitset<u64, 16>, ArcFamily>,
  );

  #[inline]
  fn build(data: &'a [Row]) -> Self {
    let (users, questions): (HashSet<_>, HashSet<_>) = data
      .iter()
      .map(|row| (UserRef(&row.user), QuestionRef(&row.question)))
      .unzip();
    let users = Arc::new(IndexedDomain::from_iter(users));
    let questions = Arc::new(IndexedDomain::from_iter(questions));

    let mut bitset = QuestionMap::new(&questions, |_| IndexSet::new(&users));
    let mut scores = UserMap::new(&users, |_| QuestionMap::new(&questions, |_| 0));
    for r in data {
      let (q_idx, u_idx) = (
        questions.index(&QuestionRef(&r.question)),
        users.index(&UserRef(&r.user)),
      );
      bitset.get_mut(q_idx).unwrap().insert(u_idx);
      scores.get_mut(u_idx).unwrap().insert(q_idx, r.score);
    }

    let grand_totals = UserMap::new(&users, |u| scores.get(u).unwrap().values().sum::<u32>());

    AllocCorrSet {
      questions,
      users,
      bitset,
      scores,
      grand_totals,
    }
  }

  #[inline]
  fn iter_qs(&self) -> impl Iterator<Item = QuestionIdx> + Captures<'a> + '_ {
    self.questions.indices()
  }

  #[inline]
  fn to_question(&self, q: Self::Q) -> &'a Question {
    self.questions.value(q).0
  }

  #[inline]
  fn init_scratch(&self) -> Self::Scratch {
    (
      vec![0.; self.users.len()],
      vec![0.; self.users.len()],
      IndexSet::new(&self.users),
    )
  }

  #[inline]
  fn corr_set(&self, (qs_scores, grand_scores, users): &mut Self::Scratch, qs: &[Self::Q]) -> f64 {
    users.clone_from(&self.bitset[qs[0]]);
    for q in &qs[1..] {
      users.intersect(&self.bitset[*q]);
    }

    self.corr_set_score(qs_scores, grand_scores, users, qs)
  }
}

#[cfg(test)]
mod test {
  use super::*;
  use crate::test_inner;

  test_inner!(alloc, AllocCorrSet);
}
