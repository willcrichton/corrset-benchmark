use super::{
  indexed::{QuestionIdx, QuestionRef, UserIdx, UserRef},
  CorrSetInner,
};
use crate::{utils, utils::Captures, Question, Row};
use fxhash::FxHashSet as HashSet;
use indexical::{index_vec::IndexVec, ArcFamily, BitSet, IndexSet, IndexedDomain};
use std::sync::Arc;

pub type QuestionEntry<'a, S> = (
  IndexVec<UserIdx, u32>,
  IndexSet<'a, UserRef<'a>, S, ArcFamily>,
);
pub struct BitsetCorrSet<'a, S: BitSet> {
  questions: Arc<IndexedDomain<QuestionRef<'a>>>,
  q_to_score: IndexVec<QuestionIdx, QuestionEntry<'a, S>>,
  grand_totals: IndexVec<UserIdx, u32>,
}

pub type BvecCorrSet<'a> = BitsetCorrSet<'a, indexical::impls::BitVec>;
pub type SimdCorrSet<'a> = BitsetCorrSet<'a, indexical::impls::SimdBitset<u64, 16>>;

impl<'a, S: BitSet + Send + Sync> CorrSetInner<'a> for BitsetCorrSet<'a, S> {
  type Q = QuestionIdx;
  type Scratch = ();

  fn build(data: &'a [Row]) -> Self {
    let (users, questions): (HashSet<_>, HashSet<_>) = data
      .iter()
      .map(|row| (UserRef(&row.user), QuestionRef(&row.question)))
      .unzip();
    let users = Arc::new(IndexedDomain::from_iter(users));
    let questions = Arc::new(IndexedDomain::from_iter(questions));

    let empty_vec = IndexVec::from_iter(users.indices().map(|_| 0));
    let empty_set = IndexSet::new(&users);
    let mut q_to_score = IndexVec::from_iter(
      questions
        .indices()
        .map(|_| (empty_vec.clone(), empty_set.clone())),
    );
    for r in data {
      let (q_idx, u_idx) = (
        questions.index(&QuestionRef(&r.question)),
        users.index(&UserRef(&r.user)),
      );
      let (scores, set) = &mut q_to_score[q_idx];
      scores[u_idx] = r.score;
      set.insert(u_idx);
    }

    let grand_totals = users
      .indices()
      .map(|user| {
        q_to_score
          .iter()
          .filter_map(|(scores, set)| set.contains(user).then_some(scores[user]))
          .sum::<u32>()
      })
      .collect::<IndexVec<_, _>>();

    BitsetCorrSet {
      questions,
      q_to_score,
      grand_totals,
    }
  }

  fn iter_qs(&self) -> impl Iterator<Item = QuestionIdx> + Captures<'a> + '_ {
    self.questions.indices()
  }

  fn to_question(&self, q: Self::Q) -> &'a Question {
    self.questions.value(q).0
  }

  fn init_scratch(&self) -> Self::Scratch {}

  fn corr_set(&self, _: &mut (), qs: &[Self::Q]) -> f64 {
    let mut users = self.q_to_score[qs[0]].1.clone();
    for q in &qs[1..] {
      users.intersect(&self.q_to_score[*q].1);
    }

    let (qs_scores, grand_scores): (Vec<_>, Vec<_>) = users
      .indices()
      .map(|u| {
        let total = qs
          .iter()
          .map(|q| unsafe {
            let (u_scores, _) = self.q_to_score.raw.get_unchecked(q.index());
            *u_scores.raw.get_unchecked(u.index())
          })
          .sum::<u32>();
        let grand_total = unsafe { *self.grand_totals.raw.get_unchecked(u.index()) };
        (total as f64, grand_total as f64)
      })
      .unzip();
    utils::correlation(&qs_scores, &grand_scores)
  }
}

#[cfg(test)]
mod test {
  use super::*;
  use crate::test_inner;

  test_inner!(bitset, BvecCorrSet);
}
