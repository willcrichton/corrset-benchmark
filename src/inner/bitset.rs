use super::{
  imap::{QuestionMap, UserMap},
  indexed::{QuestionIdx, QuestionRef, UserRef},
  CorrSetInner,
};
use crate::{utils, utils::Captures, Question, Row};
use fxhash::FxHashSet as HashSet;
use indexical::{bitset::BitSet, pointer::ArcFamily, IndexSet, IndexedDomain};
use std::sync::Arc;

pub type QuestionEntry<'a, S> = (UserMap<'a, u32>, IndexSet<'a, UserRef<'a>, S, ArcFamily>);
pub struct BitsetCorrSet<'a, S: BitSet> {
  questions: Arc<IndexedDomain<QuestionRef<'a>>>,
  q_to_score: QuestionMap<'a, QuestionEntry<'a, S>>,
  grand_totals: UserMap<'a, u32>,
}

pub type BvecCorrSet<'a> = BitsetCorrSet<'a, indexical::bitset::bitvec::BitVec>;
pub type SimdCorrSet<'a> = BitsetCorrSet<'a, indexical::bitset::simd::SimdBitset<u64, 16>>;

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

    let mut q_to_score = QuestionMap::new(&questions, |_| {
      (
        UserMap::<'_, u32>::new(&users, |_| 0),
        IndexSet::new(&users),
      )
    });
    for r in data {
      let (q_idx, u_idx) = (
        questions.index(&QuestionRef(&r.question)),
        users.index(&UserRef(&r.user)),
      );
      let (scores, set) = q_to_score.get_mut(q_idx).unwrap();
      scores.insert(u_idx, r.score);
      set.insert(u_idx);
    }

    let grand_totals = UserMap::new(&users, |u| {
      q_to_score
        .values()
        .filter_map(|(scores, set)| set.contains(u).then_some(*scores.get(u).unwrap()))
        .sum::<u32>()
    });

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
            let (u_scores, _) = self.q_to_score.get_unchecked(*q);
            *u_scores.get_unchecked(u)
          })
          .sum::<u32>();
        let grand_total = unsafe { *self.grand_totals.get_unchecked(u) };
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
