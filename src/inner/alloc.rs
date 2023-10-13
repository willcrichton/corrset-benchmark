use super::{
  indexed::{QuestionIdx, QuestionRef, UserRef},
  ivec::{QuestionMap, UserMap},
  CorrSetInner,
};
use crate::{utils, utils::Captures, Question, Row};
use fxhash::FxHashSet as HashSet;
use indexical::{bitset::simd::SimdBitset, pointer::ArcFamily, IndexSet, IndexedDomain};
use std::sync::Arc;

pub type QuestionEntry<'a> = (
  UserMap<'a, u32>,
  IndexSet<'a, UserRef<'a>, SimdBitset<u64, 16>, ArcFamily>,
);
pub struct AllocCorrSet<'a> {
  questions: Arc<IndexedDomain<QuestionRef<'a>>>,
  users: Arc<IndexedDomain<UserRef<'a>>>,
  q_to_score: QuestionMap<'a, QuestionEntry<'a>>,
  grand_totals: UserMap<'a, u32>,
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

    AllocCorrSet {
      questions,
      users,
      q_to_score,
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
    users.clone_from(&self.q_to_score[qs[0]].1);
    for q in &qs[1..] {
      users.intersect(&self.q_to_score[*q].1);
    }

    let mut n = 0;
    for (i, u) in users.indices().enumerate() {
      let total = qs
        .iter()
        .map(|q| unsafe {
          let (u_scores, _) = self.q_to_score.get_unchecked(*q);
          *u_scores.get_unchecked(u)
        })
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

#[cfg(test)]
mod test {
  use super::*;
  use crate::test_inner;

  test_inner!(alloc, AllocCorrSet);
}
