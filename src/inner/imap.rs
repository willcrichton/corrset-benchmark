use std::sync::Arc;

use super::{
  indexed::{QuestionIdx, QuestionRef, UserRef},
  CorrSetInner,
};
use crate::{
  utils::{self, Captures},
  Question, Row,
};
use fxhash::FxHashSet as HashSet;
use indexical::{map::DenseArcIndexMap as DenseIndexMap, IndexedDomain};

pub type QuestionMap<'a, T> = DenseIndexMap<'a, QuestionRef<'a>, T>;
pub type UserMap<'a, T> = DenseIndexMap<'a, UserRef<'a>, T>;

pub struct ImapCorrSet<'a> {
  questions: Arc<IndexedDomain<QuestionRef<'a>>>,
  users: Arc<IndexedDomain<UserRef<'a>>>,
  q_to_score: QuestionMap<'a, UserMap<'a, Option<u32>>>,
  grand_totals: UserMap<'a, u32>,
}

impl<'a> CorrSetInner<'a> for ImapCorrSet<'a> {
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
      UserMap::<'_, Option<u32>>::new(&users, |_| None)
    });
    for r in data {
      q_to_score
        .get_mut(&QuestionRef(&r.question))
        .unwrap()
        .insert(UserRef(&r.user), Some(r.score));
    }

    let grand_totals = UserMap::new(&users, |u| {
      q_to_score.values().filter_map(|v| v[u]).sum::<u32>()
    });
    ImapCorrSet {
      questions,
      users,
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
    let (qs_scores, grand_scores): (Vec<_>, Vec<_>) = self
      .users
      .indices()
      .filter_map(|u| {
        let total = qs
          .iter()
          .map(|q| self.q_to_score[*q][u])
          .sum::<Option<u32>>()?;
        let grand_total = self.grand_totals[u];
        Some((total as f64, grand_total as f64))
      })
      .unzip();
    utils::correlation(&qs_scores, &grand_scores)
  }
}

#[cfg(test)]
mod test {
  use super::*;
  use crate::test_inner;

  test_inner!(imap, ImapCorrSet);
}
