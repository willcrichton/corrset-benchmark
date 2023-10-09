use crate::{
  utils::{self, Captures},
  Question, Row, User,
};

use fxhash::{FxHashMap as HashMap, FxHashSet as HashSet};
use indexical::{define_index_type, IndexedDomain};

use super::CorrSetInner;

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct QuestionRef<'a>(pub &'a Question);
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct UserRef<'a>(pub &'a User);

define_index_type! {
  pub struct QuestionIdx for QuestionRef<'a> = u16;
  DISABLE_MAX_INDEX_CHECK = cfg!(not(debug_assertions));
}

define_index_type! {
  pub struct UserIdx for UserRef<'a> = u32;
  DISABLE_MAX_INDEX_CHECK = cfg!(not(debug_assertions));
}

pub struct IndexedCorrSet<'a> {
  users: IndexedDomain<UserRef<'a>>,
  questions: IndexedDomain<QuestionRef<'a>>,
  q_to_score: HashMap<QuestionIdx, HashMap<UserIdx, u32>>,
  grand_totals: HashMap<UserIdx, u32>,
}

impl<'a> CorrSetInner<'a> for IndexedCorrSet<'a> {
  type Q = QuestionIdx;
  type Scratch = ();

  fn build(data: &'a [Row]) -> Self {
    let (users, questions): (HashSet<_>, HashSet<_>) = data
      .iter()
      .map(|row| (UserRef(&row.user), QuestionRef(&row.question)))
      .unzip();

    let users = IndexedDomain::from_iter(users);
    let questions = IndexedDomain::from_iter(questions);

    let q_to_score = utils::group_by(data.iter().map(|r| {
      (
        questions.index(&(QuestionRef(&r.question))),
        users.index(&(UserRef(&r.user))),
        r.score,
      )
    }));
    let u_to_score = utils::group_by(data.iter().map(|r| {
      (
        users.index(&(UserRef(&r.user))),
        questions.index(&(QuestionRef(&r.question))),
        r.score,
      )
    }));
    let grand_totals = u_to_score
      .iter()
      .map(|(user, scores)| {
        let total = scores.values().sum::<u32>();
        (*user, total)
      })
      .collect::<HashMap<_, _>>();

    IndexedCorrSet {
      users,
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
    let (qs_scores, grand_scores): (Vec<_>, Vec<_>) = self
      .users
      .indices()
      .filter_map(|u| {
        let total = qs
          .iter()
          .map(|q| self.q_to_score[q].get(&u).copied())
          .sum::<Option<u32>>()?;
        let grand_total = self.grand_totals[&u];
        Some((total as f64, grand_total as f64))
      })
      .unzip();
    utils::correlation(&qs_scores, &grand_scores)
  }
}
