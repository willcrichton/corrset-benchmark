use crate::{utils, Question, Row, User};
use fxhash::FxHashMap as HashMap;

use super::CorrSetInner;

pub struct NaiveCorrSet<'a> {
  q_to_score: HashMap<&'a Question, HashMap<&'a User, u32>>,
  grand_totals: HashMap<&'a User, u32>,
}

impl<'a> CorrSetInner<'a> for NaiveCorrSet<'a> {
  type Q = &'a Question;
  type Scratch = ();

  fn build(data: &'a [Row]) -> Self {
    // Setup auxiliary data structures
    let q_to_score = utils::group_by(data.iter().map(|r| (&r.question, &r.user, r.score)));
    let u_to_score = utils::group_by(data.iter().map(|r| (&r.user, &r.question, r.score)));
    let grand_totals = u_to_score
      .iter()
      .map(|(user, scores)| {
        let total = scores.values().sum::<u32>();
        (*user, total)
      })
      .collect::<HashMap<_, _>>();

    NaiveCorrSet {
      q_to_score,
      grand_totals,
    }
  }

  fn iter_qs(&self) -> impl Iterator<Item = &'a Question> + '_ {
    self.q_to_score.keys().copied()
  }

  fn to_question(&self, q: &'a Question) -> &'a Question {
    q
  }

  fn init_scratch(&self) -> Self::Scratch {}

  fn corr_set(&self, _: &mut (), qs: &[Self::Q]) -> f64 {
    let (qs_scores, grand_scores): (Vec<_>, Vec<_>) = self
      .grand_totals
      .iter()
      .filter_map(|(u, grand_total)| {
        let total = qs
          .iter()
          .map(|q| self.q_to_score[*q].get(u).copied())
          .sum::<Option<u32>>()?;
        Some((total as f64, *grand_total as f64))
      })
      .unzip();
    utils::correlation(&qs_scores, &grand_scores)
  }
}

#[cfg(test)]
mod test {
  use super::*;
  use crate::test_inner;

  test_inner!(naive, NaiveCorrSet);
}
