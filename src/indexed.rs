use crate::{correlation, CorrSet, Question, Row, User, TOP_N};
use float_ord::FloatOrd;
use fxhash::FxHashMap as HashMap;
use indexical::{define_index_type, IndexedDomain};
use indicatif::ProgressIterator;
use itertools::Itertools;

define_index_type! {
  pub struct QuestionIdx for Question = u16;
  DISABLE_MAX_INDEX_CHECK = true; //cfg!(not(debug_assertions));
}

define_index_type! {
  pub struct UserIdx for User = u32;
  DISABLE_MAX_INDEX_CHECK = true; //cfg!(not(debug_assertions));
}

pub struct IndexedCorrSet;

impl CorrSet for IndexedCorrSet {
  fn k_sets(&self, data: &[Row], k: usize) -> Vec<Vec<Question>> {
    macro_rules! group_by {
      ($f:expr) => {
        data
          .iter()
          .map($f)
          .into_grouping_map()
          .collect::<HashMap<_, _>>()
      };
    }

    // NEW: convert users/questions into indexed domains
    let (mut users, mut questions): (Vec<_>, Vec<_>) =
      data.iter().map(|row| (&row.user, &row.question)).unzip();
    users.dedup();
    questions.dedup();
    let users = IndexedDomain::from_iter(users.into_iter().cloned());
    let questions = IndexedDomain::from_iter(questions.into_iter().cloned());

    // This code is the same, except we're using indices rather than the raw strings
    let q_to_score = group_by!(|r: &Row| (
      questions.index(&r.question),
      (users.index(&r.user), r.score)
    ));
    let u_to_score = group_by!(|r: &Row| (
      users.index(&r.user),
      (questions.index(&r.question), r.score)
    ));
    let grand_totals = u_to_score
      .iter()
      .map(|(user, scores)| {
        let total = scores.values().map(|n| *n as usize).sum::<usize>();
        (*user, total)
      })
      .collect::<HashMap<_, _>>();

    // This code is also the same
    let pb = crate::pb(q_to_score.len(), k);
    let q_combs = questions.indices().combinations(k).progress_with(pb);
    let q_corrs = q_combs.filter_map(|qs| {
      let (qs_scores, grand_scores): (Vec<_>, Vec<_>) = grand_totals
        .iter()
        .filter_map(|(u, grand_total)| {
          let total = qs
            .iter()
            .map(|q| q_to_score[q].get(u).copied())
            .sum::<Option<u32>>()?;
          Some((total as f64, *grand_total as f64))
        })
        .unzip();
      let r = correlation(&qs_scores, &grand_scores);
      (!r.is_nan()).then_some((qs, r))
    });

    // At the end, we have to convert indices back to the original strings
    q_corrs
      .sorted_by_key(|(_, r)| FloatOrd(*r))
      .take(TOP_N)
      .map(|(qs, _)| {
        qs.into_iter()
          .map(|q| questions.value(q).clone())
          .collect_vec()
      })
      .collect_vec()
  }
}
