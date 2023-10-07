use crate::{correlation, indexed::UserIdx, CorrSet, Question, Row, TOP_N};
use float_ord::FloatOrd;

use indexical::{index_vec::IndexVec, IndexedDomain};
use indicatif::ProgressIterator;
use itertools::Itertools;

pub struct BchecksCorrSet;

impl CorrSet for BchecksCorrSet {
  fn k_sets(&self, data: &[Row], k: usize) -> Vec<Vec<Question>> {
    let (mut users, mut questions): (Vec<_>, Vec<_>) =
      data.iter().map(|row| (&row.user, &row.question)).unzip();
    users.dedup();
    questions.dedup();
    let users = IndexedDomain::from_iter(users.into_iter().cloned());
    let user_indices = || (0..users.len()).map(UserIdx::from_usize);
    let questions = IndexedDomain::from_iter(questions.into_iter().cloned());

    let empty_vec = IndexVec::from_iter(users.indices().map(|_| None));
    let mut q_to_score = IndexVec::from_iter(questions.indices().map(|_| empty_vec.clone()));
    for r in data {
      q_to_score[questions.index(&r.question)][users.index(&r.user)] = Some(r.score);
    }

    let grand_totals = users
      .indices()
      .map(|user| q_to_score.iter().filter_map(|v| v[user]).sum::<u32>())
      .collect::<IndexVec<UserIdx, _>>();

    let pb = crate::pb(q_to_score.len(), k);
    let q_combs = questions
      .as_vec()
      .indices()
      .combinations(k)
      .progress_with(pb);
    let q_corrs = q_combs.filter_map(|qs| {
      let (qs_scores, grand_scores): (Vec<_>, Vec<_>) = user_indices()
        .filter_map(|u| {
          let total = qs
            .iter()
            .map(|q| unsafe {
              let scores = q_to_score.raw.get_unchecked(q.index());
              *scores.raw.get_unchecked(u.index())
            })
            .sum::<Option<u32>>()?;
          let grand_total = unsafe { grand_totals.raw.get_unchecked(u.index()) };
          Some((total as f64, *grand_total as f64))
        })
        .unzip();
      let r = correlation(&qs_scores, &grand_scores);
      (!r.is_nan()).then_some((qs, r))
    });

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
