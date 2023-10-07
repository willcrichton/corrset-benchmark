use crate::{correlation, CorrSet, Question, Row, TOP_N};
use float_ord::FloatOrd;
use fxhash::FxHashMap as HashMap;
use indicatif::ProgressIterator;
use itertools::Itertools;

pub struct NaiveCorrSet;

impl CorrSet for NaiveCorrSet {
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

    // Setup auxiliary data structures
    let q_to_score = group_by!(|r: &Row| (&r.question, (&r.user, r.score)));
    let u_to_score = group_by!(|r: &Row| (&r.user, (&r.question, r.score)));
    let grand_totals = u_to_score
      .iter()
      .map(|(user, scores)| {
        let total = scores.values().map(|n| *n as usize).sum::<usize>();
        (*user, total)
      })
      .collect::<HashMap<_, _>>();

    // Execute combinatorial computation
    let pb = crate::pb(q_to_score.len(), k);
    let q_combs = q_to_score.keys().copied().combinations(k).progress_with(pb);
    let q_corrs = q_combs.filter_map(|qs| {
      let (qs_scores, grand_scores): (Vec<_>, Vec<_>) = grand_totals
        .iter()
        .filter_map(|(u, grand_total)| {
          let total = qs
            .iter()
            .map(|q| q_to_score[*q].get(u).copied())
            .sum::<Option<u32>>()?;
          Some((total as f64, *grand_total as f64))
        })
        .unzip();
      let r = correlation(&qs_scores, &grand_scores);
      (!r.is_nan()).then_some((qs, r))
    });

    q_corrs
      .sorted_by_key(|(_, r)| FloatOrd(*r))
      .take(TOP_N)
      .map(|(qs, _)| qs.into_iter().cloned().collect_vec())
      .collect_vec()
  }
}
