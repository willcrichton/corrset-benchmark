use crate::{correlation, indexed::UserIdx, CorrSet, Question, Row, TOP_N};
use float_ord::FloatOrd;

use indexical::{impls::BitvecRefIndexSet as IndexSet, index_vec::IndexVec, IndexedDomain};
use indicatif::ProgressIterator;
use itertools::Itertools;

pub struct BitsetCorrSet;

impl CorrSet for BitsetCorrSet {
  fn k_sets(&self, data: &[Row], k: usize) -> Vec<Vec<Question>> {
    let (mut users, mut questions): (Vec<_>, Vec<_>) =
      data.iter().map(|row| (&row.user, &row.question)).unzip();
    users.dedup();
    questions.dedup();
    let users = &IndexedDomain::from_iter(users.into_iter().cloned());
    let questions = &IndexedDomain::from_iter(questions.into_iter().cloned());

    let empty_vec = IndexVec::from_iter(users.indices().map(|_| 0));
    let empty_set = IndexSet::new(&users);
    let mut q_to_score = IndexVec::from_iter(
      questions
        .indices()
        .map(|_| (empty_vec.clone(), empty_set.clone())),
    );
    for r in data {
      let (question, user) = (questions.index(&r.question), users.index(&r.user));
      let (scores, set) = &mut q_to_score[question];
      scores[user] = r.score;
      set.insert(user);
    }

    let grand_totals = users
      .indices()
      .map(|user| {
        q_to_score
          .iter()
          .filter_map(|(scores, set)| set.contains(user).then_some(scores[user]))
          .sum::<u32>()
      })
      .collect::<IndexVec<UserIdx, _>>();

    let pb = crate::pb(q_to_score.len(), k);
    let q_combs = questions
      .as_vec()
      .indices()
      .combinations(k)
      .progress_with(pb);
    let q_corrs = q_combs.filter_map(|qs| {
      let mut users = q_to_score[qs[0]].1.clone();
      for q in &qs[1..] {
        users.intersect(&q_to_score[*q].1);
      }

      let (qs_scores, grand_scores): (Vec<_>, Vec<_>) = users
        .indices()
        .map(|u| {
          let total = qs
            .iter()
            .map(|q| unsafe {
              let (scores, _) = q_to_score.raw.get_unchecked(q.index());
              *scores.raw.get_unchecked(u.index())
            })
            .sum::<u32>();
          let grand_total = unsafe { grand_totals.raw.get_unchecked(u.index()) };
          (total as f64, *grand_total as f64)
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
