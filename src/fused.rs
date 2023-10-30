use crate::{
  inner::{
    alloc::{AllocCorrSet, UserSet},
    indexed::QuestionIdx,
  },
  utils::pb_style,
  CorrSetInner, Question, Row,
};
use float_ord::FloatOrd;

use indicatif::{ProgressBar, ProgressIterator};
use itertools::Itertools;
use rayon::prelude::*;

pub struct CorrSetFused<'a> {
  inner: AllocCorrSet<'a>,
}

struct QuestionCombinations<'a, 'b> {
  inner: &'b AllocCorrSet<'a>,
  qs: Vec<QuestionIdx>,
  k: usize,
  users: Vec<UserSet<'a>>,
  qs_scores: &'b mut [f64],
  grand_scores: &'b mut [f64],
  first: bool,
}

impl<'a, 'b> QuestionCombinations<'a, 'b> {
  pub fn new(
    inner: &'b AllocCorrSet<'a>,
    root: QuestionIdx,
    k: usize,
    qs_scores: &'b mut [f64],
    grand_scores: &'b mut [f64],
  ) -> Self {
    let qs = (0..k)
      .map(|j| QuestionIdx::from_usize(root.index() + j))
      .collect_vec();

    let mut users = vec![inner.q_to_score[qs[0]].1.clone()];
    for q in &qs[1..k] {
      let mut last = users.last().unwrap().clone();
      last.intersect(&inner.q_to_score[*q].1);
      users.push(last);
    }

    QuestionCombinations {
      inner,
      qs,
      k,
      users,
      qs_scores,
      grand_scores,
      first: true,
    }
  }
}

impl<'a, 'b> Iterator for QuestionCombinations<'a, 'b> {
  type Item = (Vec<QuestionIdx>, f64);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    if self.first {
      self.first = false;
    } else {
      let n = self.inner.questions.len();
      let mut i = self.k - 1;

      while self.qs[i] == n + i - self.k {
        if i > 1 {
          i -= 1;
        } else {
          return None;
        }
      }

      self.qs[i] += 1;
      let [cur, prev] = unsafe { self.users.get_many_unchecked_mut([i, i - 1]) };
      cur.clone_from(prev);
      cur.intersect(&self.inner.q_to_score[self.qs[i]].1);

      for j in (i + 1)..self.k {
        self.qs[j] = self.qs[j - 1] + 1;
        let [cur, prev] = unsafe { self.users.get_many_unchecked_mut([j, j - 1]) };
        cur.clone_from(prev);
        cur.intersect(&self.inner.q_to_score[self.qs[j]].1);
      }
    }

    let output = (
      self.qs.clone(),
      self.inner.corr_set_score(
        self.qs_scores,
        self.grand_scores,
        unsafe { self.users.last().unwrap_unchecked() },
        &self.qs,
      ),
    );

    Some(output)
  }
}

impl<'a> CorrSetFused<'a> {
  #[inline]
  pub fn build(data: &'a [Row]) -> Self {
    CorrSetFused {
      inner: AllocCorrSet::build(data),
    }
  }

  #[inline]
  pub fn k_set(&self, k: usize) -> Vec<&'a Question> {
    let n = self.inner.questions.len();
    let (qs, _) = self
      .inner
      .questions
      .indices()
      .take(n - k + 1)
      .progress_with(ProgressBar::new((n - k) as u64).with_style(pb_style()))
      .par_bridge()
      .map_init(
        || self.inner.init_scratch(),
        |(qs_scores, grand_scores, _), root| {
          QuestionCombinations::new(&self.inner, root, k, qs_scores, grand_scores)
            .max_by_key(|(_, r)| FloatOrd(*r))
        },
      )
      .flatten()
      .max_by_key(|(_, r)| FloatOrd(*r))
      .unwrap();
    qs.into_iter()
      .map(|q| self.inner.to_question(q))
      .collect_vec()
  }
}
