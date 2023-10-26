use corrset::{utils, Question, User};
use fxhash::FxHashSet;
use itertools::Itertools;
use rayon::prelude::*;
use std::collections::HashMap;
use std::time::Instant;

fn intersect_into(dst: &mut [u64], src_a: &[u64], src_b: &[u64]) {
  debug_assert_eq!(dst.len(), src_a.len());
  debug_assert_eq!(dst.len(), src_b.len());
  for ((dst, src_a), src_b) in dst.iter_mut().zip(src_a).zip(src_b) {
    *dst = src_a & src_b
  }
}

fn iterate_bits(src: &[u64]) -> BitsIterator {
  BitsIterator {
    vec: src,
    idx: 0,
    curr: src[0],
  }
}

struct BitsIterator<'a> {
  vec: &'a [u64],
  idx: usize,
  curr: u64,
}

impl<'a> Iterator for BitsIterator<'a> {
  type Item = usize;

  fn next(&mut self) -> Option<Self::Item> {
    if self.curr == 0 {
      if self.idx >= self.vec.len() {
        return None;
      }

      match self.vec[(self.idx + 1)..]
        .iter()
        .find_position(|v| **v != 0)
      {
        None => {
          self.idx = self.vec.len();
          return None;
        }
        Some((idx, val)) => {
          self.idx += idx + 1;
          self.curr = *val;
        }
      }
    }

    let offset = self.curr.trailing_zeros();
    self.curr ^= 1 << offset;
    Some(self.idx * 64 + offset as usize)
  }
}
fn main() {
  let mut args = std::env::args().skip(1);

  let k = match args.next() {
    Some(k) => k.parse::<usize>().unwrap(),
    None => 5,
  };
  assert_ne!(k, 0);

  let kind = match args.next() {
    Some(kind) => kind,
    None => "large".to_string(),
  };

  let start = Instant::now();
  let data = corrset::load_rows(format!("data/data-{kind}.json")).unwrap();
  let read = start.elapsed();
  println!("Read {} file in {}ms", kind, read.as_millis());

  let start = Instant::now();
  let (users, questions): (FxHashSet<_>, FxHashSet<_>) =
    data.iter().map(|row| (&row.user, &row.question)).unzip();
  let users: Vec<_> = users.into_iter().cloned().collect();
  let user_lookup: HashMap<&User, usize> = users.iter().enumerate().map(|(i, u)| (u, i)).collect();

  let questions: Vec<_> = questions.into_iter().cloned().collect();
  let question_lookup: HashMap<&Question, usize> =
    questions.iter().enumerate().map(|(i, u)| (u, i)).collect();

  let mut scores = vec![vec![0; questions.len()]; users.len()]; // [user][question]
  let mut grand_scores = vec![0; users.len()];

  let created = start.elapsed();
  println!("Created users and questions in {}ms", created.as_millis());
  println!("{} users", users.len());
  println!("{} questions", questions.len());

  let start = Instant::now();
  let mut bitsets: Vec<_> = (0..questions.len())
    .map(|_| vec![0u64; (users.len() + 63) / 64])
    .collect();

  for row in data.iter() {
    let u_idx = user_lookup[&row.user];
    let q_idx = question_lookup[&row.question];
    bitsets[q_idx][u_idx / 64] |= 1 << (u_idx % 64);
    scores[u_idx][q_idx] = row.score;
    grand_scores[u_idx] += row.score;
  }

  let scores = scores;
  let bitsets = bitsets;

  let bitsets_elapsed = start.elapsed();
  println!("Created scores in {}ms", bitsets_elapsed.as_millis());

  let start = Instant::now();
  let max = questions.len();
  let (max_set, max_value) = questions[0..(max - k + 1)]
    .par_iter()
    .enumerate()
    .map(|(start_idx, _)| {
      let mut working_questions: Vec<_> = (start_idx..(start_idx + k)).collect();
      let mut working_bitsets = vec![vec![0; (users.len() + 63) / 64]; k];
      let mut actual_scores = vec![0.0; users.len()];
      let mut predicted_scores = vec![0.0; users.len()];

      let calc_question =
        |working_questions: &[usize], working_bitsets: &mut [Vec<u64>], idx: usize| {
          if idx == 0 {
            return;
          }

          let (start, end) = working_bitsets.split_at_mut(idx);
          let prev = start.last().unwrap();
          let next = end.first_mut().unwrap();
          intersect_into(next, prev, &bitsets[working_questions[idx]]);
        };
      // clone base bitset
      working_bitsets[0].clone_from(&bitsets[working_questions[0]]);
      for idx in 1..k {
        calc_question(&mut working_questions, &mut working_bitsets, idx);
      }

      let calc_correlation = |actual_scores: &mut [f64],
                              predicted_scores: &mut [f64],
                              working_questions: &[usize],
                              bitset: &[u64]|
       -> f64 {
        let mut count = 0usize;
        for ((user_idx, actual_score), predicted_score) in iterate_bits(bitset)
          .zip(actual_scores.iter_mut())
          .zip(predicted_scores.iter_mut())
        {
          let user_row = &scores[user_idx];
          *actual_score = grand_scores[user_idx] as f64;
          *predicted_score = working_questions
            .iter()
            .map(|question_idx| user_row[*question_idx])
            .sum::<u32>() as f64;
          count += 1;
        }
        utils::correlation(&predicted_scores[..count], &actual_scores[..count])
      };

      // calc initial scores
      let mut max_correlation = calc_correlation(
        &mut actual_scores,
        &mut predicted_scores,
        &mut working_questions,
        working_bitsets.last().unwrap(),
      );
      let mut max_questions = working_questions.clone();

      loop {
        // get next question set
        // searching from the end, find first element that can increment
        let start = working_questions
          .iter()
          .enumerate()
          .skip(1)
          .rposition(|(offset, q_idx)| *q_idx + k - offset < questions.len());

        match start {
          // if not found, break
          None => break,
          // otherwise increment found element, and set following elements to be sequential
          Some(start_idx) => {
            // start_idx is off by one due to skipping first field
            let start_idx = start_idx + 1;
            let start_value = working_questions[start_idx] + 1;
            for idx in start_idx..working_questions.len() {
              working_questions[idx] = start_value + idx - start_idx;
              calc_question(&mut working_questions, &mut working_bitsets, idx);
            }
          }
        }

        // calc correlation
        let correlation = calc_correlation(
          &mut actual_scores,
          &mut predicted_scores,
          &mut working_questions,
          working_bitsets.last().unwrap(),
        );

        // compare against
        if correlation >= max_correlation {
          max_correlation = correlation;
          max_questions = working_questions.clone();
        }
      }

      (max_questions, max_correlation)
    })
    .max_by(|(_, a), (_, b)| a.total_cmp(b))
    .unwrap();
  let run = start.elapsed();

  println!("Finished processing in {}ms", run.as_millis());
  println!("{max_set:?} {max_value}");
  for question in max_set {
    println!("{:?}", questions[question]);
  }
}
