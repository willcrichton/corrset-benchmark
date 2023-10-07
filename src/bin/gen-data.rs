use std::env;

use corrset::{Question, Row, User};
use itertools::Itertools;
use rand::{seq::IteratorRandom, thread_rng, Rng};
use uuid::Uuid;

fn main() {
  let mut args = env::args().skip(1);
  let num_users = args.next().unwrap().parse::<usize>().unwrap();
  let num_questions = args.next().unwrap().parse::<usize>().unwrap();
  let sparsity = args.next().unwrap().parse::<f32>().unwrap();

  let all_qs = (0..num_questions)
    .map(|_| Uuid::new_v4().to_string())
    .collect_vec();
  let all_users = (0..num_users)
    .map(|_| Uuid::new_v4().to_string())
    .collect_vec();

  let mut rng = thread_rng();
  let mut rows = Vec::new();
  for q in all_qs {
    let sample_size = ((all_users.len() as f32) * sparsity) as usize;
    for u in all_users.iter().choose_multiple(&mut rng, sample_size) {
      let row = Row {
        user: User(u.clone()),
        question: Question(q.clone()),
        score: if rng.gen_bool(0.5) { 1 } else { 0 },
      };
      rows.push(row);
    }
  }

  println!("{}", serde_json::to_string(&rows).unwrap());
}
