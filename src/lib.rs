#![feature(return_position_impl_trait_in_trait, associated_type_defaults, get_many_mut)]

use anyhow::Result;

use serde::{Deserialize, Serialize};
use std::{fs::File, io::BufReader, path::Path};

pub mod inner;
pub mod outer;
pub mod fused;
mod utils;

pub use inner::{inner_names, CorrSetInner};
pub use outer::{outer_names, CorrSetOuter};

#[derive(PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Debug)]
pub struct User(pub String);

#[derive(PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Debug)]
pub struct Question(pub String);

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Row {
  pub user: User,
  pub question: Question,
  pub score: u32,
}

pub fn load_rows(path: impl AsRef<Path>) -> Result<Vec<Row>> {
  Ok(serde_json::from_reader(BufReader::new(File::open(path)?))?)
}
