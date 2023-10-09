use crate::{inner::CorrSetInner, Question};

pub mod chunked;
pub mod parallel;
pub mod serial;

pub const TOP_N: usize = 10;

pub trait CorrSetOuter {
  fn new() -> Self;
  fn k_sets<'a, T: CorrSetInner<'a>>(&self, corrset: &T, k: usize) -> Vec<Vec<&'a Question>>;
}

#[macro_export]
macro_rules! dispatch_outer_method {
  ($key:expr, $f:ident, $($arg:expr),*) => {{
    match $key {
      "0_serial" => $f::<$crate::outer::serial::CorrSetSerial>($($arg),*),
      "1_parallel" => $f::<$crate::outer::parallel::CorrSetParallel>($($arg),*),
      "2_chunked" => $f::<$crate::outer::chunked::CorrSetChunked>($($arg),*),
      k => unimplemented!("{k}"),
    }
  }};
}

pub fn inner_names() -> Vec<String> {
  vec!["0_serial".into(), "1_parallel".into(), "2_chunked".into()]
}
