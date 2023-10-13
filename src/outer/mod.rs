use crate::{inner::CorrSetInner, Question};

pub mod batched;
pub mod parallel;
pub mod serial;

pub const TOP_N: usize = 10;

pub trait CorrSetOuter {
  fn new() -> Self;
  fn k_set<'a, T: CorrSetInner<'a>>(
    &self,
    corrset: &T,
    combinations: impl Iterator<Item = Vec<T::Q>> + Send,
  ) -> Vec<&'a Question>;
}

#[macro_export]
macro_rules! dispatch_outer_method {
  ($key:expr, $f:ident, $($arg:expr),*) => {{
    match $key {
      "0_serial" => $f::<$crate::outer::serial::CorrSetSerial>($($arg),*),
      "1_parallel" => $f::<$crate::outer::parallel::CorrSetParallel>($($arg),*),
      "2_batched" => $f::<$crate::outer::batched::CorrSetBatched>($($arg),*),
      k => unimplemented!("{k}"),
    }
  }};
}

pub fn outer_names() -> Vec<String> {
  vec!["0_serial".into(), "1_parallel".into(), "2_batched".into()]
}
