use std::collections::HashSet;

use maplit::hashset;

use crate::{outer::serial::CorrSetSerial, CorrSetInner, CorrSetOuter, Question, Row, User};

pub fn mock_data() -> Vec<Row> {
  let mk = |u: &str, q: &str, s| Row {
    user: User(u.to_string()),
    question: Question(q.to_string()),
    score: s,
  };
  vec![
    mk("a", "1", 1),
    mk("a", "2", 1),
    mk("a", "3", 0),
    mk("b", "1", 0),
    mk("b", "2", 0),
    mk("b", "3", 1),
    mk("c", "1", 1),
    mk("c", "2", 1),
    mk("c", "3", 0),
  ]
}

pub fn test<'a, T: CorrSetInner<'a>>(data: &'a [Row]) {
  let outer = CorrSetSerial::new();
  let inner = T::build(data);

  assert_eq!(
    outer
      .k_set(&inner, inner.combinations(2))
      .into_iter()
      .cloned()
      .collect::<HashSet<_>>(),
    hashset![Question("1".to_string()), Question("2".to_string())]
  );
}

#[macro_export]
macro_rules! test_inner {
  ($name:ident, $t:ty) => {
    #[test]
    fn $name() {
      let data = $crate::inner::test_utils::mock_data();
      $crate::inner::test_utils::test::<$t>(&data);
    }
  };
}
