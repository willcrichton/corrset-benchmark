use corrset::{
  dispatch_inner_method, dispatch_outer_method, fused::CorrSetFused, CorrSetInner, CorrSetOuter,
  Row,
};

fn main() {
  let mut args = std::env::args().skip(1);

  let outer_method = args.next().unwrap();
  let inner_method = args.next().unwrap();
  let k = match args.next() {
    Some(k) => k.parse::<usize>().unwrap(),
    None => 5,
  };
  let kind = match args.next() {
    Some(kind) => kind,
    None => "large".to_string(),
  };

  let data = &corrset::load_rows(format!("data/data-{kind}.json")).unwrap();

  fn run_outer<O: CorrSetOuter>(data: &[Row], k: usize, inner_method: &str) {
    let outer = O::new();
    fn run_inner<'a, I: CorrSetInner<'a>>(data: &'a [Row], k: usize, outer: impl CorrSetOuter) {
      let cs = I::build(data);
      let combs = cs.combinations(k);
      println!("{:#?}", outer.k_set(&cs, combs));
    }
    dispatch_inner_method!(inner_method, run_inner, data, k, outer);
  }

  if outer_method.as_str() == "fused" {
    let cs = CorrSetFused::build(data);
    println!("{:#?}", cs.k_set(k));
    return;
  }

  dispatch_outer_method!(
    outer_method.as_str(),
    run_outer,
    data,
    k,
    inner_method.as_str()
  );
}
