use corrset::{dispatch_inner_method, dispatch_outer_method, CorrSetInner, CorrSetOuter, Row};

fn main() {
  let mut args = std::env::args().skip(1);
  let kind = args.next().unwrap();
  let k = args.next().unwrap().parse::<usize>().unwrap();
  let outer_method = args.next().unwrap();
  let inner_method = args.next().unwrap();

  let data = corrset::load_rows(format!("data/data-{kind}.json")).unwrap();

  fn run_outer<O: CorrSetOuter>(data: &[Row], k: usize, inner_method: &str) {
    let outer = O::new();
    fn run_inner<'a, I: CorrSetInner<'a>>(data: &'a [Row], k: usize, outer: impl CorrSetOuter) {
      let cs = I::build(data);
      println!("{:#?}", outer.k_sets(&cs, k));
    }
    dispatch_inner_method!(inner_method, run_inner, data, k, outer);
  }

  dispatch_outer_method!(
    outer_method.as_str(),
    run_outer,
    &data,
    k,
    inner_method.as_str()
  );
}
