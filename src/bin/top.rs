fn main() {
  let mut args = std::env::args().skip(1);
  let kind = args.next().unwrap();
  let k = args.next().unwrap().parse::<usize>().unwrap();
  let method = args.next().unwrap();

  let data = corrset::load_rows(format!("data/data-{kind}.json")).unwrap();
  let methods = corrset::methods();

  println!("{:#?}", methods[&method].k_sets(&data, k));
}
