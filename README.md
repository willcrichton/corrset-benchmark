# k-CorrSet Benchmark

A repository to test different performance optimizations in a sparse matrix computation. Read the post explaining the benchmark: <https://willcrichton.net/notes/k-corrset/>


## Running the Benchmark

To run the benchmark, first generate the synthetic data:

```
mkdir data
cargo run --release --bin gen-data -- 60000 200 0.2 > data/data-large.json
```

Then run the benchmark:

```
cargo bench
```


## Running One Configuration

To run a single configuration, you can use the `top` binary. It takes at least two arguments: the name of the outer loop (like `2_batched`) and the name of the inner loop (like `6_alloc`).
Check out the [`outer_names`](https://github.com/willcrichton/corrset-benchmark/blob/main/src/outer/mod.rs) and [`inner_names`](https://github.com/willcrichton/corrset-benchmark/blob/main/src/inner/mod.rs) functions to see a list of all the names. You can also optionally provide a value of `k` (default 5) and a name for the dataset (default `"large"`).


To get a quick-and-dirty ETA for the full computation, you can run with a progress bar enabled:

```
cargo run --release --bin top --features progress -- 2_batched 6_alloc
```

To get a profile for the configuration, install the [samply](https://github.com/mstange/samply/) tool. Then run:

```
cargo build --release --bin top
samply record ./target/release/top 2_batched 6_alloc
```

Let it run for ~30s, then hit Ctrl+C and the profile should open up in your browser.
