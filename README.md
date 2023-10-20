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