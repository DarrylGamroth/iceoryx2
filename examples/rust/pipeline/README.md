# Pipeline

This example demonstrates the staged `Pipeline` API in one process.
It creates one ingress, two workers, and one egress endpoint:

* ingress injects `u64` samples
* worker stage `0` adds `10`
* worker stage `1` multiplies by `2`
* egress receives and prints the final values

## How to Run

```sh
cargo run --example pipeline
```
