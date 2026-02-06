# Pipeline

This example demonstrates the staged `Pipeline` API with three separate
processes:

* ingress injects `u64` samples
* worker stage `0` adds `10`
* egress receives and prints the final values

For dynamic payload and user-header semantics, see:

* `examples/rust/pipeline_dynamic_user_header/README.md`

## How to Run

Open three terminals.

### Terminal 1

```sh
cargo run --example pipeline_egress
```

### Terminal 2

```sh
cargo run --example pipeline_worker
```

### Terminal 3

```sh
cargo run --example pipeline_ingress
```
