# Pipeline Dynamic User Header

This example demonstrates staged `Pipeline` communication with:

* dynamic payload (`[u8]`) via `loan_slice_uninit(...)`
* custom user header mutation across stages
* three separate processes (ingress, worker, egress)

## How to Run

Open three terminals.

### Terminal 1

```sh
cargo run --example pipeline_dyn_user_header_egress
```

### Terminal 2

```sh
cargo run --example pipeline_dyn_user_header_worker
```

### Terminal 3

```sh
cargo run --example pipeline_dyn_user_header_ingress
```
