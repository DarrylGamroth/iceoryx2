# Pipeline Dynamic User Header

This example demonstrates a staged pipeline with:

- dynamic payloads (`bb::Slice<uint8_t>`)
- custom user headers propagated and mutated across stages
- three separate processes (`ingress`, `worker`, `egress`)

## How to Build

Build all C++ examples from the repository root:

```sh
cmake -S . -B target/ff/cc/build -DBUILD_EXAMPLES=ON
cmake --build target/ff/cc/build
```

## How to Run

Open three terminals.

### Terminal 1

```sh
./target/ff/cc/build/examples/cxx/pipeline_dynamic_user_header/example_cxx_pipeline_dynamic_user_header_egress
```

### Terminal 2

```sh
./target/ff/cc/build/examples/cxx/pipeline_dynamic_user_header/example_cxx_pipeline_dynamic_user_header_worker
```

### Terminal 3

```sh
./target/ff/cc/build/examples/cxx/pipeline_dynamic_user_header/example_cxx_pipeline_dynamic_user_header_ingress
```
