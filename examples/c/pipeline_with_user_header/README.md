# Pipeline With User Header

This example demonstrates a three-stage pipeline setup with three separate
processes:

- ingress process loans a sample, fills payload and user header, then sends it
- worker process receives, mutates payload and header, and forwards
- egress process receives and prints the final payload/header

## How to Build

Build all C examples from the repository root:

```sh
cmake -S . -B target/ff/cc/build -DBUILD_EXAMPLES=ON -DBUILD_CXX=OFF
cmake --build target/ff/cc/build
```

## How to Run

Open three terminals.

### Terminal 1

```sh
./target/ff/cc/build/examples/c/pipeline_with_user_header/example_c_pipeline_with_user_header_egress
```

### Terminal 2

```sh
./target/ff/cc/build/examples/c/pipeline_with_user_header/example_c_pipeline_with_user_header_worker
```

### Terminal 3

```sh
./target/ff/cc/build/examples/c/pipeline_with_user_header/example_c_pipeline_with_user_header_ingress
```
