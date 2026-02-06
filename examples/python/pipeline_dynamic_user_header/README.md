# Pipeline Dynamic User Header

This example demonstrates staged `Pipeline` communication with:

- dynamic payloads (`iox2.Slice[ctypes.c_uint8]`)
- custom user headers propagated and mutated across stages
- three separate processes (`ingress`, `worker`, `egress`)

## How to Build

Before proceeding, a virtual environment with all dependencies needs to be
created. You can find the detailed instructions in the
[Python Examples Readme](../README.md).

```sh
poetry --project iceoryx2-ffi/python install
```

Then build and install the Python bindings into the virtual environment:

```sh
poetry --project iceoryx2-ffi/python run maturin develop --manifest-path iceoryx2-ffi/python/Cargo.toml --target-dir target/ff/python
```

## How to Run

Open three terminals.

### Terminal 1

```sh
poetry --project iceoryx2-ffi/python run python examples/python/pipeline_dynamic_user_header/egress.py
```

### Terminal 2

```sh
poetry --project iceoryx2-ffi/python run python examples/python/pipeline_dynamic_user_header/worker.py
```

### Terminal 3

```sh
poetry --project iceoryx2-ffi/python run python examples/python/pipeline_dynamic_user_header/ingress.py
```
