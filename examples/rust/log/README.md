# Log

This example illustrates the `log` messaging pattern with one appender and one tailer.

It uses:
- `enable_safe_overflow(true)`
- `retention_size(4)`

With a slow tailer, you should see sequence gaps (`gap detected`) once old samples are overwritten.

## How to Run

### Terminal 1

```sh
cargo run --example log_tailer
```

### Terminal 2

```sh
cargo run --example log_appender
```
