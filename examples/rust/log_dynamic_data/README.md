# Log Dynamic Data

This example illustrates the `log` messaging pattern with dynamic payloads (`[u8]`).

It uses:
- `enable_safe_overflow(false)`
- appender `unable_to_deliver_strategy(UnableToDeliverStrategy::Block)`

This demonstrates blocking backpressure when tailers cannot keep up.

## How to Run

### Terminal 1

```sh
cargo run --example log_dyn_tailer
```

### Terminal 2

```sh
cargo run --example log_dyn_appender
```
