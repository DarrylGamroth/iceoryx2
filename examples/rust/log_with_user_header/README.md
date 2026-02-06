# Log With User Header

This example illustrates the `log` messaging pattern with a custom user header.

It uses:
- `enable_safe_overflow(false)`
- appender `unable_to_deliver_strategy(UnableToDeliverStrategy::DiscardSample)`

This demonstrates a drop-on-pressure behavior while still exposing sequence gaps on the tailer.

## How to Run

### Terminal 1

```sh
cargo run --example log_user_header_tailer
```

### Terminal 2

```sh
cargo run --example log_user_header_appender
```
