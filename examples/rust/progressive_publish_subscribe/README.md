# Progressive publish/subscribe polling

This experimental example announces one image allocation to two subscribers and
then commits recognizable rows into its monotonically growing immutable
prefix. The subscribers poll the shared atomic snapshot at different rates, validate
every row, and print publication-to-observation latency.

```bash
cargo run -p example --example progressive_publish_subscribe_polling
```

The committed-prefix and state polling paths allocate no memory and issue no system
calls. Delays in the example are deliberate and model acquisition and reader
work; they are not part of the transport.
