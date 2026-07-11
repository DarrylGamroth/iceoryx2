# Progressive publish/subscribe upstream reconciliation

The implementation branch is based on upstream commit
`04941b22e36fad80d1a45abcba5b80be1358550b` (workspace version `0.9.999`).

The current implementation still matches the assumptions in the progressive
publish/subscribe implementation plan:

- publish/subscribe delivers one shared-memory offset per sample and receiver;
- `SegmentState` reference-counts complete allocations;
- publisher loans are returned through `Sender::return_loaned_sample`;
- subscriber leases are returned through the existing completion queue;
- `MessageTypeDetails` determines internal-header, user-header, and payload
  placement as well as the pool bucket alignment and size.

The implementation therefore keeps publish/subscribe as the transport and adds
a statically compatible delivery mode, a distinct internal header, and distinct
publisher/subscriber/sample types. Ordinary `Sample` and `SampleMut` remain
unchanged. The progressive header alignment makes both the allocation alignment
and the rounded allocation size multiples of 128 bytes.

No upstream pipeline, DMA-BUF, WaitSet, or alternate sample-tracking work
invalidates the plan. WaitSet notification and external-device coherency remain
explicitly outside this implementation.

## Resolved design questions

- A progressive builder wraps the existing publish/subscribe builder and
  returns distinct progressive port and sample types. Ordinary `Sample` and
  `SampleMut` invariants are unchanged.
- The progressive header owns its origin metadata rather than embedding the
  ordinary header. This keeps the ABI and two-line layout explicit.
- `send()` starts with a zero-length prefix. Safe `write_from_slice` or unsafe
  `set_published_len` advances it afterward.
- Sending with no connected receivers succeeds and returns the active writer,
  matching ordinary delivery's successful zero-recipient result.
- Existing offsets and publisher identity provide the version-1 generation
  protection; no additional shared generation field is added.
- Version 1 includes `write_from_slice`; external acquisition uses the unsafe
  pointer and watermark APIs. WaitSet notification remains deferred.

## Crash behavior retained from upstream

A graceful active-writer drop release-stores `Aborted`. An abrupt publisher
process death cannot run that drop and therefore cannot publish a terminal
state into an already borrowed sample. Existing receiver mappings and
whole-sample cleanup keep the allocation from being unsafely reused while a
subscriber lease exists, but its state remains `Filling`. Applications need a
node-death/timeout policy for this case until a future cleanup-owned terminal
marker is designed. This is a liveness uncertainty, not permission to infer
DMA coherence or to read beyond the watermark.
