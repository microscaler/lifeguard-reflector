# LifeReflector performance

Extracted from Lifeguard's [`docs/perf/lifeguard-baseline.md`](https://github.com/microscaler/lifeguard/blob/main/docs/perf/lifeguard-baseline.md)
when LifeReflector moved to this repository (27 Oct 2026). The design review is in
[`lifereflector-design-review.md`](lifereflector-design-review.md).

## How it was measured

Lifeguard's perf suite (`examples/perf-suite` there) on a 24-core dev box against local Postgres 16
and Redis 7. The `reflector_crate` scenario drove this crate end to end (it stayed behind with the
move; it is in Lifeguard's history up to commit `cc5d592`): writers commit rows through the
trigger `#[notify]` generates, and each key's invalidation is timed in a wrapping `CacheSink`. The
`reflector_*` scenarios there are the Phase 1 prototype (`NOTIFY` → refresh `SELECT` → Redis
`SET`), per-key and batched.

## End to end

LifeReflector measured end to end: a writer commits a row
with the generated trigger, the reflector `UNLINK`s its key.

| Load | Keys/s | e2e p50 | e2e p99 | Notes |
|---|---:|---:|---:|---|
| 1 writer, 500 rows/s | 422 | 0.78 ms | 2.6 ms | 1,994 flushes for 2,000 changes: each change goes out on its own |
| 16 writers, as fast as they go | 75.9k | 0.32 ms | 2.1 ms | 34k flushes (about 7 keys each): batches grow with load |
| same, 1M unrelated keys in Redis, writes start during the start-up catch-up | 77.1k (burst), 419 (paced) | 0.32 ms / 0.84 ms | 2.1 ms / 2.8 ms | the catch-up `SCAN`s a page per loop pass between live flushes, so it does not delay them |

For comparison, the Phase 1 prototype that re-read each row and `SET` it fell behind under the
same burst (8.4k keys/s, lag growing to 26 s); its batched variant kept up (82.6k/s, p99 2.2 ms),
which is the shape the crate now has, minus the re-read. Writers are unaffected by the trigger's
extra `v` and `ts` fields (the INSERT's p50 stays at ~0.18–0.21 ms in both scenarios).

Two earlier paced runs showed an e2e p50 of 2.5 s. In the first, the start-up catch-up ran to
completion before live changes were served (it is now incremental). The second used the old
harness, which started writing on a fixed 1.5 s timer; it was not reproduced once the harness
waited for leadership, so its cause is not fully pinned down. The third row starts writing during
a long catch-up on purpose and shows no delay; rerun it after any change to the loop.

## Findings that concern the reflector

| # | Finding | Evidence | Proposal (keeps the API) | Expected gain | Size |
|---|---|---|---|---|---|
| 7 | **Notifications have no wake-up.** `Client::notifications()` is a queue the caller must poll. | The reflector prototype pays its 200 µs poll interval; a 50 ms poll in an application adds 0–50 ms | A `may` channel or event signalled by the `pg-io` coroutine on `NotificationResponse` (in `may_postgres`) | Removes the poll latency; enables a real reflector | Small (other repo) |
| 11 | LifeReflector: the per-key refresh loop cannot keep up with bursts; batching can. Full design review in the companion doc. | per-key 7–14k/s with lag growing to 15–37 s; batched 74–80k/s, p99 2.7–2.8 ms | Batched loop, version-checked writes, backfill on reconnect | — | Large: needs decisions |

Finding 7 is fixed in `may_postgres` (microscaler fork, 9c55df0): `Client::next_notification`
blocks until the connection delivers a notification.
