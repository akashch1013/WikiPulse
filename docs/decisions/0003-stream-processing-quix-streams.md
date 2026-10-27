# 0003. Stream processing with Quix Streams

Status: Accepted (2026-10-28)

## Context
The stream processor needs:
- per-key deduplication on `meta.id`
- event-time windows (1-min tumbling per wiki and editor type, 60-min hopping per page) with an explicit grace
  period and explicit handling of events later than the grace
- persistent state that survives restarts
- sinks to the lakehouse and Postgres that are flushed before offsets are committed

The budget is about 384 MB of RAM locally and 0.5 vCPU / 1 GiB on Azure Container Apps.

## Decision
Use Quix Streams 3.28.x (Python 3.12), with RocksDB state and changelog topics.
- **Windows:** grace period of 5 minutes, with an `on_late` callback that produces late events to
  `wikipulse.page_change.late.v1` and increments a metric.
- **Processing guarantee:** `at-least-once`, in both environments, so the code path is the same everywhere.
  Every sink write is idempotent: bronze rows keyed by `meta.id`, and Postgres upserts keyed by window key.

## Alternatives considered
- **PyFlink 2.3.0.** A JVM JobManager plus TaskManager needs roughly 1.6 + 1.7 GB in default configuration. That
  exceeds the local budget, and it adds a cluster to operate for 17 ev/s.
- **Spark Structured Streaming.** It needs a 1–2 GB driver minimum and runs as micro-batches. It is heavy for this
  rate.
- **RisingWave v3.1.** Its documented minimum for a compute node is 2 cores / 8 GiB.
- **Bytewax 0.21.1.** The company behind it stopped commercial work in May 2025, and the project is
  community-maintained with no release since November 2024.
- **faust-streaming 0.15.3.** It is maintained, but its windowing has weaker late-data semantics and no grace or
  `on_late` hook.
- **Hand-written consumer with in-memory windows.** It would lose state on restart and reimplement watermarking.

## Consequences
- Exactly-once mode (Kafka transactions) is not used, because Event Hubs Standard has no transactions. Duplicates
  from replays are removed by the dedup state and again in silver.
- Streaming aggregates are provisional. The hourly batch recompute from silver is authoritative, and a
  reconciliation test bounds the difference by the recorded late and duplicate counts.
- RocksDB state and changelog topics need fsync-friendly local storage (see 0013), and the changelog topics must
  be pre-created on Event Hubs.
- Quix is less known to reviewers than Flink. The architecture doc states the window, watermark and lateness
  semantics explicitly rather than relying on the library name.
