# ADR 0002: Layered package and incremental processing

- Status: accepted
- Date: 2026-11-07
- Supersedes: the flat module layout of v0.1

## Context

v0.1 proved the pipeline end to end, but:

- modules mixed concerns (the extractor built file paths, the loader parsed
  BIDS files and ran SQL);
- adding an entity meant editing the CLI and several modules;
- every load rebuilt all raw tables, so load time grew with total history;
- per-dataset downloads ran one at a time.

## Decision

1. **Layers with one direction of dependency:** sources → storage → extract /
   load → pipeline → cli. Details in [architecture.md](../architecture.md).
2. **Interfaces at the seams:** `ObjectStore` (Protocol) for storage,
   `Extractor` and `RawTableLoader` (abstract base classes) for entities, with
   registries so new ones plug in without editing callers.
3. **Dependency injection:** extractors receive a `RunContext` (settings,
   client, store) instead of building their own. Tests inject temporary
   storage and mocked HTTP.
4. **Incremental load:** a load log stores each partition's fingerprint;
   only new or changed partitions are reloaded, each in its own transaction.
5. **Bounded concurrency** for per-dataset downloads, with thread-local HTTP
   sessions and a single-writer watermark.
6. **Errors vs warnings:** failed items are errors (exit code 1, retried next
   run); upstream quirks needing no action are warnings (recorded only), to
   avoid alert fatigue on a daily schedule.

## Alternatives considered

- **fsspec for storage.** Gives S3/GCS for free but adds a dependency and hides
  the atomic-write semantics this pipeline relies on. A small explicit
  protocol keeps the guarantee visible; an fsspec-backed implementation can
  still be added behind it.
- **asyncio (httpx) instead of threads.** Higher ceiling for concurrency, but
  the bottleneck is politeness to a free service, not threads. Threads keep
  the code synchronous and easier to read.
- **Keep full rebuilds.** Simpler, but defeats the point of demonstrating
  incremental processing.

## Consequences

- More files, each with a single, documented responsibility.
- 43 tests at 97% coverage; mypy strict and coverage gates in CI.
- Raw layer paths changed (state and manifests moved under `_system/`).
  v0.1 data should be removed with `make clean` before the first v0.2 run.
