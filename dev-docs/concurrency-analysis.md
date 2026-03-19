# Concurrency analysis: concurrent build requests

## The problem

The current build flow has a check-then-act race condition. Two concurrent requests for the same (dataset_name, dataset_version) with overlapping timestamp ranges can both see timestamps as missing and both insert data, producing duplicate rows.

```
Request A: get_existing_timestamps() -> sees T as missing
Request B: get_existing_timestamps() -> sees T as missing
Request A: builds T, inserts
Request B: builds T, inserts  (DUPLICATE)
```

Beyond duplicates, this also wastes compute (building the same data twice in subprocesses).

## Current architecture context

- single FastAPI process, async endpoints, builders run in sync subprocesses
- psycopg v3 connection pool (min=2, max=10)
- each DB function (`get_existing_timestamps`, `insert_rows`, etc.) checks out its own connection from the pool
- recursive dependency builds each invoke `_build_recursive()` independently
- no existing locks or concurrency primitives anywhere in the codebase
- the (dataset_name, dataset_version, timestamp) triple is non-unique by design (multi-row datasets like mock-multi-ohlc produce multiple rows per timestamp)

## Approaches considered

### 1. Coarse PostgreSQL advisory lock on (dataset_name, dataset_version)

Lock the entire dataset for the duration of a build using `pg_advisory_lock(hash)` where the hash is derived from (name, version).

Before `_build_recursive()` does the check-build-insert, acquire an advisory lock on a dedicated pool connection. Any other request for the same dataset blocks until the first finishes. Release explicitly in a `finally` block.

**Pros:**
- simple to implement (~15 lines, no schema changes)
- eliminates both duplicates and wasted compute
- advisory locks auto-release if the connection/session drops (crash-safe)
- works well at current scale (single service, low concurrency)
- if we ever move to multiple service instances, advisory locks still work (same DB)
- easy to reason about correctness: one build at a time per dataset

**Cons:**
- serializes ALL builds for a dataset, even non-overlapping time ranges (e.g. Jan and July block each other)
- builds can be slow (subprocess execution) -- the second caller blocks for the full duration
- holds one pool connection per in-flight dataset build just for the lock (idle during subprocess execution)
- coarse granularity wastes potential parallelism

### 2. Fine-grained advisory lock on (dataset_name, dataset_version, timestamp)

Lock individual timestamps rather than entire datasets. For each missing timestamp, acquire `pg_advisory_lock(hash(name, version, timestamp))` before building it.

**Pros:**
- maximizes parallelism -- non-overlapping ranges don't block each other
- no schema changes needed
- more fair under concurrent load

**Cons:**
- many more lock acquisitions (one per timestamp per dependency level)
- lock management complexity: must acquire/release many locks, handle partial failures
- risk of deadlocks if two requests build overlapping ranges in different orders (mitigated by always sorting timestamps, but adds a constraint)
- recursive builds compound the lock count
- harder to reason about correctness
- per-timestamp overhead may be significant for large ranges (e.g. 252 NYSE trading days in a year = 252 lock round-trips)

### 3. Database build status table (optimistic claim approach)

Add a `build_status` table that tracks which (dataset, version, timestamp) combinations are currently being built or have been built. Use `INSERT ... ON CONFLICT DO NOTHING` with a unique constraint to "claim" timestamps.

```sql
CREATE TABLE build_status (
    dataset_name TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    timestamp TIMESTAMP(6) NOT NULL,
    status TEXT NOT NULL DEFAULT 'building',  -- 'building' | 'complete' | 'failed'
    claimed_at TIMESTAMP(6) NOT NULL DEFAULT now(),
    UNIQUE (dataset_name, dataset_version, timestamp)
);
```

**Pros:**
- fine-grained without advisory lock overhead
- naturally idempotent -- retries work cleanly
- provides observability: query what's currently being built
- works across multiple service instances
- can be extended to track build history, timing, etc.

**Cons:**
- requires a schema migration (new table)
- must handle cleanup of stale 'building' rows from crashed processes (needs a TTL or reaper)
- two tables to keep in sync (`build_status` and `datasets`)
- adds latency: extra DB round-trips per timestamp
- more moving parts (new table, cleanup logic, status transitions)
- the unique constraint on (name, version, timestamp) only works because status is per-timestamp, but the `datasets` table allows multiple rows per timestamp (multi-row datasets) -- so the "claim" table and the data table have fundamentally different cardinality

### 4. Application-level in-memory lock (asyncio.Lock per dataset)

Maintain a `dict[tuple[str, str], asyncio.Lock]` in the service layer. Acquire before building, release after.

**Pros:**
- zero DB overhead, fastest option
- simple to implement
- no schema changes

**Cons:**
- only works within a single process -- breaks immediately with multiple workers or instances
- builders run in sync subprocesses, so the lock must be held across `run_in_executor` calls
- no persistence -- if the process restarts mid-build, there's no record of in-flight work
- doesn't protect against DB-level races if another writer is ever added

### 5. Hybrid: coarse advisory lock + duplicate-prevention insert guard

Use the coarse advisory lock (approach 1) plus a secondary safeguard at the insert level (e.g. a check-before-insert query or a partial unique index).

**Pros:**
- defense in depth -- even if the lock is somehow bypassed, no duplicates

**Cons:**
- the secondary guard is hard to define because (name, version, timestamp) is intentionally non-unique (multi-row datasets produce multiple rows per timestamp)
- a unique index on (name, version, timestamp) would break multi-row datasets
- hashing the full `data` JSONB is fragile and expensive
- adds complexity for a scenario (lock bypass) that shouldn't happen

## Decision

**Approach 1: coarse advisory lock on (dataset_name, dataset_version).**

Rationale:
1. **matches current reality** -- single-service deployment with low concurrency. serializing builds per dataset is not a real bottleneck; subprocess execution is
2. **simplicity** -- ~15 lines of code, no migrations, no cleanup logic
3. **correctness is easy to verify** -- one build at a time per dataset means no races, no duplicates, no wasted compute
4. **clear upgrade path** -- if serialization becomes a bottleneck, can move to approach 2 or 3 without changing the external API

### Implementation detail: dedicated lock connection

The advisory lock is acquired on a **dedicated pool connection** separate from the connections used by `get_existing_timestamps()` and `insert_rows()`. This works because PostgreSQL advisory locks are cluster-wide: any connection calling `pg_advisory_lock(key)` blocks until the lock is available, regardless of which connection holds it.

This means we don't need to refactor the DB layer to thread connections through. The existing functions keep working as-is.

### Implementation detail: lock release during recursive builds

The advisory lock for a dataset is **released before recursing into dependencies** and **re-acquired after** dependencies finish building. This avoids holding one idle lock connection per level of the dependency tree, which could exhaust the pool under concurrent load.

The flow for `_build_recursive(A)` where A depends on B:

1. Acquire lock(A), check which timestamps are missing, determine B needs building, release lock(A)
2. Recurse: acquire lock(B), build B, release lock(B)
3. Re-acquire lock(A), re-check missing timestamps, build A, release lock(A)

The re-check in step 3 is necessary because another concurrent request may have built some of A's timestamps while the lock was released. This is just one extra `get_existing_timestamps` call per dependency level.

**Tradeoff:** two concurrent requests for A could both independently decide B needs building and both recurse into B. But lock(B) serializes them, so one builds and the other sees everything already built and skips. No duplicates, just a redundant check -- much better than risking pool exhaustion.

**Pool impact:** at most 1 lock connection per in-flight dataset build (not per dependency depth level). With N concurrent requests, worst case = N lock connections. Current pool max is 10, so this is comfortable.
