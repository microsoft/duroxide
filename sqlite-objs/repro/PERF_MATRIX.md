# Provider Validation Performance Matrix

This file captures the provider-validation comparisons between:

- Built-in `sqlite` using in-memory databases
- Built-in `sqlite` using on-disk local files
- `sqlite-objs` using Azure Blob Storage (`OBJS_AZURE`)

All comparisons below are for the same suite shape:

- 202 provider-validation tests
- `cargo nextest run`
- identical `-j` values across implementations for each comparison row

## Versions

- `duroxide` root sqlite provider: current workspace version
- `sqlite-objs`: `0.1.3-alpha`

`sqlite-objs 0.1.3-alpha` is important because earlier versions reproduced allocator crashes under concurrent Azure-backed opens. On `0.1.3-alpha`, the crash is gone and the suite passes.

## Commands

### Built-in sqlite, in-memory

```bash
cd /Users/affandar/workshop/drox/duroxide
cargo nextest run --all-features -j <N> --test sqlite_provider_validations
```

### Built-in sqlite, local file

```bash
cd /Users/affandar/workshop/drox/duroxide
SQLITE_PROVIDER_MODE=FILE cargo nextest run --all-features -j <N> --test sqlite_provider_validations
```

### sqlite-objs, Azure Blob Storage

```bash
cd /Users/affandar/workshop/drox/duroxide
bash ./run-sqlite-objs-tests.sh -j <N> --test provider_validations
```

The sqlite-objs crate reads Azure configuration from `sqlite-objs/.env`.

## Timing Matrix

| nextest `-j` | sqlite in-memory | sqlite local file | sqlite-objs Azure |
|---|---:|---:|---:|
| 1 | 31.671s | 32.647s | 615.221s |
| 4 | 10.007s | 10.413s | 155.194s |
| 14 | 6.309s | 6.522s | 47.381s |
| 28 | 5.574s | 6.060s | 30.776s |
| 64 | not re-run for this report | 5.932s | 23.481s |

## Direct Comparison Notes

- Local sqlite on disk is very close to local sqlite in memory for this test suite.
- `sqlite-objs` on Azure is much slower in absolute terms, but it scales strongly as concurrency rises.
- At `-j 14`, sqlite-objs Azure is about `7.5x` slower than built-in sqlite in-memory and about `7.3x` slower than sqlite local file.
- At `-j 64`, sqlite-objs Azure is about `4.0x` slower than sqlite local file.

## Scaling Interpretation

### sqlite in-memory

- `31.671s -> 5.574s` from `-j 1` to `-j 28`
- about `5.7x` faster at higher concurrency

### sqlite local file

- `32.647s -> 6.060s` from `-j 1` to `-j 28`
- about `5.4x` faster at higher concurrency

### sqlite-objs Azure

- `615.221s -> 30.776s` from `-j 1` to `-j 28`
- about `20x` faster at higher concurrency

This suggests Azure-backed sqlite-objs is much more latency-bound at low concurrency. Higher nextest concurrency hides a large amount of blob-storage round-trip overhead by overlapping tests.

## What `-j` Means Here

For `cargo nextest run`, `-j N` means:

- up to `N` test cases can run concurrently
- each test case is generally executed in its own subprocess
- it does not mean a single test function gets `N` internal threads

In this suite, one test means one Rust test function such as `test_sqlite_worker_peek_lock_semantics`.

## What “Within a Test” Means

Within one test function:

- the test may create one provider instance or several
- some tests intentionally use a shared factory that reuses the same provider/database inside that one test

That reuse is independent of nextest parallelism.

## Blob and File Creation Semantics

### sqlite-objs in `OBJS_AZURE`

Most provider-validation tests call a factory that creates a fresh provider. In Azure mode, that provider gets a unique blob name:

- `test-<uuid>.db`

This means:

- different tests usually use different Azure blobs
- tests using the shared factory reuse one provider/blob within that one test

### sqlite local file mode

The built-in sqlite file-backed comparison uses a fresh temp file per provider creation.

This means:

- different tests usually use different local SQLite files
- shared-factory tests reuse one provider/file within that one test

## Azure Blob Verification

The sqlite-objs Azure-backed tests were verified to use real blobs in container `duroxide-sqlite`.

Example observed blob:

- `test-0b4777e2-9285-46d5-8309-f198658daf3e.db`
- type: `PageBlob`
- size: `110592`
- last modified: `2026-03-14T23:56:30+00:00`

The container also contains repro blobs such as:

- `repro-two-proc-small.db`
- `repro-distinct-latest-0.db`
- `repro-distinct-latest-1.db`

## Does More Parallelism Create More Blobs?

Not in a strict one-blob-per-thread sense.

What higher `-j` does:

- increases the number of test cases that can be active at the same time
- therefore increases how many fresh provider instances may be created concurrently
- therefore increases how many blobs can be active concurrently

What it does not mean:

- `-j 64` does not imply exactly 64 blobs for the whole run

## Measured Blob Count at `-j 64`

A fresh before/after Azure container snapshot was taken around a full sqlite-objs run:

```bash
bash ./run-sqlite-objs-tests.sh -j 64 --test provider_validations
```

Observed counts:

- before run: `1940` `test-*.db` blobs
- after run: `2143` `test-*.db` blobs
- newly created by this run: `203`

Interpretation:

- `-j 64` did not create 64 blobs
- it created `203` blobs over the entire successful run
- total created blobs track the number of test/provider setups far more than the `-j` value itself

So the fair statement is:

- more parallelism allows more blobs to be active at once
- it does not impose one blob per nextest worker slot

## Repro Outcome Summary

The dedicated repro lives in:

- `sqlite-objs/repro/azure_vfs_repro.rs`

Before upgrading sqlite-objs, concurrent in-process Azure-backed opens caused native allocator crashes. After upgrading to `sqlite-objs 0.1.3-alpha`:

- the allocator crash disappeared
- same-process concurrent access now behaves like normal SQLite locking or succeeds cleanly
- the full sqlite-objs provider-validation suite passes under nextest

## Current Status

- built-in sqlite provider validations pass in both in-memory and file-backed comparison modes
- sqlite-objs Azure provider validations pass on `sqlite-objs 0.1.3-alpha`
- the root helper script `run-sqlite-objs-tests.sh` can run the sqlite-objs suite from the main repo root