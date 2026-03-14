# Azure VFS Repro

This folder contains a minimal repro for the sqlite-objs Azure VFS instability.

## Binary

Run from the crate root:

```bash
cd sqlite-objs
cargo run --bin azure_vfs_repro -- <mode> ...
```

The binary loads Azure credentials from `sqlite-objs/.env`.

## Modes

### Single-thread baseline

```bash
cargo run --bin azure_vfs_repro -- threads-distinct repro-single 1 2 0
```

Observed result:

- Succeeds

### Two threads, same process, same Azure DB file

```bash
cargo run --bin azure_vfs_repro -- threads repro-two-threads-small.db 2 2 0
```

Observed result:

- Native allocator abort
- Example failure: `pointer being freed was not allocated`

### Two threads, same process, different Azure DB files

```bash
cargo run --bin azure_vfs_repro -- threads-distinct repro-distinct 2 2 0
```

Observed result:

- Same native allocator abort as above
- This shows the issue is not limited to same-file contention

### Two processes, same Azure DB file

```bash
cargo run --bin azure_vfs_repro -- spawn-two repro-two-proc-small.db 2 0
```

Observed result:

- No allocator crash
- One child may succeed while the other exits with normal SQLite locking (`database is locked`)

## Conclusion

The failure boundary is concurrent in-process use of the sqlite-objs Azure VFS path.

What the repro indicates:

- Plain SQLite modes (`LOCAL_INMEM`, `LOCAL_FILE`) are fine
- Cross-process access to the same Azure-backed DB produces ordinary lock contention
- In-process concurrent Azure-backed opens trigger native memory corruption, even for different DB files

That strongly suggests the bug is in the sqlite-objs Azure VFS/native path rather than in Duroxide provider logic or normal SQLite locking semantics.