# filock — Design Plan

## Overview

`filock` is a Rust crate providing thread-safe, closure-based file locking using `flock(2)`. It offers a simple, predictable API for concurrent file access with deadlock-free guarantees within the same process.

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  FileStore                       │
│  ┌──────────────────┐  ┌──────────────────────┐ │
│  │  Mutex<StoreState>│  │     Condvar          │ │
│  │  (lock tracking)  │  │  (wait notification) │ │
│  └──────────────────┘  └──────────────────────┘ │
├─────────────────────────────────────────────────┤
│  Lock Compatibility Matrix                       │
│  ┌──────────┬──────────┬───────────────┐        │
│  │Held\Req  │ Shared   │ Exclusive     │        │
│  ├──────────┼──────────┼───────────────┤        │
│  │ None     │ ✅ Grant │ ✅ Grant      │        │
│  │ Sh (oth) │ ✅ Grant │ ❌ Block      │        │
│  │ Sh (self)│ ✅ Grant │ ❌ No upgrade │        │
│  │ Ex (oth) │ ❌ Block │ ❌ Block      │        │
│  │ Ex (self)│ ❌ No dn │ ❌ No re-entr │        │
│  └──────────┴──────────┴───────────────┘        │
├─────────────────────────────────────────────────┤
│  Kernel Level: flock(2) per operation fd        │
└─────────────────────────────────────────────────┘
```

## Files

| File | Purpose |
|------|---------|
| `src/lib.rs` | Public API re-exports, crate docs |
| `src/error.rs` | `FileLockError`, `LockType` |
| `src/fd.rs` | Per-operation fd open (no thread-local table) |
| `src/store.rs` | `FileStore` — the core lock manager |
| `tests/concurrency.rs` | Multi-threaded stress & contention tests |
| `tests/edge_cases.rs` | Lifecycle, error propagation, boundary tests |
| `tests/multiprocess.rs` | Cross-fd flock behavior tests |
| `examples/basic.rs` | Usage example |

## Design Decisions

### 1. Single Mutex + Condvar (not nested locks)

The original design used per-file `FileCondvar` with an internal mutex, creating a nested lock pattern (`cv.internal → state`) that caused deadlocks. The final design uses **one global `Mutex<StoreState>` + one `Condvar`**:

- **Waiter**: locks state → checks condition → `cv.wait(state)` (atomically releases + sleeps)
- **Notifier**: locks state → updates → drops state → `cv.notify_all()`

Single mutex = no deadlock possible. Lost wakeups prevented by the standard Condvar loop pattern.

### 2. Per-operation fd (no thread-local table)

Each `with_read`/`with_write` opens its own fd, applies flock, runs the closure, unlocks, and closes. This avoids:
- Thread-local storage complexity
- fd reuse issues across nested calls
- Reference counting bugs

The cost is one extra `open()`/`close()` per operation, which is negligible for file locking workloads.

### 3. Closure-based API (`FnOnce(&mut File) -> io::Result<R>`)

Locks are held for exactly the closure's duration. This:
- Eliminates lock leaks (forget to unlock)
- Provides panic safety (lock released on unwind)
- Makes the API simple and predictable

### 4. Blocking, try, and timeout variants

```rust
// Blocking (Condvar-based, no spin-wait)
store.with_read(path, |file| { ... })?;
store.with_write(path, |file| { ... })?;

// Non-blocking
store.try_with_read(path, |file| { ... })?;   // Option<R>
store.try_with_write(path, |file| { ... })?;   // Option<R>

// Timeout
store.with_read_timeout(path, dur, |file| { ... })?;  // Result<R, FileLockError>
store.with_write_timeout(path, dur, |file| { ... })?;
```

### 5. No lock upgrades (safety over flexibility)

Trying to upgrade Shared→Exclusive or downgrade Exclusive→Shared on the same thread is blocked (would deadlock at kernel level with same-fd flock). The in-process check detects this and waits (or times out).

### 6. Panic safety

The closure is wrapped in `catch_unwind`. On panic:
1. Kernel flock is released (fd dropped)
2. In-process state is released
3. Panic is resumed (no poisoning)

The store remains usable after a panic.

## Locking Protocol

```
acquire_and_execute(path, lock_type, closure):
  1. Resolve canonical path
  2. Lock state mutex
  3. Loop:
     a. Check can_grant(lock_type, holder)
     b. If yes → add_holder, break
     c. If no → cv.wait(state) or cv.wait_timeout(state, remaining)
  4. Open fd, apply flock(LOCK_SH|LOCK_EX)
  5. catch_unwind(closure)
  6. Drop fd (kernel unlock)
  7. release_lock (remove holder, notify_all)
  8. Return result
```

## Test Coverage

| Category | Tests | What's tested |
|----------|-------|---------------|
| Unit (store.rs) | 14 | Basic R/W, concurrent readers, writer blocks, try-lock, timeout, counter integrity, modify, holder tracking |
| Unit (fd.rs) | 3 | File open, create, separate fds per thread |
| Concurrency | 12 | Data corruption under contention, many readers, writer exclusion, try-lock under contention, timeout precision, stress (16 threads × 5 files), rapid lock churn, mixed workload, panic recovery, drop safety |
| Edge cases | 15 | Directory creation, subdirectory auto-creation, canonical paths, re-entrant shared, error propagation (sync + timeout), zero timeout, introspection, independent files, large files, Send+Sync |
| Multiprocess | 5 | Exclusive flock blocks, shared flock concurrent, non-blocking EWOULDBLOCK, advisory bypass, lock release on fd close |
| Doc tests | 1 | Quick start example compiles |
| **Total** | **47** | |

## Future Considerations

- **Async support** (`tokio::fs` integration) — separate crate or feature flag
- **Windows support** — would need `LockFile`/`UnlockFile` instead of `flock`
- **Per-file condvars** — for better notification precision at scale (single global condvar is O(waiters) per notify)
- **Lock upgrade/downgrade API** — safe version that reopens the fd internally
